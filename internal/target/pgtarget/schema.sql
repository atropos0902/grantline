create schema if not exists grantline;

create table if not exists grantline.progress (
  replication text primary key,
  epoch       bigint not null,
  owner       text not null default '',  -- the process that took it over last
  phase       text not null default 'new',
  lsn         pg_lsn not null default '0/0',
  commit_time timestamptz,
  ready_at    pg_lsn,
  system_id   text not null default '',
  timeline    int not null default 0,
  updated     timestamptz not null default now()
) with (fillfactor = 20);

create table if not exists grantline.copied (
  replication text, tbl text,
  planned     boolean not null default false,  -- every chunk is in grantline.chunk
  finished    boolean not null default false,  -- every chunk has landed and the ledger is gone
  had_rows    boolean not null default false,  -- the target wasn't empty, so chunks also delete
  primary key (replication, tbl)
);

create table if not exists grantline.chunk (
  replication text, tbl text, no int,
  lo text[], hi text[],
  landed   boolean not null default false,
  snapshot pg_snapshot, marker pg_lsn,
  rows bigint, refused bigint, crc32c bigint, epoch bigint, at timestamptz,
  primary key (replication, tbl, no)
);

-- The last transaction the stream applied to each key while its table is being
-- copied. A deleted key keeps its row here. Key '*' stands for a truncate.
create table if not exists grantline.ledger (
  replication text, tbl text, key text,
  xid xid8 not null, lsn pg_lsn not null, op "char" not null,
  reread boolean not null default false,
  primary key (replication, tbl, key)
) with (fillfactor = 80);

create table if not exists grantline.journal (
  replication text not null, tbl text not null, key text not null,
  op "char" not null,
  xid xid8, lsn pg_lsn, snapshot pg_snapshot, chunk int, epoch bigint,
  at timestamptz not null default clock_timestamp()
);
create index if not exists journal_key on grantline.journal using hash (key);
create index if not exists journal_at on grantline.journal using brin (at);

-- Moves the position, or only checks the owner when l is null. The caller
-- commits in the same round trip, and an error here turns that into a rollback.
create or replace function grantline.advance(r text, e bigint, l pg_lsn, t timestamptz, ready boolean)
returns void language plpgsql as $$
begin
  update grantline.progress
     set lsn = coalesce(l, lsn), commit_time = coalesce(t, commit_time), updated = clock_timestamp(),
         phase = case when ready then 'ready' else phase end,
         ready_at = case when ready then l else ready_at end
   where replication = r and (e is null or epoch = e);
  if not found then
    raise exception 'replication % is owned by a newer worker', r using errcode = 'GL001';
  end if;
end $$;
