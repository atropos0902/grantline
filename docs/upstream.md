# Upstream

**pglogrepl refuses an insert into a table without published columns.** `InsertMessage.Decode` wants at least 8 bytes of message. The smallest valid one is 7: the relation's ID, the tag and a column count of zero. Found by `TestAgreesWithPglogrepl`, which compares the two decoders on generated rows. Not reported yet. It needs a table nobody would replicate, so it is a small fix to send with its test, and nothing here waits for it.
