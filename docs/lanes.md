# Why four lanes aren't four times one

`campaign lanes` drains 400,000 changes. Four lanes take 6.7 s where one takes 12.7, which is 1.9 times. With 5 ms to the target and back it is 14.2 s against 30.3, 2.1 times. The target was 2.5 and it is missed.

An earlier run, with other work on the machine, gave 1.4 and 1.8. In one like it the four cores were 95 to 99% busy. The source decodes the log on one core and can't use a second. The engine takes about half of one. Four lanes are four Postgres sessions that want a core each, and there are four cores in all.

So lanes buy what is left after decoding. A bigger machine will say how much more there is, this one can't.

What the campaign does show: the rows are the same with one lane and with four, every time.
