# First issues

**Show the refused rows of a chunk in the console.** The journal has them: operation `R` with the chunk's number. Clicking a square in the map could list them. Touches `ListChunks`, `Chunks` in `App.tsx`. No engine code.

**A `--since` flag for explain.** Keys with a long history print a lot. The journal has a block range index on `at`. Touches `explain.go` and the command line.

**Read chunk boundaries from the index.** `NextBound` walks the index row by row to find each boundary. On a wide key that is the slowest part of planning. Try `TABLESAMPLE` on the primary key for boundaries that are only roughly even, and bring a profile. Touches `chunk.go`. Chunks may then differ in size, so check what that does to the byte budget.
