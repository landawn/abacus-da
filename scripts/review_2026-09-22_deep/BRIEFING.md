# Review round 2026-09-22 (deep) — BRIEFING for review+fix agents

You are ONE agent in a multi-agent, line-by-line **bug-fix + Javadoc-improvement** review of
`abacus-da-all/src/main/java` (67 units, ~88k lines). HEAD = `3a72a70`, working tree clean at
round start. You OWN a slice of files (given in your prompt). Unlike earlier rounds, **you apply
your own fixes** in the files you own and **add a regression unit test for every code fix** to the
existing test class(es) that cover your files.

## What changed since the last deep review (the fresh variables — focus here)
The last full review was 2026-08-01 (`7ff88ab`). Since then:
- **~10.5k inserted / 3.6k deleted lines across 40 main files** (naming work, exception-contract
  passes, a NPE→IAE null-argument pass, new `com.landawn.abacus.da.cs` constant class used as
  argument names in `N.checkArgNotNull(x, cs.x)`, validation-order reorders, lots of Javadoc).
  Run `git diff 7ff88ab..HEAD -- <your file>` and review those hunks with extra care — new code
  and rewritten docs are where new defects are most likely. But still read the WHOLE file.
- **Dependencies bumped: abacus-common 8.0.1 and abacus-query 4.9.3** (were 7.8.8 / 4.8.9).
  Library behavior your code relies on may have changed. Verify against the jars in
  `~/.m2/repository/com/landawn/abacus/...` (sources jars are there — extract/read them).
  KNOWN change: **`ContinuableFuture.map(func)` in 8.0.x wraps a failure of `func` in
  `ExecutionException` when `get()` is called** (7.x rethrew it unwrapped). This SUPERSEDES the
  older "map mapper exceptions rethrow UNWRAPPED" note in the 07-25 briefing. For
  `thenRunAsync(BiConsumer)`/callback-style APIs do NOT assume — probe it.

## MANDATORY: read the settled list first
Read `scripts/review_2026-07-25/BRIEFING.md` section "DO NOT RE-FLAG" IN FULL (skip its
"NEW THIS ROUND" and "How to verify" sections, which are stale), plus the additions in
`scripts/review_2026-07-26/BRIEFING.md` and `scripts/review_2026-08-01/BRIEFING.md`. Those are
settled decisions from ~20 prior rounds; re-raising them is noise. Exceptions to that list:
the ContinuableFuture note above, and anything the post-08-01 code changes made genuinely wrong.
Additional settled decisions after 08-01 (do not re-litigate):
- Null-argument policy: a direct public-API parameter that ALWAYS fails on null →
  `N.checkArgNotNull(x, cs.x)` → IllegalArgumentException. NPE is kept for inherited contracts
  (compareTo, driver codec SPI), object state, result-state (`queryForSingleNonNull`), callback
  results, and null ELEMENTS inside collections (except Mongo insertMany/bulkInsert → IAE).
  Null-accepting paths are preserved; checks are not moved between eager and async.
- `cs` lives in `com.landawn.abacus.da.cs` (project-local); never import `com.landawn.abacus.util.cs`.
- Unchecked exceptions are documented via `@throws`, not added to `throws` clauses.
- Mongo closed-client `IllegalStateException` is documented on eager sync/reactive methods;
  Cassandra 65,535-statement batch limit `IllegalStateException` documented on batchInsert/Update.

## Workstreams
- **WS1 bugs (correctness)** — highest bar: name the failing input/state and the wrong outcome,
  and prove it (probe or failing test). FIX it and add a RED→GREEN regression test.
- **WS2 Javadoc** — doc contradicts actual behavior; wrong/phantom `@param`/`@throws`/`@return`;
  broken `{@link}`; examples that would not compile or whose `// returns`/`// throws` comments are
  false; copy-paste drift from a sibling; typos/garbled sentences. FIX in place (comment-only).
  Improve clarity only where it's genuinely unclear or incomplete — no churn/rewording for taste.
- **WS3** wrong/misleading exception or log message text — fix.
- Performance/simplification: only real, zero-risk wins; otherwise report, don't apply.

## Rules for EDITING (strict)
1. Edit ONLY the main-source files in your slice. For tests, edit ONLY the test classes listed
   for your slice (a few test classes are shared between two slices — always Read the file
   immediately before each Edit, and put your new tests in one contiguous block near the end of
   the class so concurrent edits don't collide).
2. Use the Edit tool. **Never** use `sed -i`, Python rewrites, or other whole-file rewrites —
   they silently convert CRLF→LF on these files.
3. Do NOT run `mvn`, do NOT touch `abacus-da-all/target`, do NOT `git commit/stash/checkout/reset`.
   Other agents are editing other files concurrently.
4. **Behavior changes need a clear bug.** If a fix would change a public contract in a way that's
   a judgment call (API shape, different exception type for a documented case, removing
   leniency someone may rely on), do NOT apply it — list it under PROPOSED in your report with
   evidence. Fixing code so it matches its own documented contract IS in scope.
5. Every code fix gets a regression test (JUnit 5, existing class, follow its style —
   most extend `TestBase`, many use Mockito). Verify it FAILS before the fix (or reason
   precisely why it would) and PASSES after. Doc-only fixes need no test.
6. Keep the file compiling at all times (other agents compile the whole tree). Make each
   Edit self-contained.

## Tools (parallel-safe — use these, never mvn)
SCRATCH = `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/1e5f1a98-7e8a-43c7-aea9-cd5c6b848f1b/scratchpad`
- Compile all main sources to a private dir:
  `sh $SCRATCH/tools/compile.sh <yourtag>` → prints errors then `COMPILE OK`/`COMPILE FAILED`.
  Output classes: `$SCRATCH/out/<yourtag>`. If it fails in a file you do NOT own, another agent
  is mid-edit — wait ~1 min and retry (don't touch their file).
- Compile + run specific test classes (optionally `#method`):
  `sh $SCRATCH/tools/test.sh <yourtag> <TestClassSimpleNameOrFqcn>[#method] ...`
  → prints failures and `TESTS found=.. succeeded=.. failed=..`. Use a UNIQUE tag (e.g. your
  slice letter: `sliceE`), since the tag names the private output dir.
- Probes: write `Probe.java` under `$SCRATCH/probe-<yourtag>/`, compile with
  `javac -cp "$SCRATCH/out/<yourtag>;$(cat $SCRATCH/testcp.txt)" -d ...` and run with java.
  (`testcp.txt` = full test classpath, Windows `;` separator. Use `cygpath -m` for paths.)
- Driver ground truth: jars in `~/.m2/repository`; `javap -c -p`, or unzip `-sources.jar`.
  Versions: hbase-client 2.6.x, azure-cosmos 4.8x, google-cloud-bigquery 2.6x, neo4j-ogm 5.0.x,
  DataStax v4 (`cassandra/`) and 3.11.x (`cassandra/v3/`) — NOTE the dir names are the opposite
  of what you'd guess; mongodb driver 5.x; AWS SDK v1 (`aws/dynamodb/`) and v2 (`aws/dynamodb/v2/`).
  Check `testcp.txt` for exact versions.
- Live-service tests: Cassandra/DynamoDB/Mongo/Neo4j `*ExecutorTest` classes that open real
  connections may or may not have a service reachable. Prefer adding tests to the OFFLINE
  (Mockito/pure-logic) class for your file. If only a live class fits and the service is down,
  add the test anyway and say "compile-verified only".

## Report (your final message — the main agent verifies it against the diff)
```
APPLIED
- [WS1|WS2|WS3][P1|P2|P3] file:line — what was wrong → what you changed.
  Evidence: <code excerpt / probe result / driver bytecode>. Test: <TestClass#method> (RED→GREEN) or "doc-only".
PROPOSED (not applied — needs a decision)
- ...
VERIFICATION
- compile.sh result; test.sh result for each touched test class (found/succeeded/failed).
PER-FILE VERDICT
- file: CLEAN | N fixes
```
Be concise; no narration of what you read. Don't report things you checked and found fine
beyond the per-file verdict.

## Cross-cutting findings from earlier slices this round (check your files for the same patterns)
- **abacus-common 8.0.1 `N.asList(...)`, `N.asSet(...)`, `N.asMap(...)` return IMMUTABLE collections.** Flag
  main code that mutates such a result (or hands it to a driver API that mutates its argument, e.g.
  HBase `Table.delete(List)` removes applied Deletes), or docs promising a mutable result. Javadoc samples
  that pass `N.asList`/`Arrays.asList` into list-mutating APIs are also defects.
- **`ContinuableFuture.map(func)` re-runs `func` on EVERY `get()`** (8.0.1 source, ContinuableFuture:1501).
  If `func` consumes a one-shot resource (driver cursor/iterator/ResultSet page), a second `get()` yields a
  different (empty) result. Slice O fixed the v4 Cassandra async executors with a memoizing wrapper
  (`AsyncCassandraExecutorBase.memoize`, package-private). Check any `.map(...)` on a ContinuableFuture in
  your files that reads a one-shot source.
- **Byte conversions:** `N.convert(ByteBuffer, byte[].class)` returns null; `N.convert(byte[], ByteBuffer.class)`
  returns a buffer with position == limit (reads as empty under NIO read convention). Check BYTES/binary
  conversion paths.
- **`Dataset.forEach(consumer)`** passes a `DisposableObjArray` row, not a bean — samples like
  `dataset.forEach(product -> process(product))` are wrong; use `dataset.toList(Bean.class).forEach(...)`.
- **Memory:** the machine is shared by ~6 agents; if a JVM crashes (hs_err_pid*.log), delete the hs_err/replay
  files it created and retry. Don't run more test classes at once than you need.
