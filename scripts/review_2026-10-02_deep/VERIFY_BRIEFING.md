# 2026-10-02 VERIFICATION pass — BRIEFING for reviewer agents

The 2026-10-02 deep round (ledger `scripts/review_2026-10-02_deep/state.md`, original briefing `BRIEFING.md` in the
same folder) left an UNCOMMITTED diff on top of HEAD `b462b7d`: 20 main files under `abacus-da-all/src/main/java`
(~965+/234-) and 18 test files (+1586). Those changes were written by the agents that found the bugs and have NOT
been independently reviewed. History says this matters: each of the last two rounds found regressions introduced by
the previous round's own fixes.

You are an INDEPENDENT reviewer for a group of those changed files (given in your prompt). You now OWN those files
and their listed test classes, and you fix whatever is wrong.

## Your job, for every changed hunk in your files (`git diff HEAD -- <file>`), line by line
1. **Correct** — does it really fix the stated bug (see the slice's entry in `state.md`), for every input? Attack it with
   adversarial inputs (nulls, empties, nested/recursive shapes, subclasses, primitives vs wrappers, unusual types, huge
   inputs, concurrency where relevant). Probe with real code; don't just reason.
2. **No regressions** — compare behavior against clean HEAD for inputs that worked before (not just the bug input).
   Clean-HEAD main classes are prebuilt at `$SCRATCH/out/head`; `sh $SCRATCH/tools/test-head.sh <tag> <TestClass>[#m]`
   runs the CURRENT test sources against them (use it to prove a test is RED on HEAD), and probes can put
   `$SCRATCH/out/head` vs `$SCRATCH/out/<tag>` on the classpath to diff outputs over a corpus.
3. **Complete** — the same defect fixed in every sibling/overload/twin (sync / async / reactive; Cassandra v3 / v4;
   DynamoDB v1 / v2 / v2-async; executor vs mapper; every read path: toEntity / readRow / row mapper / single value /
   typed array / Dataset / stream / UDT). If a twin lives in a file you don't own, report it under FOR MAIN.
4. **Efficient** — no needless per-row/per-cell work on hot paths (repeated reflection/BeanInfo/Type lookups that can be
   hoisted, allocations when nothing changes, quadratic scans, extra copies of large result sets). Fix real waste only
   when the fix is simple and safe; otherwise report.
5. **Commented** — every complex/unusual fix has a short why-comment; plain parameter validation needs none. Comments must
   be accurate (no stale comment describing old behavior).
6. **Tested** — every fix has unit tests that cover ALL relevant scenarios, not just the single repro: each affected
   overload/path, the negative/edge cases, and a "previous behavior still works" pin where the change could plausibly
   regress something. Every new test must assert something meaningful and be RED on HEAD if it claims to cover a fix
   (prove with test-head.sh). Add missing tests; fix weak ones.
7. **Documented** — every public method whose behavior changed has accurate Javadoc (`@throws`, prose, examples), and so
   do public delegates in your files (mappers / async / reactive). No Javadoc may contradict the code.

Also read the code AROUND each hunk (the whole method and its callers) — a fix can be right locally and wrong in context.
You do NOT need to re-review unchanged code elsewhere in the file.

## Settled items — do not re-raise
Read `scripts/review_2026-10-02_deep/BRIEFING.md` sections "MANDATORY: read the settled lists first", "Known library
traps", and "Cross-cutting findings"; and the PROPOSED/"left for user" items in `state.md`. Those were deliberately not
applied. (You may of course find that an APPLIED fix is wrong — that is the point of this pass.)

## Rules for EDITING (strict)
1. Edit ONLY your files and your listed test classes. Use the Edit tool. **Never** `sed -i`, Python rewrites or other
   whole-file rewrites (they silently convert CRLF→LF). Never reformat untouched code.
2. Do NOT run `mvn`, do NOT touch `abacus-da-all/target`, do NOT `git commit/stash/checkout/reset/restore/worktree`.
   Other reviewers edit other files concurrently. Never revert another agent's change wholesale; fix it in place.
3. Behavior changes need a clear bug; a judgment call goes to PROPOSED with evidence.
4. Keep the tree compiling at all times; make each Edit self-contained.
5. Put new tests in ONE contiguous block near the end of each test class with a `// ---- 2026-10-02 verify<X> ----`
   marker (always Read the file right before each Edit).

## Tools (parallel-safe — never mvn)
SCRATCH = `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/a890f6d9-f4c8-4716-9768-805309f546ec/scratchpad`
- `sh $SCRATCH/tools/compile.sh <tag>` — compile all main sources into `$SCRATCH/out/<tag>`.
- `sh $SCRATCH/tools/test.sh <tag> <TestClassSimpleNameOrFqcn>[#method] ...` — compile + run tests against the working tree.
- `sh $SCRATCH/tools/test-head.sh <tag> <TestClass>[#method] ...` — run current tests against clean-HEAD classes (RED proof).
- Probes under `$SCRATCH/probe-verify<X>/`; classpath `$(cat $SCRATCH/testcp.txt)` (`;`-separated; `cygpath -m` paths);
  `javac -J-Xmx512m`, `java -Xmx512m -XX:+UseSerialGC`.
- The previous round's probes for your files are in `$SCRATCH/probe-slice<L>/` and notes in `$SCRATCH/notes-slice<L>.md`
  (slice letters per `state.md`). Library sources: `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/20a80222-594d-4731-903d-c95ca3e4c906/scratchpad/src-ac-8.1.0/` (abacus-common) and `.../src-aq-4.9.4/` (abacus-query).
- Services: Docker containers (Cassandra, DynamoDB Local, Cosmos/BigQuery emulators) are DOWN and must not be started.
  The local MongoDB (Windows service mongod) is up. Prefer offline/Mockito tests.
- **Memory is tight**: one JVM at a time; if a JVM dies with "insufficient memory"/"malloc failed", delete the
  hs_err_pid*/replay_pid* files it created, wait 1–2 min, retry (a few times at most, then report).

## Report (your final message — the main agent re-verifies against the diff)
```
VERDICT per changed hunk/fix: OK | FIXED (what) | PROPOSED
FIXES APPLIED
- [BUG|REGRESSION|INCOMPLETE|PERF|COMMENT|TEST|DOC] file:line — problem → change. Evidence. Test (RED on HEAD? / GREEN).
PROPOSED (not applied)
FOR MAIN (twins in files you don't own)
VERIFICATION — compile result; test.sh results per touched class (found/succeeded/failed); test-head.sh RED checks.
```
Also write `$SCRATCH/notes-verify<X>.md`. Be concise.
