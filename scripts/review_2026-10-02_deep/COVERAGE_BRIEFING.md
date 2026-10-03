# 2026-10-04 TEST-COVERAGE pass — BRIEFING

User request: "add unit tests for each fix if not yet or missed fix". The working tree carries an UNCOMMITTED diff on top
of HEAD `b462b7d` (the 10-02 round + verification pass + 10-03/10-04 follow-ups; ledger `state.md` in this folder lists
every fix with its tests). Your job, for the main files you own:

1. **Enumerate every behavioral change** in `git diff HEAD -- <main file>` (ignore Javadoc/comment-only hunks, but DO count
   exception-message changes). Group hunks into "fixes" (one bug = one fix, possibly several hunks/sites/overloads).
2. **Map each fix to its tests** (in the uncommitted test diff `git diff HEAD -- <test files>`, or pre-existing tests).
   A fix counts as covered only if a test (a) exercises EVERY changed site/path/overload of that fix (sync/async/reactive,
   v3/v4, v1/v2, each read path such as toEntity / readRow / row mapper / single value / typed array / Dataset / stream /
   UDT / mapper delegate), and (b) FAILS on clean HEAD where HEAD had the bug — prove with
   `sh $SCRATCH/tools/test-head.sh <tag> <TestClass>[#method]` (runs current test sources against prebuilt HEAD classes at
   `$SCRATCH/out/head`). For a fix of a bug introduced AFTER HEAD (a regression of an earlier fix in the same diff), the
   test is RED on the intermediate build instead — say so; it must still pass now and pin the behavior.
   Also check (c) "previous behavior still works" pins exist where the change could plausibly regress something.
3. **Add the missing tests** (and fix weak ones — e.g. asserting only "no exception", or stopping at the first of several
   cases so later cases are never proven). Look for **missed fixes** too: a sibling/twin path where the same bug still
   exists (fix it in your files with a why-comment + test; report twins in files you don't own under FOR MAIN).
4. Run your test classes; every new test GREEN now, and RED on HEAD when it covers a HEAD bug.

## Rules (strict)
- Edit ONLY your main files (only to fix a genuinely missed fix) and your listed test classes. Edit tool only (CRLF files;
  never `sed -i`/whole-file rewrites). New tests in ONE block per test class near the end, marker
  `// ---- 2026-10-04 coverage<X> ----`. Read the file right before each Edit.
- No `mvn`, no `abacus-da-all/target`, no `git commit/stash/checkout/reset/restore/worktree`. Other agents edit other files.
- Keep the tree compiling. Don't reformat untouched code. Don't re-raise settled/left-for-user items (see `state.md`).
- Live services: Cassandra, DynamoDB Local, Cosmos emulator, MongoDB are UP now (Docker); the BigQuery emulator is down.
  Prefer offline/Mockito tests; live tests are fine where they are the natural place (clean up probe data).
- Memory: one JVM at a time; on "malloc failed"/"insufficient memory" delete hs_err/replay files, wait, retry a few times.

## Tools
SCRATCH = `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/a890f6d9-f4c8-4716-9768-805309f546ec/scratchpad`
- `sh $SCRATCH/tools/compile.sh <tag>`; `sh $SCRATCH/tools/test.sh <tag> <TestClass>[#m] ...` (working tree);
  `sh $SCRATCH/tools/test-head.sh <tag> <TestClass>[#m] ...` (current tests vs HEAD classes);
  `MAINTAG=<dir> sh $SCRATCH/tools/test-on.sh <tag> <TestClass>[#m]` (vs `$SCRATCH/out/<dir>`; snapshots `pre1004`
  = before fixBQ2, `pre1004b` = before the text-type-variable fix).
- Notes of earlier agents: `$SCRATCH/notes-*.md`; probes `$SCRATCH/probe-*/`.

## Report (final message)
```
COVERAGE TABLE: fix (file:line / ledger entry) | sites | tests | RED on HEAD? | gaps found
TESTS ADDED: TestClass#method — what it covers — RED on HEAD/intermediate? GREEN now
MISSED FIXES APPLIED (if any): file:line — bug → change — test
FOR MAIN: twins in files you don't own
VERIFICATION: test.sh found/succeeded/failed per class; test-head.sh results
```
Also write `$SCRATCH/notes-coverage<X>.md`.
