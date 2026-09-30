# Review round 2026-09-29 (deep) — BRIEFING for review+fix agents

You are ONE agent in a multi-agent, line-by-line **bug-fix + Javadoc-improvement** review of
`abacus-da-all/src/main/java` (67 units, ~93k lines). HEAD = `9671b2b`, working tree clean at
round start. You OWN a slice of files (given in your prompt). **You apply your own fixes** in the
files you own and **add a regression unit test for every code fix** to the test class(es) listed
for your slice. **Comment complex or unusual fixes** in the code (a short `//` comment saying why);
no comment is needed for plain parameter validation.

## What changed since the last deep review (the fresh variables — focus here)
The previous deep round was 2026-09-27 (same 21-slice split as this one). Its fixes were committed in
`9671b2b` and have NOT been independently reviewed since — they were written by the agents that
proposed them. Run `git diff 3dc3adf 9671b2b -- <your file>` and review those hunks with extra
care (new helper methods, parser changes, CqlBuilder DELETE rendering, per-row conversions, etc.):
check each fix is correct, complete across all overloads/siblings, doesn't regress another path,
and that its Javadoc matches. But still read the WHOLE file line by line.

Dependencies are UNCHANGED since that round: **abacus-common 8.1.0, abacus-query 4.9.4**.

## MANDATORY: read the settled lists first
Settled decisions from ~22 prior rounds; re-raising them is noise:
1. `scripts/review_2026-07-25/BRIEFING.md` section "DO NOT RE-FLAG" IN FULL (skip its "NEW THIS ROUND" /
   "How to verify" sections), plus the additions in `scripts/review_2026-07-26/BRIEFING.md` and
   `scripts/review_2026-08-01/BRIEFING.md`.
2. `scripts/review_2026-09-22_deep/BRIEFING.md` sections "MANDATORY", "Cross-cutting findings", and the
   "Findings" in `scripts/review_2026-09-22_deep/state.md` (PROPOSED items there were decided NOT applied
   unless a later round applied them).
3. The 2026-09-27 round's notes for your slice letter: `$OLD/notes-slice<X>.md` (see Tools for $OLD).
   Same slice letters as this round. They say what was checked/probed/fixed last time.
4. 2026-09-27 **decided-NOT-applied** (don't re-raise without NEW evidence): Cassandra Set for lone `IN ?`
   unpacked positionally; CqlBuilder `delete(Class, excluded)` with nothing left → whole-row delete; raw
   builders render collections as JSON strings; Mongo groupBy(Collection) with dotted names → server 16412;
   toList Object[]/List row type vs readRow; DDB v1 byte[] stored as S JSON text; nested ByteBuffer → "" on
   DDB write; AnyPut writes getter-only/@Transient props; Cosmos get/gett treats 404 substatus 1003 (missing
   container) as absent; Cosmos nested bean props render sub-entity table name; BigQuery STRUCT→Object prop,
   single-value REPEATED unwrap.
5. Policy (don't re-litigate): null direct public-API parameter that ALWAYS fails → `N.checkArgNotNull(x, cs.x)`
   (IAE); NPE kept for inherited contracts, object state, result state (`queryForSingleNonNull`), callback
   results, null elements in collections (except Mongo insertMany/bulkInsert → IAE). `cs` =
   `com.landawn.abacus.da.cs` (never `com.landawn.abacus.util.cs`). User commit `7718119` deliberately added
   unchecked exceptions to `throws` clauses everywhere — leave them. Mongo closed-client ISE / Cassandra
   65,535-statement batch-limit ISE are documented conventions. DynamoDB IAE-for-null `@throws` convention kept.
   The Mongo `{"_id": {"$oid": ...}}` extended-JSON example form is correct — do not change it.

## Known library traps (verify against these; details in $SCRATCH/dep-notes-*.md)
- abacus-common 8.1.0 `N.convert(x, T)` returns x unchanged when `T.isInstance(x)` — REGISTERED converters are
  skipped (BigQuery FieldValueList is-a List; `N.convert(driverRow, Object.class)` returns the raw row).
- `N.asList/asSet/asMap` return IMMUTABLE collections. `ContinuableFuture.map(func)` re-runs `func` on EVERY
  `get()` and wraps its failure in ExecutionException; then*Async drop one ExecutionException layer.
- `N.convert(ByteBuffer, byte[].class)` → null; `N.convert(byte[], ByteBuffer.class)` → position==limit buffer.
- `Dataset.forEach` passes a `DisposableObjArray`, not a bean.
- abacus-query 4.9.4: parent `from(...)` rejects non-QUERY ops (CqlBuilder renders column DELETE itself);
  comment-only `Filters.expr("/* c */")` rejected; naming conversion skips quoted identifier segments.
- Getter-only props on an `@Entity` SUPERCLASS appear in a plain subclass's propInfoList (8.1.0).

## Workstreams
- **WS1 bugs (correctness)** — highest bar: name the failing input/state and the wrong outcome, and prove it
  (probe or failing test). FIX it and add a RED→GREEN regression test.
- **WS2 Javadoc** — doc contradicts actual behavior; wrong/phantom `@param`/`@throws`/`@return`; broken
  `{@link}`; examples that would not compile or whose `// returns`/`// throws` comments are false; copy-paste
  drift from a sibling; typos/garbled sentences. FIX in place (comment-only). Improve clarity only where it's
  genuinely unclear or incomplete — no churn/rewording for taste.
- **WS3** wrong/misleading exception or log message text — fix.
- Performance/simplification: only real, zero-risk wins; otherwise report, don't apply.

## Rules for EDITING (strict)
1. Edit ONLY the main-source files in your slice. For tests, edit ONLY the test classes listed for your
   slice (a few are shared between two slices — always Read the file immediately before each Edit, and put
   your new tests in ONE contiguous block near the end of the class, with a `// ---- 2026-09-29 slice<X> ----`
   marker comment, so concurrent edits don't collide).
2. Use the Edit tool. **Never** use `sed -i`, Python rewrites, or other whole-file rewrites — they silently
   convert CRLF→LF on these files.
3. Do NOT run `mvn`, do NOT touch `abacus-da-all/target`, do NOT `git commit/stash/checkout/reset/worktree`.
   Other agents are editing other files concurrently.
4. **Behavior changes need a clear bug.** If a fix would change a public contract in a way that's a judgment
   call (API shape, different exception type for a documented case, removing leniency someone may rely on),
   do NOT apply it — list it under PROPOSED with evidence. Fixing code so it matches its own documented
   contract IS in scope.
5. Every code fix gets a regression test (JUnit 5, existing class, follow its style — most extend `TestBase`,
   many use Mockito). Verify it FAILS before the fix (or reason precisely why it would) and PASSES after.
   Doc-only fixes need no test.
6. Comment complex or unusual fixes (why, not what). No comment for parameter validation.
7. Keep the file compiling at all times (other agents compile the whole tree). Make each Edit self-contained.

## Tools (parallel-safe — use these, never mvn)
SCRATCH = `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/510410a4-7dfb-4e8a-87dc-3c209e4fae33/scratchpad`
OLD     = `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/20a80222-594d-4731-903d-c95ca3e4c906/scratchpad`
  (previous round: `notes-slice<X>.md`, `probe-slice<X>/` probes you can reuse, extracted library sources
  `src-ac-8.1.0/` (abacus-common) and `src-aq-4.9.4/` (abacus-query).)
- Compile all main sources to a private dir: `sh $SCRATCH/tools/compile.sh <yourtag>` → errors then
  `COMPILE OK`/`COMPILE FAILED`. Output classes: `$SCRATCH/out/<yourtag>`. If it fails in a file you do NOT
  own, another agent is mid-edit — wait ~1 min and retry (don't touch their file).
- Compile + run specific test classes (optionally `#method`):
  `sh $SCRATCH/tools/test.sh <yourtag> <TestClassSimpleNameOrFqcn>[#method] ...` → failures and
  `TESTS found=.. succeeded=.. failed=..`. Use your UNIQUE tag (`slice<X>`).
- Probes: write under `$SCRATCH/probe-slice<X>/`, compile with
  `javac -cp "$SCRATCH/out/slice<X>;$(cat $SCRATCH/testcp.txt)" -d ...` and run with java
  (`testcp.txt` = full test classpath, `;` separator; use `cygpath -m` for paths).
- Driver ground truth: jars in `~/.m2/repository` (`javap -c -p`, or unzip `-sources.jar`). Check
  `testcp.txt` for exact versions. NOTE `cassandra/` = DataStax driver **v4**, `cassandra/v3/` = **3.x**;
  `aws/dynamodb/` = SDK v1, `aws/dynamodb/v2/` = SDK v2.
- Live services (Cassandra, Mongo, DynamoDB Local, Cosmos/BigQuery emulators, Neo4j) may or may not be up.
  Prefer adding tests to the OFFLINE (Mockito/pure-logic) class for your file. If only a live class fits and
  the service is down, add the test anyway and say "compile-verified only".
- Memory: ~6 agents share the machine. If a JVM crashes (hs_err_pid*.log), delete the hs_err/replay files it
  created and retry. Don't run more test classes at once than you need.

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
Be concise; no narration of what you read. Don't report things you checked and found fine beyond the
per-file verdict.

## Cross-cutting findings from earlier slices THIS round
(the main agent appends here as slices report — re-read this section before you finish)
- (slice A) **Read-only (getter-only) properties fail entity reads.** With abacus-common 8.1.0 a getter-only property
  inherited from an `@Entity` superclass is in `propInfoList`, so write paths emit its value, but
  `PropInfo.setPropValue` throws `UnsupportedOperationException` on read → the whole row/item fails. HBaseExecutor
  (9671b2b) and DDB v1 now skip such props on read. DDB v1's check (PropInfo.isReadOnlyProperty is package-private):
  `propInfo.field == null && propInfo.jsonXmlExpose == JsonXmlField.Direction.SERIALIZE_ONLY` (exact per ParserUtil
  8.1.0:3135-3141). Check every toEntity/readRow path in your files that calls `setPropValue` from driver data.
- (slice A) DDB v1 `convertValue`'s `isJsonArrayText` only checks brackets: text like `"[a] and [b]"` into a
  String[]/Object[] target threw ParsingException → now falls back to `N.convert`. Slice C: same block in v2.
- (slice P) `Map.containsKey(String)` on a caller-supplied map can throw ClassCastException (sorted map with non-String
  keys, e.g. TreeMap<Integer,...>) / NPE. Any "is this map a name→value container?" probe on user data must tolerate it.
