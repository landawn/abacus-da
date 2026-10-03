# Review round 2026-10-02 (deep) — BRIEFING for review+fix agents

You are ONE agent in a multi-agent, line-by-line **bug-fix** review of `abacus-da-all/src/main/java`
(67 units, ~94k lines). HEAD = `b462b7d`, working tree clean at round start. You OWN a slice of files
(given in your prompt). **You apply your own fixes** in the files you own and **add a regression unit
test for every code fix** to the test class(es) listed for your slice. **Comment complex or unusual
fixes** in the code (a short `//` comment saying why); no comment is needed for plain parameter validation.

## What changed since the last deep review (the fresh variables — focus here FIRST)
The previous deep round was 2026-09-29 (same 21-slice split as this one). Its fixes were committed in
`b462b7d` ("aaa") and have NOT been independently reviewed — they were written by the agents that
proposed them. History shows this matters: the 09-29 round found 4 regressions introduced by the 09-27
round's own fixes. Run `git diff 9671b2b b462b7d -- <your file>` and review those hunks with extra care:
- new helpers: `isReadOnlyProperty` / getter-only-prop skips (DDB v1/v2, BigQuery, Cassandra v3/v4 incl.
  UDTCodec), `containsKeySafely`, `toArrayElement`/`convertRepeatedValue` (BigQuery), `unmaskBrackets`
  and slice `..` handling (ParsedCql), `checkCanAppendCqlFrom` (CqlBuilder), Mongo `BsonValueCodec`/
  `DocumentCodec` decode in `GeneralCodec`, `stream(MongoCursor, Object/Bson/Map/Document)` short-circuit,
  `ResultSets` `getAvailableWithoutFetching`, empty-container-for-parameterless-query (Cassandra v3/v4),
  UDT decode by position (v4), `CqlMapper.saveTo` ordering;
- check each fix is correct, complete across ALL overloads/siblings (sync/async/reactive, v3/v4, v1/v2),
  doesn't regress another path, and that its Javadoc matches.
But still read the WHOLE file line by line — the goal is every real bug in the file, old or new.

Dependencies are UNCHANGED since the 09-27 round: **abacus-common 8.1.0, abacus-query 4.9.4**
(jakarta.xml.bind-api 4.0.5 was added as a TEST dependency in b462b7d).

## MANDATORY: read the settled lists first
Settled decisions from ~23 prior rounds; re-raising them is noise:
1. `scripts/review_2026-07-25/BRIEFING.md` section "DO NOT RE-FLAG" IN FULL (skip its "NEW THIS ROUND" /
   "How to verify" sections), plus the additions in `scripts/review_2026-07-26/BRIEFING.md` and
   `scripts/review_2026-08-01/BRIEFING.md`.
2. `scripts/review_2026-09-22_deep/BRIEFING.md` sections "MANDATORY", "Cross-cutting findings", and the
   "Findings" in `scripts/review_2026-09-22_deep/state.md` (PROPOSED items there were decided NOT applied
   unless a later round applied them).
3. `scripts/review_2026-09-29_deep/BRIEFING.md` (esp. "Known library traps", "Cross-cutting findings") and
   `scripts/review_2026-09-29_deep/state.md` "Findings" — what was fixed/proposed last round per slice.
   Plus the 09-29 notes for your slice letter: `$PREV/notes-slice<X>.md` (same slice letters as this round;
   they say what was checked/probed/fixed last time; `$PREV/probe-slice<X>/` has reusable probes).
4. **Decided-NOT-applied** (don't re-raise without NEW evidence):
   - 09-27: Cassandra Set for lone `IN ?` unpacked positionally; CqlBuilder `delete(Class, excluded)` with
     nothing left → whole-row delete; raw builders render collections as JSON strings; Mongo groupBy(Collection)
     with dotted names → server 16412; toList Object[]/List row type vs readRow; DDB v1 byte[] stored as S JSON
     text; nested ByteBuffer → "" on DDB write; AnyPut writes getter-only/@Transient props; Cosmos get/gett treats
     404 substatus 1003 (missing container) as absent; Cosmos nested bean props render sub-entity table name;
     BigQuery STRUCT→Object prop, single-value REPEATED unwrap.
   - 09-29: Cosmos default ctor SNAKE_CASE vs SDK camelCase (documented); Cosmos dedupe selectPropNames; Mongo
     Bson-projection scalar read returns `_id` for docs lacking the field (documented); BigQuery delete/exists
     with null id → `IS NULL`; BigQuery STRUCT → List<Long> prop raw strings; TIMESTAMP → Year; Dataset.toList
     (abacus-common) getter-only UOE; insert/update write computed getter-only column; Code/Symbol deprecated
     BSON types treated as beans; dotted-path-to-getter-only UOE (root cause in abacus-common BeanInfo).
   - 09-29 behavior change (keep): Cassandra v3+v4 accept a single empty Map/Collection/array for a
     parameterless query (binds nothing).
5. Policy (don't re-litigate): null direct public-API parameter that ALWAYS fails → `N.checkArgNotNull(x, cs.x)`
   (IAE); NPE kept for inherited contracts, object state, result state (`queryForSingleNonNull`), callback
   results, null elements in collections (except Mongo insertMany/bulkInsert → IAE). `cs` =
   `com.landawn.abacus.da.cs` (never `com.landawn.abacus.util.cs`). User commit `7718119` deliberately added
   unchecked exceptions to `throws` clauses everywhere — leave them. Mongo closed-client ISE / Cassandra
   65,535-statement batch-limit ISE are documented conventions. DynamoDB IAE-for-null `@throws` convention kept.
   The Mongo `{"_id": {"$oid": ...}}` extended-JSON example form is correct — do not change it.

## Known library traps (verify against these)
- abacus-common 8.1.0 `N.convert(x, T)` returns x unchanged when `T.isInstance(x)` — REGISTERED converters are
  skipped (BigQuery FieldValueList is-a List; `N.convert(driverRow, Object.class)` returns the raw row).
- `N.asList/asSet/asMap` return IMMUTABLE collections. `ContinuableFuture.map(func)` re-runs `func` on EVERY
  `get()` and wraps its failure in ExecutionException; then*Async drop one ExecutionException layer.
- `N.convert(ByteBuffer, byte[].class)` → null; `N.convert(byte[], ByteBuffer.class)` → position==limit buffer.
- `Dataset.forEach` passes a `DisposableObjArray`, not a bean.
- abacus-query 4.9.4: parent `from(...)` rejects non-QUERY ops (CqlBuilder renders column DELETE itself);
  comment-only `Filters.expr("/* c */")` rejected; naming conversion skips quoted identifier segments.
- Getter-only props on an `@Entity` SUPERCLASS appear in a plain subclass's propInfoList (8.1.0); reading them
  via `PropInfo.setPropValue` throws UOE. The project's skip test: `propInfo.field == null &&
  propInfo.jsonXmlExpose == JsonXmlField.Direction.SERIALIZE_ONLY`.
- `Map.containsKey(String)` on a caller-supplied map can throw ClassCastException (TreeMap<Integer,..>) / NPE.
- Library sources extracted at `$LIB/src-ac-8.1.0/` (abacus-common) and `$LIB/src-aq-4.9.4/` (abacus-query).

## Scope
- **Bugs (correctness)** are the goal — highest bar: name the failing input/state and the wrong outcome, and
  prove it (probe or failing test). FIX it and add a RED→GREEN regression test.
- Wrong/misleading exception or log message text — fix.
- Javadoc: fix only where it is factually WRONG about behavior (contradicts the code, phantom/missing `@throws`
  for a real exception, example whose `// returns` comment is false, broken `{@link}`). No rewording for taste —
  the docs have had many passes.
- Performance/simplification: only real, zero-risk wins; otherwise report, don't apply.

## Rules for EDITING (strict)
1. Edit ONLY the main-source files in your slice. For tests, edit ONLY the test classes listed for your
   slice (a few are shared between slices — always Read the file immediately before each Edit, and put
   your new tests in ONE contiguous block near the end of the class, with a `// ---- 2026-10-02 slice<X> ----`
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
SCRATCH = `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/a890f6d9-f4c8-4716-9768-805309f546ec/scratchpad`
PREV    = `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/510410a4-7dfb-4e8a-87dc-3c209e4fae33/scratchpad`
  (09-29 round: `notes-slice<X>.md`, `probe-slice<X>/`)
LIB     = `C:/Users/haiyangl/AppData/Local/Temp/claude/C--Users-haiyangl-Landawn-abacus-da/20a80222-594d-4731-903d-c95ca3e4c906/scratchpad`
  (extracted library sources `src-ac-8.1.0/`, `src-aq-4.9.4/`)
- Compile all main sources to a private dir: `sh $SCRATCH/tools/compile.sh <yourtag>` → errors then
  `COMPILE OK`/`COMPILE FAILED`. Output classes: `$SCRATCH/out/<yourtag>`. If it fails in a file you do NOT
  own, another agent is mid-edit — wait ~1 min and retry (don't touch their file).
- Compile + run specific test classes (optionally `#method`):
  `sh $SCRATCH/tools/test.sh <yourtag> <TestClassSimpleNameOrFqcn>[#method] ...` → failures and
  `TESTS found=.. succeeded=.. failed=..`. Use your UNIQUE tag (`slice<X>`).
- Probes: write under `$SCRATCH/probe-slice<X>/`, compile with
  `javac -J-Xmx512m -cp "$SCRATCH/out/slice<X>;$(cat $SCRATCH/testcp.txt)" -d ...` and run with
  `java -Xmx512m -XX:+UseSerialGC ...` (`testcp.txt` = full test classpath, `;` separator; use `cygpath -m` for paths).
- Driver ground truth: jars in `~/.m2/repository` (`javap -c -p`, or unzip `-sources.jar`). Check
  `testcp.txt` for exact versions. NOTE `cassandra/` = DataStax driver **v4**, `cassandra/v3/` = **3.x**;
  `aws/dynamodb/` = SDK v1, `aws/dynamodb/v2/` = SDK v2.
- Live services (Cassandra, Mongo, DynamoDB Local, Cosmos/BigQuery emulators, Neo4j) may or may not be up.
  Prefer adding tests to the OFFLINE (Mockito/pure-logic) class for your file. If only a live class fits and
  the service is down, add the test anyway and say "compile-verified only".
- **MEMORY IS TIGHT** on this machine (Windows commit limit nearly exhausted by other workloads). The tool
  scripts already cap heaps. Run ONE JVM at a time; never run test classes you don't need. If a JVM fails with
  "insufficient memory"/"Native memory allocation (malloc) failed", delete the hs_err_pid*/replay_pid* files it
  created, wait ~1–2 min, and retry (don't loop more than a few times — report it instead).

## Report (your final message — the main agent verifies it against the diff)
```
APPLIED
- [BUG|MSG|DOC][P1|P2|P3] file:line — what was wrong → what you changed.
  Evidence: <code excerpt / probe result / driver bytecode>. Test: <TestClass#method> (RED→GREEN) or "doc-only".
PROPOSED (not applied — needs a decision)
- ...
VERIFICATION
- compile.sh result; test.sh result for each touched test class (found/succeeded/failed).
PER-FILE VERDICT
- file: CLEAN | N fixes
```
Also write a short `$SCRATCH/notes-slice<X>.md` (what you read, probed, fixed — for the next round).
Be concise; no narration of what you read. Don't report things you checked and found fine beyond the
per-file verdict.

## Cross-cutting findings from earlier slices THIS round
(the main agent appends here as slices report — re-read this section before you finish)
- (environment) All Docker containers (cassandra, dynamodb-local, cosmos-db, redis, memcached) were stopped externally at
  ~07:06 local on 10-02 (not by this round's agents). Do NOT start/stop containers; treat those services as down
  (live tests abort; "compile-verified only" is acceptable). mongod (Windows service) is still up.
- (slice A) Exception messages that concatenate a `Class` object render "class com.x.Foo" ("Entity class class com.x.Foo
  must be annotated..."). Use `ClassUtil.getCanonicalClassName(cls)` (HBaseExecutor 09-22 precedent). Check your files'
  `"..." + cls` / `"{}", cls` messages.
- (slices N/P) `Beans.isBeanClass` is true for value types with getters/setters (GregorianCalendar, ByteBuffer); a "single
  bean parameter = named values" check must also require `N.typeOf(cls).isBean()`.
- (slice E) Mongo `query(Collection selectPropNames, …)`→Dataset left dotted select names ("address.city") all-null — now
  filled from the nested value (sync; reactive twin in progress). (slices E/F) groupByAndCount with a group field named
  "count" + non-Document rowType now throws IAE (key was silently overwritten). Delegating mappers/async need doc twins.
- (slice S) BigQuery TIME cells with fractional seconds couldn't be read into java.sql.Time — abacus conversion rejects
  "HH:mm:ss.ffffff". If your driver returns TIME as such text, check your Time conversion path.
