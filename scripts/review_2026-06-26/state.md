# Three-Workstream Review — 2026-06-26

Branch: main. HEAD at start: f8ac230. Working tree had uncommitted WIP (CqlBuilder Dsl-constant
move into nested class + dslCache; CassandraExecutorBase import update; test import updates).

Ledger path: scripts/review_2026-06-26/state.md (NOT git-ignored; prior review_* dirs are tracked).

## Baseline
- `mvn -o -pl abacus-da-all compile` = SUCCESS (working tree compiles).
- `mvn -o -pl abacus-da-all test -Dtest=CqlBuilderTest` (BEFORE fix) = Tests run 513, Failures 13,
  Errors 495 — ALL from `CqlBuilder$Dsl` static-init NPE (see CRIT-1). Confirms WIP is broken.

## Workstream 1 — Bug fixes

### CRIT-1 [FIXED, RED→GREEN] CqlBuilder$Dsl static-init NPE (dslCache null)
- File: cassandra/CqlBuilder.java
- The WIP moved the 11 DSL constants (SCCB/ACCB/LCCB/PSB/PSC/PAC/PLC/NSB/NSC/NAC/NLC) into the
  nested `Dsl` class. Each constant's initializer calls `Dsl.forDialect(...)`, which dereferences
  `dslCache.get(sqlDialect)`. But `dslCache` was declared AFTER the constants (line 1287), so at
  constant-init time it is still null → NPE → ExceptionInInitializerError → class never loads.
- The code's own comment said dslCache should be "Declared before the predefined constants" — the
  field was simply placed wrong.
- Trace/repro: CqlBuilderTest → 495 Errors, all `Cannot invoke "java.util.Map.get(Object)" because
  "...Dsl.dslCache" is null` at forDialect(CqlBuilder.java:1328) ← <clinit>(:1225).
- FIX: moved `private static final Map<SqlDialect,Dsl> dslCache = new ConcurrentHashMap<>();` to the
  top of the Dsl class body (before the constants); removed the stray `// NOSONAR` there. The
  static{} block that populates it stays after the constants (runs after all field initializers).
- Severity: critical. Confidence: confirmed (reproduced RED, verified GREEN).
- VERIFIED GREEN: `CqlBuilderTest` = Tests run 513, Failures 0, Errors 0 (was 13F/495E).
- Also removed 3 stray `// NOSONAR` artifact lines left by the WIP edit (comment-only).

## Post-CRIT-1 baseline
- Full `mvn -o -pl abacus-da-all test` = **Tests run 3415, Failures 0, Errors 0, Skipped 2**
  (live services were reachable this run). No non-environmental failures. Matches known-good baseline.
  This is the authoritative baseline for the rest of the pass.

## Workstream 2 — CqlBuilder Javadoc (consequence of the WIP constant-move; comment-only)
- The 11 DSL constants moved from `CqlBuilder` to nested `CqlBuilder.Dsl`, breaking link targets:
  - Outer class doc (lines ~90-97): `{@link #SCCB}`...`{@link #NLC}` → `{@link Dsl#SCCB}`... (9 links, were
    resolving to non-existent CqlBuilder#X).
  - Dsl class doc (~1207): `{@link CqlBuilder#PSC/NSC/SCCB}` → `{@link #PSC/#NSC/#SCCB}`.
  - forDialect doc (~1315): `{@link CqlBuilder#PSC}`/`{@link CqlBuilder#NSC}` → `{@link #PSC}`/`{@link #NSC}`.
  - forDialect `@return` had said "a **new** Dsl" → neutral "a Dsl" (forDialect may now return a cached
    canonical instance; see note below).

## Observations (NOT bugs — recorded, not fixed)
- `Dsl.dslCache` is keyed by `SqlDialect`, which has NO equals/hashCode (plain class, identity semantics)
  and package-private fields. So the cache only hits for the 11 registered instances; `forDialect` does
  not put-on-miss, so it's bounded (no leak) and effectively returns `new Dsl` for any externally-built
  dialect. Harmless; redesign (value-keying) is out of scope (would be a refactor). Left as-is.

## Stubs (all CLEAN — empty/placeholder, no logic)
- aws/AWSRDSUtil, aws/AWSS3Util, blink/DataSetUtil, blink/DataStreamUtil, hadoop/HDFSUtil,
  hadoop/HadoopUtil, search/ElasticsearchExecutor (commented skeleton), search/LuceneExecutor,
  search/SolrExecutor, spark/DStreamUtil, spark/DatasetUtil.

## Multi-agent review of remaining 40 real files (read-only agents; main agent edits)

### Batch 1 (Cassandra core / v3+support / BigQuery+Cosmos+Neo4j) — DONE
BigQuery/Cosmos/Neo4j + CqlMapper/ParsedCql/ResultSets/v3 = CLEAN (only known non-bugs + harmless
dead code: Cosmos toSqlQuerySpec parameterization branch unreachable [RAW_SQL inlines], v3
prepareStatement empty-params else-branch unreachable — both left).

### WS2-CASS-1 [FIXED, comment-only] Async cassandra: mapper-thrown exceptions documented as WRAPPED
Empirically PROVEN (ran ContinuableFuture.completed("x").map(throw X).get()): a mapper-thrown
exception surfaces from get() DIRECTLY/UNWRAPPED — get() threw DuplicateResultException (cause=null)
and NullPointerException (cause=null), NOT ExecutionException. Mechanism: ContinuableFuture.map.get()
(abacus-common 7.8.2 :1185) runs the mapper inline and `throw ExceptionUtil.toRuntimeException(e,true)`,
which returns a RuntimeException as-is (RUNTIME_FUNC = e->(RuntimeException)e). DuplicateResultException
extends IllegalStateException (RuntimeException).
- The get/gett family (8 methods) detect duplicates via fetchOnlyOne INSIDE `execute(cp).map(...)`
  (AsyncCassandraExecutorBase:260,403), so DuplicateResultException is unwrapped — but all 8 example
  blocks + the first @return said "completes exceptionally; get() rethrows it wrapped" /
  "throws ExecutionException (cause DuplicateResultException)" / "catch(ExecutionException){getCause()}".
  WRONG. Fixed to: get() throws DuplicateResultException directly; catch(DuplicateResultException).
- queryForSingleNonNull (Optional): `Optional.of(readFirstColumn(...))` in the mapper → NPE on null
  value, unwrapped. Base @return ×2 (Condition + query variants) said "completes exceptionally with
  NPE" (examples were already correct) → fixed to "get() throws NPE directly". Concrete overrides in
  v4 AsyncCassandraExecutor + v3 AsyncCassandraExecutor said "empty Optional if ... value is null"
  (prose+@return) → fixed to NPE.
- NOT changed (verified CORRECT): all driver-failure examples (insert/update/delete/exists/count/
  list/query/findFirst/execute) — those exceptions originate in the SOURCE future's async task, so
  ContinuableFuture.this.get() inside map.get() rethrows them as ExecutionException (genuinely
  wrapped). findFirst takes first row only (no dedup), so no DuplicateResultException there.
- Files: AsyncCassandraExecutorBase.java (8 blocks + 3 @return), AsyncCassandraExecutor.java (v4,
  prose+@return), v3/AsyncCassandraExecutor.java (prose+@return). Severity med, confidence confirmed.

### WS2-CASS-2 [OPEN, unverified — NOT changed] execute/stream((Statement)null) "(cause NPE)" examples
AsyncCassandraExecutorBase example lines ~1713 (stream(Class,(Statement)null)) and ~2914
(execute((Statement)null)) claim `.get()` throws ExecutionException(cause NPE). Concrete execute wraps
`session().executeAsync(statement)` — whether executeAsync(null) throws NPE eagerly (→ doc wrong) or
returns a failed future (→ doc correct, wrapped) depends on DataStax driver internals (live-only to
verify). Left as accepted/unverified.

### Batch 2 (HBase ×3) — DONE. WS1: none. WS3: none. WS2 fixes (all comment-only, confirmed):
- AnyGet.java: 7 `{@code}` example lines used HTML entities `&lt;`/`&gt;` (render literally as `&lt;`);
  fixed to raw `<`/`>` (matches AnyScan's convention). Lines 448,452,565,956,1094,1099,1103.
- AnyIncrement.setTimeRange `@throws` dropped "(wraps the underlying IOException)" — TimeRange throws
  IAE directly; the catch(IOException) path is dead (matches AnyAppend.setTimeRange's accurate wording).
- AnyPut.addColumn(String,String,Object) and (String,String,long,Object): `@param qualifier` "must not
  be null" → "may be null to denote an empty qualifier" (toFamilyQualifierBytes(null)→null; byte[]
  overload already documents it nullable).
- AnyPut.add(Cell) + AnyAppend.add(Cell): added `@throws IllegalArgumentException if the cell's family
  is null or empty` (Mutation.add(Cell) family check; AnyDelete.add(Cell) already documented it).
- AsyncHBaseExecutor class doc: map() "runs on the completing thread" → "runs lazily on the thread
  that calls get()" (verified vs ContinuableFuture.map source).
- NOT changed: AsyncHBaseExecutor line ~106 blanket error-handling note incomplete for lazy scan
  (per-method scan docs already precise — low priority). All KNOWN NON-BUGS re-confirmed.

### v3 finding (low) — NOT changed, internally consistent
v3/CassandraExecutor queryForSingleValue/findFirst @throws IAE overstates (null class not validated;
null query w/o params → NPE not IAE). Matches the long-standing prepareStatement quirk already
documented as NPE on execute(String) (deliberate-design list). queryForSingleNonNull omits the clause.
Left to avoid churn (consistent with prior passes' decision on this exact quirk).

### Batch 3 (Mongo ×2, DynamoDB ×2) — DONE. WS1: none confirmed. Fixes applied (comment/message only):
- **[WS2, med] AsyncMongoCollectionExecutor: `.thenAcceptAsync(...)` × 6** (insertOne/insertMany/
  bulkInsert examples) — ContinuableFuture has NO thenAcceptAsync/thenAccept (only thenRunAsync
  {Runnable,Consumer,BiConsumer}, thenCallAsync, map). Examples wouldn't compile. → `.thenRunAsync(`.
  Verified by reading abacus-common 7.8.2 ContinuableFuture source.
- [WS2] reactive MongoCollectionExecutor.queryForDate @throws `{@code rowType}` (dangling) → `valueType`.
- [WS3] reactive MongoCollectionExecutor.checkResultClass message `getter\setter ... Map.class\Document.class`
  (backslashes) → forward slashes (matches sync MongoDBBase wording).
- [WS2, med] DynamoDB v1 toItem/toUpdateItem(×2) "Null properties are excluded" — only true for BEAN
  inputs; Map/Object[] write null as a NULL AttributeValue (toAttributeValue(null)→withNULL(TRUE),
  traced). Scoped the claim in all 3 doc blocks.
- [WS2, med] DynamoDB v1 Mapper.scan(Map)/scan(List,Map) scanFilter "must not be null" + @throws IAE →
  "may be null to apply no filter" + dropped @throws (null IS valid; matches executor overloads + the
  2026-06-23 async-scan fix). withScanFilter(null) accepted by SDK.
- [WS3] DynamoDB v1 asItem/asUpdateItem(Object...) IAE message "must be property name-value pairs, a
  Map, or an entity with getter/setter methods" → "must be name-value pairs (an even number of
  arguments)" (×2; varargs only accept even-count pairs — matches @param/@throws).
- NOT changed (verified, left): Mongo valueType-null @throws (CONDITIONAL — N.convert(value,null)→IAE
  only when a row matches; imprecise, not clearly wrong); async queryForDate ClassCastException @throws
  (borderline-spurious, low value); async:1066 misplaced trailing comment (cosmetic); reactive query
  Dataset overloads missing checkResultClass @throws (real sync/reactive divergence but 8 overloads,
  low value); reactive groupBy/groupByAndCount(Collection,Class) dead `groupFields` local (KNOWN
  harmless, removal would be a refactor); v2 async list/stream/scan(ScanRequest,Class) defer the
  documented IAE (surfaces via the future, not eager like async query) — consistent with the established
  "v2 async defers work" design, not a confirmed bug.
- v2 sync/async + v1 AnyUtil + reactive MongoDB/mapper + Mongo MongoDB/MongoDBBase = CLEAN.

## FINAL VERIFICATION
- `mvn -o -pl abacus-da-all compile` = SUCCESS.
- `mvn -o -pl abacus-da-all test` (full) = **Tests run 3415, Failures 0, Errors 0, Skipped 2** — matches
  baseline exactly; zero non-environmental failures. No test asserts the two changed exception-message
  strings (grepped). CqlBuilderTest 513/0/0 (was 13F/495E before CRIT-1 fix).

## SUMMARY OF CHANGES (all files under abacus-da-all/src/main/java)
- WS1 code fix: 1 (CRIT-1 CqlBuilder static-init NPE — blocked the whole suite; minimal field reorder).
- WS2 Javadoc: CqlBuilder (broken {@link} after constant-move, forDialect @return), cassandra async
  (mapper-thrown exception wrap-vs-direct: 8 get/gett blocks + queryForSingleNonNull across base+v4+v3),
  HBase (AnyGet {@code} entities ×7, AnyIncrement setTimeRange, AnyPut qualifier ×2 + add(Cell) @throws,
  AnyAppend add(Cell) @throws, AsyncHBaseExecutor map() thread), Mongo (thenAcceptAsync ×6, reactive
  queryForDate param), DynamoDB v1 (toItem/toUpdateItem null-handling ×3, Mapper.scan scanFilter ×2).
- WS3 messages: reactive Mongo checkResultClass (backslash→slash), DynamoDB v1 asItem/asUpdateItem ×2.
- No new regression tests needed (CRIT-1 already covered by CqlBuilderTest going RED→GREEN; all other
  changes are comment/message-only).

## Scope notes
- The CqlBuilder constant-move (CqlBuilder.X → CqlBuilder.Dsl.X) is the user's pre-existing WIP, NOT done
  by this pass; this pass only fixed the static-init bug it introduced + the consequent broken Javadoc.
- No dependency bumps, no new public API, no refactors. (Did NOT remove the known-harmless dead
  groupFields locals — that would be a refactor out of scope.)
