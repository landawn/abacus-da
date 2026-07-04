# Line-by-line review round 2026-07-04

Scope: ALL 51 classes under `abacus-da-all/src/main/java` (~78k lines). WS1 correctness bugs,
WS2 javadoc/comments/logs, WS3 naming/consistency/design. BC waived by user.

Baseline: HEAD `3bd5d21` (yesterday's committed round) + an **uncommitted in-progress round**
already in the working tree (23 files). Process: 12 read-only review agents (batches A–L, no
gaps/overlap), main agent sole editor + verifier. Every agent finding independently re-traced in-code
before any edit.

## Uncommitted working-tree changes (verified this session, KEPT)

All re-verified in-code and sound:
- **BigQueryExecutor.toMapValue** nested-`FieldValueList` → `toMap(field.getSubFields(), fvl, supplier)`
  (matches the repeated-record branch). CORRECT.
- **BigQueryExecutor** ctor eagerly rejects unsupported `NamingPolicy` (IAE); `BigQueryExecutorTest2`
  updated. CORRECT (guards :269-283).
- **AnyScan** `setStartStopRowForPrefixScan(byte[])`→`(Object)` + `setRowPrefixFilter(Object)` widening.
- v2 `DynamoDBExecutor.getItem` eager `checkArgNotNull(targetClass)`; Mongo rowType guard; DDB
  `in()`/ConditionBuilder `HashMap`→`LinkedHashMap`; misc doc/naming (DDB @throws, `execute`→`resultSet`,
  `updateOptions`→`options`, MongoDBBase license header, AnyUtil sizing).
- **search stubs gained `private` ctors** — REGRESSION (see FIX-1 below); the rest are sound.

## Findings ledger (verified) + disposition

Legend: FIXED / DECISION (awaiting user) / NOTED (verified, left by design or low-value).

### Main-thread discoveries (regressions introduced by the uncommitted round)
- **FIX-1 [High, Build] search stubs** — the uncommitted round changed Elasticsearch/Lucene/Solr
  ctors from implicit package-private to explicit `private`, but the committed tests
  `*ExecutorTest.testInstantiateViaPackageAccess` do same-package `new X()` → **test compilation broke**
  (`private access`). Restored package-private ctor (kept explicit + comment) and reverted the
  "cannot be instantiated" Javadoc over-claim. Verified: test-compile green, all 3 test classes pass.
- **FIX-2 [Low, Doc/doclint] AnyScan:50** (batch G1) — class-Javadoc `{@link #setStartStopRowForPrefixScan(byte[])}`
  dangled after the uncommitted `byte[]`→`Object` signature change (line 1172 already used the `(Object)`
  form). Retargeted to `(Object)`.

### Batch A — DynamoDB v1
- **A1 [High, Doc/design] AutoCloseable** — `DynamoDBExecutor` documents "AutoCloseable support" (:102) +
  a try-with-resources example (:3416) but does not `implements AutoCloseable`; the example won't compile.
  → **DECISION** (recurring; user previously chose minimal when BC mattered — BC now waived).
- A2 [Low, Doc] `scan(String,…)` stream family `@throws IAE if tableName null/empty` is lazy/SDK-side,
  not eager. → NOTED (low value; 6 sync + 6 async; consistent with the lazy-stream contract). Left.

### Batch B — DynamoDB v2
- **B1 [Low, Doc] AsyncDynamoDBExecutor v2:92** class-doc "every public method returns CompletableFuture"
  overstatement (mapper/accessor/close excepted). → FIXED.
- B2 [Low] `createRowMapper:1479` dead `rowClass==null` branch; dead-store `newQueryRequest` (4 sites).
  → NOTED (harmless, symmetric; leave).
- B3 [Low, Naming] async field `dynamoDBClient` vs accessor `dynamoDBAsyncClient()`. → NOTED (internal).
- asKey/asItem/asUpdateItem `@throws IAE`-for-null (unconfirmed N.asMap null-key) → known package
  convention (memory: do-not-mass-rewrite). Left.

### Batch C — Mongo sync
- **C1 [Low, Design] MongoCollectionMapper** omits `queryForSingleNonNull` though `queryForSingleValue`
  is present and both siblings expose it. → DECISION (additive parity method).
- C2 [Info] mapper `updateOne(String,T)` doc omits empty-payload IAE case. → NOTED (lighter mapper-doc
  style). Left.
- Executor otherwise CLEAN.

### Batch D — Mongo async / base
- **D1 [Low, Doc/msg] MongoDBBase.readRow:1153** — `"Unsupported target type: "+rowType` is misleading:
  that `else` is reached only when `rowType` IS a supported scalar but `row.size() > 2`. Reworded to
  `"Cannot read a document with N fields into single-value type: …"` (mirrors the BigQuery readRow fix +
  the `toList` sibling wording). → FIXED.
- D2 [Info] `toDocument` bean null-property skip + dead `isForUpdate` param. → by-design. NOTED.
- **D3 [VeryLow, Doc] AsyncMongoCollectionExecutor:98** class-Javadoc continuation mis-indented. → FIXED.

### Batch E — Mongo reactive
- **E1 [Low, Bug/drift] reactivestreams/MongoCollectionExecutor findFirst** (:835, :861) — the two
  terminal overloads lacked `checkArgNotNull(rowType)`, so a null rowType silently produced an empty
  `Mono` instead of the eager IAE thrown by the sync sibling and every other reactive read. → FIXED
  (guard added to both + `@throws` docs) + regression test `testFindFirstWithNullRowTypeThrowsIAE`.
- E2 [Info] reactive `mapReduce` executor doc omits IAE for null functions (mapper documents it).
  → NOTED (minor). Left.

### Batch F — HBase executors
- **F1 [Low, Bug/consistency] HBaseExecutor.coprocessorService(Map):2759** masked JVM `Error` via
  `ExceptionUtil.toRuntimeException(e,true)`, while its 3 sibling coprocessor methods explicitly rethrow
  `Error` unchanged ("Don't mask JVM errors"). Added the same `if (e instanceof Error) throw error;`
  passthrough. → FIXED.
- AsyncHBaseExecutor CLEAN.

### Batch G — HBase Any-query
- G1 → FIX-2 above.
- G2 [Info] AnyScan redundant `setColumnFamilyTimeRange` overrides + `minTimestamp/maxTimestamp` vs
  `minStamp/maxStamp` param drift. → NOTED (no runtime impact). Left.
- AnyGet/AnyQuery/AnyOperation/AnyOperationWithAttributes/ColumnFamily CLEAN.

### Batch H — HBase Any-mutation — CLEAN (all 6 files).

### Batch I — Cassandra base
- **I1 [Low, Doc] CassandraExecutorBase** batchUpdate:1010 / batchDelete:1311 / batchDelete:1345 —
  `@throws` omitted the "first element is null" case the code enforces (async siblings document it).
  → FIXED (added to all 3).
- **I2 [Low, Doc] CassandraExecutorBase:1298** batchDelete wording "single batch operation" → "single
  DELETE whose WHERE matches every entity's primary key (IN over the key)" (matches async + code).
  → FIXED.
- **I3 [Low, Doc] AsyncCassandraExecutorBase** 10 typed `queryForXxx(Class,String,Condition)` wrappers
  (Boolean/Char/Byte/Short/Int/Long/Float/Double/String/Date) lacked `@throws IAE` for null/empty
  propName that the delegate enforces + sync documents. → FIXED (added to all 10).
- I4 [Info] async `batchUpdate(Collection,Collection,BT)` @throws lists only propNames (entities check
  is deferred to the delegate but behavior-equivalent). → NOTED. Left.

### Batch J — Cassandra executors v3/v4
- J1 [Low] `prepareStatement` dead `else if (N.isEmpty(parameters))` (v4:1679 / v3:1914). → NOTED
  (unreachable, symmetric, harmless).
- J2 [Low] redundant `values[i]==null?…` ternary in the conversion catch (v4:1780 / v3:2015). → NOTED.
- Both async files + readRow order + batch-building v3/v4 delegation CLEAN.

### Batch K — Cassandra support
- **K1 [Low, Doc] CqlBuilder** 6 delete/update `Dsl` methods (:1610/:1703/:1729/:1761/:1851/:1880)
  hardcoded "snake_case naming policy" though `Dsl` is policy-agnostic and the class uses the corrected
  "per the DSL's naming policy (snake_case in the PSC examples shown)" wording elsewhere. → FIXED (all 6).
- ParsedCql / ResultSets / CqlMapper CLEAN.

### Batch L — BigQuery / Cosmos / Neo4j / AnyUtil / 11 stubs — CLEAN.

## Fixes applied (main source: 10 files; tests: 1 file)

| # | File | Change | WS |
|---|------|--------|----|
| 1 | search/ElasticsearchExecutor, LuceneExecutor, SolrExecutor | ctor `private`→pkg-private + doc | Bug(build) |
| 2 | hbase/AnyScan | :50 `@link (byte[])`→`(Object)` | Doc |
| 3 | aws/dynamodb/v2/AsyncDynamoDBExecutor | :92 class-doc scope fix | Doc |
| 4 | mongodb/MongoDBBase | :1153 readRow message | Doc/msg |
| 5 | mongodb/AsyncMongoCollectionExecutor | :98 indentation | Doc |
| 6 | mongodb/reactivestreams/MongoCollectionExecutor | findFirst rowType guard ×2 + docs | Bug |
| 7 | hbase/HBaseExecutor | :2759 coprocessor Error passthrough | Bug |
| 8 | cassandra/CassandraExecutorBase | I1 ×3 @throws + I2 wording | Doc |
| 9 | cassandra/AsyncCassandraExecutorBase | I3 ×10 @throws | Doc |
| 10 | cassandra/CqlBuilder | K1 ×6 naming-policy wording | Doc |
| T | reactivestreams/MongoCollectionExecutorTest | +testFindFirstWithNullRowTypeThrowsIAE | test |

## Verification gates
- `mvn -o -pl abacus-da-all compile` = SUCCESS.
- `mvn -o -pl abacus-da-all clean test-compile` = SUCCESS (cleared stale error-stub classes).
- `mvn -o -pl abacus-da-all javadoc:javadoc` = exit 0, no doclint warnings on changed files.
- Targeted tests (search stubs ×3, MongoDBBase, reactive executor+mapper) = green.
- `mvn -o javadoc:javadoc` after AutoCloseable + mapper changes = exit 0, no doclint warnings.
- **Full suite = 3443 tests, 0 Failures, 1 Error, 2 Skipped, BUILD SUCCESS (exit 0).** The 1 error is
  the known-flaky live-Mongo `MongoDBExecutorTest.test_update_async` (concurrent tests share one live
  collection; NPE on a null doc) — **passes in isolation** (verified this session, exit 0) and is
  untouched by any change in this round. Baseline 3441 + 2 new regression tests
  (`testFindFirstWithNullRowTypeThrowsIAE`, `testqueryForSingleNonNull`) = 3443.

## Design/naming decisions — RESOLVED by user + APPLIED
1. **AutoCloseable → user chose "delete the examples + prose"** (NOT add the interface). APPLIED:
   converted the 11 executor-typed `try (Executor e = new Executor(...)) {…}` example blocks to the
   explicit `Executor e = …; try {…} finally { e.close(); }` form and removed the executor-AutoCloseable
   prose across DynamoDBExecutor v1, v2-sync, v2-async, CassandraExecutor v4, v3, HBaseExecutor. The
   legitimate `Stream`/`ResultScanner`/`Table`/`MongoCursor` try-with-resources examples (those types ARE
   AutoCloseable) were left untouched. Verified: 0 executor-typed `try (` + 0 "AutoCloseable support"/"is
   AutoCloseable" prose remain; compile + doclint clean.
2. **MongoCollectionMapper.queryForSingleNonNull → user chose "add it".** APPLIED: added
   `queryForSingleNonNull(propName, filter, valueType)` delegating to the executor (mirrors
   `queryForSingleValue`), + parity test `MongoCollectionMapperTest.testqueryForSingleNonNull`.
