# Multi-agent line-by-line review — 2026-06-25

Module: `abacus-da-all` (51 classes under `src/main/java`). Working tree clean at start (HEAD `e78df7a`).
Baseline: `mvn -o clean compile -pl abacus-da-all` = BUILD SUCCESS. Full offline test baseline = **3415 tests, 0 Failures, 1 Error, 2 Skipped** (the 1 Error = known flaky live-Mongo `MongoDBExecutorTest.test_update_async`; no non-environmental failures — verified via `grep '<<< (FAILURE|ERROR)!' | grep -vEi 'cassandra|dynamodb|neo4j|AbacusDATestSuite'` = empty). Identical to 2026-06-24 baseline.
Ledger path: `scripts/review_2026-06-25/state.md` (tracked in git, consistent with prior passes).

Workstreams: (1) bug fixes [HIGH], (2) Javadoc/comments [comment-only], (3) logging/exception messages [LOW].
Prior ledgers honored: `scripts/review_2026-06-24/state.md`, `scripts/review_2026-06-23/state.md` + memory `abacus-da-review-status`, `abacus-da-deep-review-2026-06` (known non-bugs NOT re-litigated).

## Known false-positives / deliberate design (do NOT re-flag — carried from prior passes)
- Cassandra v3/v4 `prepareStatement`: null multi-param bind → `N.defaultValueOf(type)` (deliberate, identical both versions).
- v4 `protocolCodeDataType` total over 26 codes → no `javaClass==null` guard needed (unreachable).
- HBase `Any*` equals/hashCode are row-key-only (Get/RowMutations/Increment) or reference-identity (Scan/Query/OWA); `compareTo` intentionally inconsistent with equals (mirrors HBase).
- Live-map exposure (`AnyMutation.getFamilyCellMap`, `AnyRowMutations.val`) + by-ref `of(...)` are documented contracts.
- Cassandra `toList` passthrough guard = `targetClass.isAssignableFrom(Row.class)` (do NOT flip).
- `MongoDBBase.objectId2Filter` `{"_id":{"$oid":..}}` extended-JSON form is correct (recurring false positive — leave).
- DynamoDB IAE-for-null SDK convention kept package-wide.
- `CqlMapper` ids "used as map key as-is; not validated" (deliberate); all 4 `add(...)` overloads void + reject-dup.
- `ParsedCql.namedParameters()` returns `Map<Integer,String>` (locked; executors index by `.get(i)`).
- Reactive Mongo `.map→.mapNotNull` migration complete; `groupBy/groupByAndCount(Collection,Class)` unused `groupFields` local = known dead code, not a bug.
- DynamoDB v1 async `thenRunAsync(item->…)` has a `Consumer` overload (false positive).
- Cosmos `prepareQuery` alias `c` qualification is the fix (keep); `if(count>0)limit` branch dead (moot).
- Cassandra dir naming: `cassandra/`=DataStax v4, `cassandra/v3/`=3.x (opposite of dir name).
- AnyPut/AnyDelete `.of((Object)null)` → NPE (single-arg HBase ctor derefs row.length) is CORRECT — recurring NPE↔IAE flip, do NOT change.
- v2 async list/query/stream/scan blocking `.get()` pagination inside thenApplyAsync — accepted tradeoff, not a bug.
- v4/v3 CassandraExecutor.toEntity non-Row value set-as-is (no N.convert) — intentional; only nested Row→bean converted.
- Cassandra `logger.debug("..{}..{}..{}..", i, from, to, e)` trailing Throwable → stacktrace (SLF4J idiom), not discarded.
- N.convert(Object, Type) short-circuits when raw container class assignable (use `type.valueOf(N.toJson(value))` to force element conversion — already done in DynamoDB v1 + BigQuery toEntity).

## Coverage partition (each file → exactly one agent; reused from 2026-06-24, 51 files)
- A [stubs]: spark/{DatasetUtil,DStreamUtil}, search/{Elasticsearch,Lucene,Solr}Executor, blink/{DataStreamUtil,DataSetUtil}, aws/{AWSRDSUtil,AWSS3Util}, hadoop/{HadoopUtil,HDFSUtil} — 11
- B [hbase-mutation]: hbase/{AnyOperation,AnyOperationWithAttributes,AnyMutation,AnyPut,AnyDelete,AnyIncrement,AnyAppend,AnyRowMutations}, hbase/annotation/ColumnFamily — 9
- C [hbase-query/exec]: hbase/{AnyQuery,AnyGet,AnyScan,HBaseExecutor,AsyncHBaseExecutor} — 5
- D [cqlbuilder]: cassandra/CqlBuilder — 1
- E [cassandra-v4]: cassandra/{CassandraExecutor,CassandraExecutorBase,AsyncCassandraExecutor,AsyncCassandraExecutorBase} — 4
- F [cassandra-v3/support]: cassandra/v3/{CassandraExecutor,AsyncCassandraExecutor}, cassandra/{ResultSets,ParsedCql,CqlMapper} — 5
- G [mongo-sync]: mongodb/{MongoDBBase,MongoDB,MongoCollectionExecutor,MongoCollectionMapper,AsyncMongoCollectionExecutor} — 5
- H [mongo-reactive]: mongodb/reactivestreams/{MongoDB,MongoCollectionExecutor,MongoCollectionMapper} — 3
- I [dynamodb-v1]: aws/dynamodb/{DynamoDBExecutor,AsyncDynamoDBExecutor}, aws/AnyUtil — 3
- J [dynamodb-v2]: aws/dynamodb/v2/{DynamoDBExecutor,AsyncDynamoDBExecutor} — 2
- K [bq/cosmos/neo4j]: gcp/BigQueryExecutor, azure/CosmosContainerExecutor, neo4j/Neo4jExecutor — 3
Total = 51.

## Batch status
| Batch | Groups | Status |
|-------|--------|--------|
| 1 | G (mongo-sync), H (mongo-reactive), D (CqlBuilder) | DONE — 0 actionable |
| 2 | E (cassandra-v4), F (cassandra-v3/support), I (dynamodb-v1) | DONE — 0 actionable |
| 3 | J (dynamodb-v2), K (bq/cosmos/neo4j), C (hbase-query/exec) | DONE — 1 fix (J) |
| 4 | B (hbase-mutation), A (stubs) | DONE — 0 actionable |

## Test verification
- Baseline (before): **3415 tests, 0 Failures, 1 Error, 2 Skipped** (1 Error = flaky live-Mongo `test_update_async`).
- After fix: **3415 tests, 0 Failures, 0 Errors, 2 Skipped** (`mvn -o test -pl abacus-da-all`; flaky test passed this run). No non-environmental failures. No regression. Change is comment-only so no new test required.

## RESULT — 1 actionable fix (of 51 files); codebase re-confirmed mature
1. **[WS2 comment-only] `aws/dynamodb/v2/DynamoDBExecutor`** — `extractData(QueryResponse,int,int)` (~:1883) and `extractData(ScanResponse,int,int)` (~:1941) were missing `@throws NullPointerException`. Both deref `queryResult.items()`/`scanResult.items()` (NPE on null) BEFORE the offset/count IAE check in the `extractData(List,int,int)` delegate — yet their own single-arg siblings (`extractData(QueryResponse)`:1850, `extractData(ScanResponse)`:1909) document `@throws NPE`, and the QueryResponse 3-arg's own Usage-Example (:1874) shows `extractData((QueryResponse) null, 0, 1); // throws NullPointerException`. Added the missing `@throws NullPointerException` (before the existing IAE clause) to both. Comment-only, no behavior change, no test needed. `mvn -o compile -pl abacus-da-all` = BUILD SUCCESS.

## Findings ledger (all verified personally by main agent)

### Batch 1 (G=mongo-sync, H=mongo-reactive, D=CqlBuilder) — 0 actionable
- [G] CLEAN — all 5 files. Filter null-guards complete via executeQuery/driver chokepoint; resource cleanup via `.onClose(Fn.close(cursor))`; sync-vs-async @throws consistent.
- [H][FALSE-POSITIVE / dead code] reactive `MongoCollectionExecutor.executeQuery` (~:1980/1991) has a dead `filter == null ? coll.find() : coll.find(filter)` ternary AFTER the chokepoint `N.checkArgNotNull(filter,"filter")` (same in sync ~:2511). HARMLESS leftover of the chokepoint-guard implementation; not a bug/doc/logging issue; same class as prior-pass documented dead code (groupFields, Cosmos limit). LEAVE — removal is an out-of-scope refactor.
- [D][FALSE-POSITIVE] CqlBuilder IN/NotIn (:1035/:1088) `setParameter(propName + (i+1), ...)` does NOT pre-call `sanitizeNamedParameterName` while BETWEEN (:976) does. VERIFIED NOT A BUG: `setParameter`→`setParameterForNamedSQL`→`nextNamedParameterName`→`sanitizeNamedParameterName` (abacus-query AbstractQueryBuilder:5243) sanitizes ALL named params inside `setParameter`. BETWEEN pre-sanitizes ONLY because it builds a prefixed/capitalized name (`"min"+capitalize(...)`); without pre-sanitizing, the dot-strip in `sanitizeNamedParameterName` would eat the `min` prefix. IN/NotIn names need no pre-sanitization. → added to deliberate-design list.

### Batch 2 (E=cassandra-v4, F=cassandra-v3/support, I=dynamodb-v1) — 0 actionable
- [E][FALSE-POSITIVE] `CassandraExecutorBase.queryForSingleValue(Class targetClass, Class valueClass, String propName, Condition)` (:1995) `@throws IAE if targetClass/valueClass/propName null`. Agent claimed targetClass/valueClass throw a different type. VERIFIED EMPIRICALLY (Trace harness against compiled classpath): `N.convert(x, null)`→IAE ('cls' cannot be null); `CqlBuilder.NSC.select(...).from((Class)null)`→IAE ('entityClass' cannot be null, the builder guards entityClass — NOT the raw getBeanInfo NPE path). All three params→IAE. Doc ACCURATE.
- [E][KNOWN-EDGE, not actionable] `CassandraExecutor.queryForSingleValue/queryForSingleNonNull(valueClass, query, parameters)` (:1075/:1125) `@throws IAE if valueClass or query null`. valueClass null→IAE (N.convert, confirmed). query null: WITH params→IAE (`parseCql`→`ParsedCql.parse`→`checkArgNotNull`); WITHOUT params→NPE (`prepareStatement(query)` derefs `query.length()` at :1613). This is the same long-standing `prepareStatement(query)` NPE quirk already deliberately documented as NPE on `execute(String)` in a prior pass; the IAE doc is correct for the common parameterized path. Recurring NPE↔IAE edge the user flagged as low-value — LEAVE.
- [F] CLEAN — v3 CassandraExecutor+AsyncCassandraExecutor, ResultSets, ParsedCql (quote/brace tracking confirmed correct), CqlMapper.
- [I] dynamodb-v1 (DynamoDBExecutor, AsyncDynamoDBExecutor, AnyUtil) — agent's items (debug-log-not-in-javadoc, dual-pass toValue comment, Mapper batchGetItem null) were all self-concluded "no bug"/INFO noise; no genuine WS1/WS2/WS3 defect. CLEAN.

### Batch 3 (J=dynamodb-v2, K=bq/cosmos/neo4j, C=hbase-query/exec) — 1 fix (J)
- [J][WS2 FIXED] extractData(QueryResponse,int,int) + extractData(ScanResponse,int,int) missing `@throws NullPointerException` — see RESULT #1 above.
- [J] AsyncDynamoDBExecutor CLEAN (blocking `.get()` inside thenApplyAsync = known accepted tradeoff).
- [K][FALSE-POSITIVE / dead code] BigQuery `entityToCondition` (:1677) `if (N.isEmpty(conds))` is genuinely unreachable: line :1651 throws when `keyNames.isEmpty()`, and the `else` branch runs only when `size>=2`, so `conds` always has ≥2 elements. Harmless defensive code; removal is out-of-scope refactor (same class as prior-pass dead-code). LEAVE.
- [K] Cosmos + Neo4j CLEAN.
- [C] hbase AnyQuery/AnyGet/AnyScan/HBaseExecutor/AsyncHBaseExecutor CLEAN.

### Batch 4 (B=hbase-mutation, A=stubs) — 0 actionable
- [B] CLEAN — AnyOperation, AnyOperationWithAttributes, AnyMutation, AnyPut, AnyDelete, AnyIncrement, AnyAppend, AnyRowMutations, ColumnFamily. All known false-positives (Put/Delete `.of(null)`→NPE, offset/length→IAE, live-map exposure, row-key-only equals) re-confirmed correct.
- [A] CLEAN — all 11 stub files (spark/search/blink/aws-util/hadoop) confirmed empty placeholders (2–4 non-trivial lines each: class decl + private ctor, no logic).

## New false-positives appended to deliberate-design list (do NOT re-flag)
- CqlBuilder IN/NotIn named-param sanitization happens INSIDE `setParameter`→`nextNamedParameterName`→`sanitizeNamedParameterName`; BETWEEN's explicit pre-sanitize is needed only for its prefixed name. No inconsistency.
- CassandraExecutorBase 4-arg `queryForSingleValue/queryForSingleNonNull` `@throws IAE` for targetClass/valueClass/propName is ACCURATE (targetClass→builder `from` IAE; valueClass→`N.convert` IAE; propName→checkArgNotEmpty IAE) — empirically verified.
- `CassandraExecutor.queryForSingleValue(valueClass, query, parameters)` null `query` → IAE with params / NPE without params (prepareStatement(query) length deref). Same long-standing prepareStatement quirk; do not churn the @throws.
- reactive Mongo `executeQuery` dead `filter==null?...` ternary after chokepoint guard — harmless dead code, leave.
- BigQuery `entityToCondition` `if(N.isEmpty(conds))` after the `keyNames.isEmpty()` guard + size>=2 else-branch — unreachable harmless defensive code, leave.
