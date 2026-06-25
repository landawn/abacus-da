# Multi-agent line-by-line review — 2026-06-24

Module: `abacus-da-all` (51 classes under `src/main/java`). Working tree clean at start (HEAD `c67160a`).
Baseline: `mvn -o -pl abacus-da-all clean compile` = BUILD SUCCESS. Full offline test baseline measuring (bg).

Workstreams: (1) bug fixes [HIGH], (2) Javadoc/comments [comment-only], (3) logging/exception messages [LOW].
Prior ledgers honored: `scripts/review_2026-06-23/state.md` + memory `abacus-da-review-status` (known non-bugs NOT re-litigated).

## Known false-positives / deliberate design (do NOT re-flag — from prior passes)
- Cassandra v3/v4 `prepareStatement`: null multi-param bind → `N.defaultValueOf(type)` (deliberate, identical both versions).
- v4 `protocolCodeDataType` total over 26 codes → no `javaClass==null` guard needed (unreachable).
- HBase `Any*` equals/hashCode are row-key-only (Get/RowMutations/Increment) or reference-identity (Scan/Query/OWA); `compareTo` intentionally inconsistent with equals (mirrors HBase).
- Live-map exposure (`AnyMutation.getFamilyCellMap`, `AnyRowMutations.val`) + by-ref `of(...)` are documented contracts.
- Cassandra `toList` passthrough guard = `targetClass.isAssignableFrom(Row.class)` (do NOT flip).
- `MongoDBBase.objectId2Filter` `{"_id":{"$oid":..}}` extended-JSON form is correct (recurring false positive — leave).
- DynamoDB IAE-for-null SDK convention kept package-wide.
- `CqlMapper` ids "used as map key as-is; not validated" (deliberate); all 4 `add(...)` overloads now void + reject-dup (2026-06-24 change).
- `ParsedCql.namedParameters()` returns `Map<Integer,String>` (locked; executors index by `.get(i)`).
- Reactive Mongo `.map→.mapNotNull` migration complete; `groupBy/groupByAndCount(Collection,Class)` unused `groupFields` local = known dead code, not a bug.
- DynamoDB v1 async `thenRunAsync(item->…)` has a `Consumer` overload (false positive).
- Cosmos `prepareQuery` alias `c` qualification is the fix (keep); `if(count>0)limit` branch dead (moot).
- Cassandra dir naming: `cassandra/`=DataStax v4, `cassandra/v3/`=3.x (opposite of dir name).

## Coverage partition (each file → exactly one agent)
- A [stubs]: spark/DatasetUtil, spark/DStreamUtil, search/{Elasticsearch,Lucene,Solr}Executor, blink/{DataStreamUtil,DataSetUtil}, aws/{AWSRDSUtil,AWSS3Util}, hadoop/{HadoopUtil,HDFSUtil} — 11
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
| 1 | G, H | DONE (G,H reviewed+verified) |
| 2 | K, I, J | DONE (verified) |
| 3 | F, E, D | DONE (verified, 0 fixes) |
| 4 | B, C, A | DONE (verified, 0 fixes) |

### Batch 4 (B=hbase-mutation, C=hbase-query/exec, A=stubs) — 0 actionable
- [FALSE-POSITIVE — recurring, re-confirmed] AnyPut.of((Object)null) & AnyDelete.of((Object)null) docs say NPE; agent wanted IAE. VERIFIED: AnyPut(Object)→`super(new Put(toRowBytes(rowKey)))` (AnyPut:176) and AnyDelete(Object)→`super(new Delete(...))` (AnyDelete:160) use the SINGLE-ARG HBase ctor which derefs row.length → NPE. Docs CORRECT. (offset/length/boolean variants use checkRow→IAE; those docs already say IAE.) This NPE-vs-IAE flip is a recurring false positive — DO NOT change.
- [NON-ISSUE] AnyAppend.of(byte[],int,int):254 / AnyIncrement.of(byte[],int,int):209 IAE "Row buffer is null" — 3-arg offset/length path → Mutation.checkRow(byte[],int,int) → IAE w/ that exact message. Docs CORRECT (agent low-confidence, unfounded).
- [NON-ISSUE] setTimeRange `[min,max)` bounds, AnyDelete "maximum timestamp" wording — agent itself concluded "no change needed".
- CLEAN: AnyOperation, AnyOperationWithAttributes, AnyMutation, AnyRowMutations, ColumnFamily; AnyQuery, AnyGet, AnyScan, HBaseExecutor, AsyncHBaseExecutor; all 11 stub files (spark/search/blink/aws-util/hadoop).

## Test results
- Baseline (before): **3414 tests, 0 failures, 0 errors, 2 skipped** (offline, live svcs reachable).
- After fixes: **3415 tests, 0 Failures, 1 Error, 2 Skipped** (= baseline + 1 new regression test).
  - The 1 Error = `MongoDBExecutorTest.test_update_async:1005` NPE (`doc` null) — LIVE-Mongo integration flake, different class/package than edits; re-ran MongoDBExecutorTest = **17/0/0** (passes). NON-regression (per [[abacus-da-test-baseline]]).
- Targeted: BigQueryExecutorTest 145→**146** green; MongoDBExecutorTest rerun 17/0/0 green.

## RESULT — 2 actionable fixes (of 51 files); codebase confirmed mature
1. [WS1 code+test] BigQueryExecutor.update(Object,Set) — checkArgNotNull(entity) (NPE→IAE), regression added.
2. [WS2 comment] MongoCollectionExecutor.queryForDate(String,Bson,Class) — `@throws` dropped nonexistent "rowType" param.

## New false-positives appended to deliberate-design list
- reactive aggregate(List,Class) null-pipeline IAE is EAGER (driver `notNull("pipeline")` runs before Flux.from) — docs correct.
- AnyPut/AnyDelete `.of((Object)null)` → NPE (single-arg HBase ctor) is CORRECT — recurring flip, do not change.
- v2 async list/query/stream/scan blocking `.get()` pagination inside thenApplyAsync — accepted tradeoff, not a bug; non-blocking rewrite is out-of-scope.
- v4/v3 CassandraExecutor.toEntity non-Row value set-as-is (no N.convert) — intentional; only nested Row→bean is converted.
- Cassandra logger.debug("..{}..{}..{}..", i, from, to, e) trailing Throwable → stacktrace (SLF4J-style), not discarded.

### Batch 3 (E=cassandra-v4, F=cassandra-v3/support, D=CqlBuilder) — 0 actionable
- [FALSE-POSITIVE] v4 CassandraExecutor.toEntity:768 "type-check ordering bug" — conversion path (readRow) is intentionally only for nested Row→bean; non-Row driver-decoded values set as-is. IDENTICAL v3 pattern (:851) which F-agent independently confirmed CORRECT. Live-Cassandra tests green. Not a bug.
- [FALSE-POSITIVE] v4 CassandraExecutor:1767 logging — `logger.debug("..{}..{}..{}..", i, from, to, e)` 3 placeholders + trailing Throwable: abacus logger (SLF4J-style) logs trailing Throwable as stacktrace, NOT discarded. Idiomatic + message self-sufficient.
- [KNOWN-NIT] CassandraExecutorBase.entityToCondition missing-key message reports last missing key only — already recorded non-actionable in review_2026-06-23. Skip.
- [SKIP low-value] CqlBuilder TODO typo "is not support"(:1164), exception phrasing "doesn't include any element"(:1154), insert(Account.class) example column-count(:1508), iF() example clarity(:737) — all agent-rated low; CqlBuilder examples verified in prior passes; informal TODO. No edit.
- CLEAN: ParsedCql, CqlMapper, ResultSets, v3 CassandraExecutor+AsyncCassandraExecutor, AsyncCassandraExecutorBase, CassandraExecutorBase(logic), CqlBuilder(logic — 11 variants consistent).

### Batch 2 (K=bq/cosmos/neo4j, I=ddb-v1, J=ddb-v2)
- [WS1][FIXED+test] BigQueryExecutor.java:1293 `update(Object,Set<String>)` — missing `N.checkArgNotNull(entity)`; `prepareUpdate` deref `entity.getClass()` → NPE on null entity, contradicting `@throws` and siblings insert/update(Object)/delete(Object) (all IAE). Added guard + fixed `@throws NPE`→IAE. RED (NPE) → GREEN. Regression `BigQueryExecutorTest#testUpdateObjectWithKeys_NullEntityThrows`. BigQueryExecutorTest 145→146 green.
- [OUT-OF-SCOPE/flag] v2 AsyncDynamoDBExecutor list/query/stream/scan pagination calls `client.query(...).get()` (blocking) inside `thenApplyAsync` (lines ~1691/1816/1961/2304). Long-standing, identical across v1/v2 async; results correct. Converting to non-blocking composition is a broader rewrite → flagged, NOT fixed (guardrail). Not a correctness bug.
- [FALSE-POSITIVE] CosmosContainerExecutor `get(...)` `@see #readItem(...,Class)` is appropriate (absence-tolerant counterpart; prose already explains Optional wrapping).
- [KNOWN-NONBUG] v1 DynamoDBExecutor:1456 `rowClass==null` dead branch after checkArgNotNull — harmless (already on deliberate list); removal = out-of-scope refactor.
- [KNOWN-NONBUG] v2 stream iterators assign immutable items() without copy — safe (read-only); no bug.
- [CONSIDERED-SKIP] AnyUtil:84 error message omits offending value — class+index already specific/actionable; left to keep diff focused (marginal WS3).
- CLEAN: Neo4jExecutor, AnyUtil(logic), v1 AsyncDynamoDBExecutor, v2 DynamoDBExecutor(sync).

Baseline test: **3414 tests, 0 failures, 0 errors, 2 skipped** (offline, live svcs reachable this run).

## Findings ledger

### Batch 1 (G=mongo-sync, H=mongo-reactive)
- [WS2][FIXED] MongoCollectionExecutor.java:1555 `queryForDate(String,Bson,Class)` — `@throws` named a nonexistent param "rowType" AND claimed IAE on its null that isn't enforced (delegate `queryForSingleValue` validates only filter+propName, not valueType). Fix: dropped "or if rowType is null" to match canonical sibling (1597). Comment-only.
- [FALSE-POSITIVE] reactive MongoCollectionExecutor.aggregate(List,Class):3787 & reactive MongoCollectionMapper.aggregate:3108 — agent claimed null-pipeline IAE is DEFERRED (cold publisher). VERIFIED WRONG: driver `coll.aggregate(pipeline, clazz)` runs `notNull("pipeline")` eagerly when the publisher is built (before `Flux.from`), and executor checks rowType first → both `@throws IAE if pipeline/rowType null` are accurate & synchronous. Docs correct. → added to deliberate list.
- [FALSE-POSITIVE] sync MongoCollectionExecutor.exists(Bson):311 — doc `@throws IAE if filter null` is accurate (delegates to count(filter) which validates). Not a mismatch.
- CLEAN: MongoDB(sync+reactive), MongoDBBase, MongoCollectionMapper(sync), AsyncMongoCollectionExecutor.
