# Review round 2026-07-09 r2 — state ledger

HEAD at start: 6db79c9 (clean tree). Scope: all 51 main classes, three workstreams (WS1 bugs, WS2 javadoc/comments, WS3 logs+performance). 15 read-only Explore agents in batches of 4; main agent sole editor.

## Agent slices
- A: aws/dynamodb/DynamoDBExecutor (v1 sync) — DONE: 1 finding [WS2/P3] toEntity(Map,Class) @throws NPE→IAE for null targetClass (VERIFIED: abacus-query 4.8.7 QueryUtil.getColumn2PropNameMap does N.checkArgNotNull→IAE; v2 sibling makes no claim — left). APPLIED.
- B: aws/dynamodb/v2/DynamoDBExecutor (v2 sync) — DONE: clean, 0 findings. Noted-below-bar: asItem odd-length msg over-states (name-value pairs only); scan doc "empty attributesToGet → all attrs" unverified vs SDK.
- C: aws/dynamodb/AsyncDynamoDBExecutor + v2/AsyncDynamoDBExecutor — DONE: clean, 0 findings (v1-NPE vs v2-IAE list(QueryRequest) drift verified as genuine delegate difference, not doc drift).
- D: mongodb/MongoCollectionExecutor + mongodb/MongoDB — DONE: 1 finding [WS2/P3] sync MongoCollectionExecutor query(Bson proj,...) overloads ~:1987/:2022 missing @throws MongoException (all ~8 siblings have it); :1987 also under-claims rowType-null IAE (delegates to :2022 checkArgNotNull(rowType)). TO VERIFY+APPLY.
- E: mongodb/reactivestreams/MongoCollectionExecutor + reactivestreams/MongoDB — DONE: clean, 0 findings.
- F: mongodb/AsyncMongoCollectionExecutor + MongoDBBase — DONE: 1 applied [WS3/P3] MongoDBBase.toList :1089 IAE message printed `Optional[{…}]` (concatenated the u.Optional wrapper) → now `firstNonNull.get()` (inside isPresent branch; no test asserts the string). REJECTED per prior 2026-06-26 decision: async queryForSingleValue/queryForDate valueType-null @throws over-claim (conditional IAE — deliberately left).
- H: hbase/HBaseExecutor + AsyncHBaseExecutor — DONE: clean, 0 findings.
- G: mongodb/MongoCollectionMapper + reactivestreams/MongoCollectionMapper — DONE: 1 finding [WS2/P3] sync mapper queryForDate(String,Bson,Class) @return `Nullable<P>` → `Nullable<V>` (stale type var; VERIFIED :1399). APPLIED.
- I: hbase Any-query — DONE: clean, 0 findings (driver claims re-verified vs hbase-client/common 2.6.0 sources, incl. AnyGet.of(ByteBuffer)→IAE).
- J: hbase Any-mutation — DONE: clean, 0 findings (all NPE/IAE ctor claims re-verified vs decompiled hbase-client 2.6.0).
- K: cassandra/CassandraExecutorBase + AsyncCassandraExecutorBase — DONE: 1 applied [WS3/P3] sync count(Class,Condition) :1437 `N.asList(CqlBuilder.COUNT_ALL)` → existing `COUNT_SELECT_PROP_NAMES` constant (async sibling :1202 already uses it; zero-risk perf/consistency). N unused-import check needed at compile.
- L: cassandra concrete v3+v4 + async + ResultSets — DONE: 1 applied [WS2/P3] v3 toList(ResultSet,Class) :753 @throws NPE clause gained "or targetClass" (VERIFIED :758 derefs targetClass first; matches v4 twin). Rejected items recorded in agent report (findFirst null-targetClass edge etc. — left, consistent siblings).
- M: cassandra/CqlBuilder + CqlMapper + ParsedCql — DONE: clean, 0 findings (ParsedCql parser fixes re-traced and hold; onlyIf-INSERT+USING ordering rejected as unreachable-valid-CQL).
- N: gcp/BigQueryExecutor + azure/CosmosContainerExecutor — DONE: 2 applied, both BigQuery (Cosmos clean). [WS2/P2] toMap(FieldList,FieldValueList,IntFunction) :598 doc claimed nested schemas via getSchema(FieldValueList) but toMapValue :647 uses field.getSubFields() (stale drift from the nested-row fix) → reworded. [WS3/P3] static-init :190 comment + debug log claimed "falls back to public API" but no fallback exists (getSchema then throws IAE per its own doc :2613) → comment+message now say schema-from-row methods fail; use explicit FieldList/Schema overloads.
- O: neo4j/Neo4jExecutor + aws/AnyUtil + 11 stubs — DONE: 1 applied [WS2/P3] Neo4jExecutor stream(String,Map,boolean) :2220 "OGM logs a warning" → "debug-level diagnostic" (VERIFIED by agent vs OGM 5.0.7 ExecuteQueriesDelegate.validateQuery LOGGER.debug). AnyUtil + 11 stubs clean (stub ctor visibility consistent with tests).

## Verification
`mvn -o -q compile -pl abacus-da-all` = exit 0. Full `mvn -o test -pl abacus-da-all` = 3422 tests, Failures 0, Errors 0, Skipped 7 (aggregate; zero `<<< FAILURE/ERROR` markers). Count vs prior rounds' 3441–3442 moves with live-service availability; all green.

## Round summary
All 15 agents complete. 9 fixes applied (8 Javadoc/comment/log-message, 1 zero-risk code one-liner). 10 of 15 slices fully clean. No WS1 correctness bugs found anywhere — codebase re-confirmed mature.

Below-bar items noted but NOT acted on (for future rounds): DDB v1/v2 asItem odd-length message wording; scan doc "empty attributesToGet → all attrs" (SDK behavior unverified); async Mongo queryForSingleValue/queryForDate valueType-null @throws (prior 2026-06-26 deliberate-left); Cassandra sync/async 4-arg queryForSingleValue @throws drift (package convention, don't churn); v3/v4 findFirst null-targetClass documented-IAE-but-no-throw edge (consistent siblings, left).

## Applied fixes
1. [WS2/P3, from D — VERIFIED] sync mongodb/MongoCollectionExecutor: Bson-projection `query(...)` overloads (:1987 4-arg, :2022 6-arg) — added missing `@throws com.mongodb.MongoException` (all Collection-projection siblings have it; both overloads eagerly iterate the cursor); :1987 IAE clause now includes "rowType is null or unsupported" (delegate checks `N.checkArgNotNull(rowType)`); :2022 IAE clause now includes "offset or count is negative" (verified `executeQuery` :2530 checkArgNotNegative pair). Comment-only.
