# Review round 2026-07-17 — ledger

HEAD: 97adef4 (clean tree). 67 units under abacus-da-all/src/main/java.
Process: 18 read-only Explore agents in waves of 4-5, shared BRIEFING.md;
main agent verifies every finding against live code before editing; main agent sole editor.
Baseline suite (07-10 r2 / 07-16): full `mvn -o test -pl abacus-da-all` ≈ 3431+ tests, 0 Failures,
0 Errors (offline errors = live-Cassandra/DynamoDB/Neo4j classes only), few skipped.

## Slices
| # | Agent | Files | Status |
|---|-------|-------|--------|
| 1 | A | aws/dynamodb/DynamoDBExecutor.java (v1 sync) | DONE — 1 finding, applied |
| 2 | B | aws/dynamodb/v2/DynamoDBExecutor.java (v2 sync) | DONE — CLEAN |
| 3 | C | aws/dynamodb/AsyncDynamoDBExecutor.java + v2/AsyncDynamoDBExecutor.java | DONE — both CLEAN |
| 4 | D | mongodb/MongoCollectionExecutor.java | DONE — 1 finding (findFirst carve-out), applied |
| 5 | E | mongodb/reactivestreams/MongoCollectionExecutor.java | DONE — 2 doc findings, applied |
| 6 | F | mongodb/AsyncMongoCollectionExecutor.java + mongodb/MongoDB.java + reactivestreams/MongoDB.java | DONE — all CLEAN |
| 7 | G | mongodb/MongoCollectionMapper.java + reactivestreams/MongoCollectionMapper.java | DONE — sync CLEAN; 1 doc finding reactive, applied |
| 8 | H | mongodb/MongoDBBase.java + aws/AnyUtil.java + hbase/annotation/ColumnFamily.java + 11 stubs + 16 package-infos | DONE — 1 doc finding (ColumnFamily:87), applied |
| 9 | I | hbase/HBaseExecutor.java | DONE — 1 doc finding applied; perf nit = known-rejected item |
| 10 | J | hbase/AsyncHBaseExecutor.java + AnyQuery + AnyOperation + AnyOperationWithAttributes | DONE — all CLEAN |
| 11 | K | hbase/AnyScan.java + AnyGet.java | DONE — CLEAN; 1 edge-case test added (readVersions<1 stored-as-is) |
| 12 | L | hbase/AnyPut + AnyDelete + AnyIncrement + AnyAppend + AnyMutation + AnyRowMutations | DONE — all 6 CLEAN (verified vs hbase-client 2.6.4 sources) |
| 13 | M | cassandra/CassandraExecutorBase.java | DONE — CLEAN; 1 edge-case test added (null non-first entity) |
| 14 | N | cassandra/CassandraExecutor.java + cassandra/AsyncCassandraExecutor.java | DONE — CLEAN (dead else-if = known settled parity item, left) |
| 15 | O | cassandra/AsyncCassandraExecutorBase.java + v3/AsyncCassandraExecutor.java | DONE — both CLEAN |
(P hit a transient 529 server error mid-review; resumed)
| 16 | P | cassandra/v3/CassandraExecutor.java + cassandra/ResultSets.java | DONE — both CLEAN |
| 17 | Q | cassandra/CqlBuilder.java + CqlMapper.java + ParsedCql.java | DONE — all 3 CLEAN (generation paths executed vs abacus-query 4.8.8) |
| 18 | R | gcp/BigQueryExecutor.java + azure/CosmosContainerExecutor.java + neo4j/Neo4jExecutor.java | DONE — all 3 CLEAN (SDK signatures javap-verified) |

## FINAL RESULT
All 18 slices complete. 1 WS1 code fix + 7 doc fixes + 2 edge-case tests applied.
Full `mvn -o test -pl abacus-da-all` = **3550 tests, 0 Failures, 0 Errors, 2 Skipped** — BUILD SUCCESS.

## Findings
(recorded as they arrive; VERIFIED/REFUTED/APPLIED by main agent)

- A-1 [WS2][P3] aws/dynamodb/DynamoDBExecutor.java:98 — class-doc "Batch Operations" bullet claimed
  "automatic handling of 25-item limits"; code does no auto-split (batchWriteItem/Mapper.batchPutItem
  send one request), per-method docs + v2 sibling (:104) say caller must respect limits.
  VERIFIED (read :98 + v2 :104) → APPLIED: v1 bullet now mirrors v2 wording. Comment-only.

- D-1 [WS1][P3] mongodb/MongoCollectionExecutor.java findFirst family — findFirst/get/gett diverged
  from list/stream for Document-assignable rowTypes (Object/Bson/Map/Document): list/stream have a
  raw-document carve-out (`rowType.isAssignableFrom(Document.class)`), findFirst routed into
  single-value extraction (Collection-projection) or readRow scalar fallback (Bson-projection —
  could even throw IAE on multi-field docs). VERIFIED in both sync (:1145 vs :896/:936 guards)
  and reactive (:1075/:1135 vs :845/:873). APPLIED: same carve-out idiom added to the 4 findFirst
  terminals (sync ×2, reactive ×2) + Javadoc sentences + 4 regression tests
  (testFindFirst{Single,Bson}ProjectionObjectRowTypeReturnsRawDocument, sync + reactive).
  Note: findFirst(Map/Document.class) now returns the raw Document instance (identity) instead of
  a readRow copy — matches list/stream passthrough semantics.
- E-1 [WS2][P3] reactive MongoCollectionExecutor updateOne(Bson,Object) @throws omitted eager
  update-null IAE (toBson checkArgNotNull) — VERIFIED (:2472) → APPLIED "filter or update".
- E-2 [WS2][P3] reactive MongoCollectionExecutor updateOne(Bson,Collection) @throws omitted eager
  filter-null IAE (3-arg checkArgNotNull) — VERIFIED (:2535) → APPLIED.

## Fixes applied
1. aws/dynamodb/DynamoDBExecutor.java:98 — stale batch auto-split claim → v2 wording (doc-only).
2. mongodb/MongoCollectionExecutor.java + reactivestreams/MongoCollectionExecutor.java — findFirst
   Document-assignable raw-doc carve-out (4 terminals) + docs + 4 regression tests.
3. reactivestreams/MongoCollectionExecutor.java — 2 @throws completions on updateOne overloads.
4. reactivestreams/MongoCollectionMapper.java:2668 — bulkWrite(List) "atomically" over-claim →
   per-document-atomic/not-a-transaction wording mirroring the sync mapper (doc-only).
5. hbase/HBaseExecutor.java:3849/3880/3910 — 3 HBaseMapper coprocessor startRowKey @param lines
   gained "; null for unbounded start" (delegates map null→null=first region; sibling :3820 had it).
6. hbase/annotation/ColumnFamily.java:87 — example comment "personal:firstName" contradicted the
   @Column("given_name") in its own example → "personal:given_name" (doc-only).
   SKIPPED (known-rejected): HBaseExecutor createRowMapper per-row cached-lookup hoisting —
   network-bound micro-nit + would move multi-@Id IAE timing (already rejected 2026-07-09 r2).
