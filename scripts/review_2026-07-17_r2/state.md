# Review round 2026-07-17 r2 — ledger (full re-run requested by user)

Tree: HEAD 97adef4 + round-1 uncommitted fixes (6 source + 4 test files).
Process: same 18 slices as round 1 (see scripts/review_2026-07-17/state.md), FRESH read-only
Explore agents in waves of 4, shared BRIEFING.md (updated with round-1 outcomes);
main agent verifies every finding before editing; main agent sole editor.
Baseline suite: 3550 tests, 0 Failures, 0 Errors, 2 Skipped (2026-07-17 round-1 final run).

## Slices
| # | Agent | Files | Status |
|---|-------|-------|--------|
| 1 | A2 | aws/dynamodb/DynamoDBExecutor.java (v1 sync) | DONE — CLEAN (round-1 fix verified in place) |
| 2 | B2 | aws/dynamodb/v2/DynamoDBExecutor.java (v2 sync) | DONE — CLEAN (byte[] key admission = deliberate v2 improvement, noted) |
| 3 | C2 | aws/dynamodb/AsyncDynamoDBExecutor.java + v2/AsyncDynamoDBExecutor.java | DONE — both CLEAN |
| 4 | D2 | mongodb/MongoCollectionExecutor.java | DONE — 1 comment fix applied (toBson visibility claim); round-1 fix verified |
| 5 | E2 | mongodb/reactivestreams/MongoCollectionExecutor.java | DONE — CLEAN (round-1 fixes verified) |
| 6 | F2 | mongodb/AsyncMongoCollectionExecutor.java + mongodb/MongoDB.java + reactivestreams/MongoDB.java | DONE — all CLEAN |
| 7 | G2 | mongodb/MongoCollectionMapper.java + reactivestreams/MongoCollectionMapper.java | DONE — CLEAN; 2 edge-case tests added (reactive distinct eager IAE) |
| 8 | H2 | mongodb/MongoDBBase.java + aws/AnyUtil.java + hbase/annotation/ColumnFamily.java + 11 stubs + 16 package-infos | DONE — all CLEAN (round-1 fix verified) |
| 9 | I2 | hbase/HBaseExecutor.java | DONE — CLEAN (round-1 fix verified on all 8 coprocessor overloads) |
| 10 | J2 | hbase/AsyncHBaseExecutor.java + AnyQuery + AnyOperation + AnyOperationWithAttributes | DONE — all CLEAN |
| 11 | K2 | hbase/AnyScan.java + AnyGet.java | DONE — both CLEAN |
| 12 | L2 | hbase/AnyPut + AnyDelete + AnyIncrement + AnyAppend + AnyMutation + AnyRowMutations | DONE — all 6 CLEAN (re-verified vs hbase-client 2.6.4 sources) |
| 13 | M2 | cassandra/CassandraExecutorBase.java | DONE — CLEAN |
| 14 | N2 | cassandra/CassandraExecutor.java + cassandra/AsyncCassandraExecutor.java | DONE — both CLEAN |
| 15 | O2 | cassandra/AsyncCassandraExecutorBase.java + v3/AsyncCassandraExecutor.java | DONE — both CLEAN |
| 16 | P2 | cassandra/v3/CassandraExecutor.java + cassandra/ResultSets.java | DONE — both CLEAN (namedDataType javap-verified vs driver 3.11.5) |
| 17 | Q2 | cassandra/CqlBuilder.java + CqlMapper.java + ParsedCql.java | DONE — 2 findings in CqlBuilder, applied; CqlMapper/ParsedCql CLEAN |
| 18 | R2 | gcp/BigQueryExecutor.java + azure/CosmosContainerExecutor.java + neo4j/Neo4jExecutor.java | DONE — all 3 CLEAN |

## FINAL RESULT
All 18 slices complete; every round-1 verdict held, and r2 caught 4 additional small items
(2 comment fixes + 1 message fix in CqlBuilder + 2 edge-case tests), all applied.
Full `mvn -o test -pl abacus-da-all` = **3552 tests, 0 Failures, 1 Error, 2 Skipped** — the 1 error
is the KNOWN flaky live-Mongo `MongoDBExecutorTest.test_update_async` (shared-collection
concurrency); rerun in isolation = 1/0F/0E. Not a regression.

## Findings
(recorded as they arrive; VERIFIED/REFUTED/APPLIED by main agent)

## Fixes applied
1. mongodb/MongoCollectionExecutor.java:3158 — toBson rationale comment copied from the reactive
   sibling wrongly claimed the same-package protected toDocument(Object,boolean) overload is
   invisible (JLS 6.6.1: protected is package-visible; MongoDBBase :593 is in the same package).
   Reworded to the accurate dead-flag justification; reactive sibling's comment untouched
   (accurate there — different package). Comment-only.
2. reactivestreams/MongoCollectionMapperTest — 2 edge-case tests added
   (testDistinct_nullOrEmptyFieldName_throwsSynchronously, testDistinctWithFilter_nullArgs_throwSynchronously):
   the reactive mapper's only non-delegating guards (distinct eager IAE, guards at :3025/:3082-3083)
   were untested while the sync twin covers them.
3. cassandra/CqlBuilder.java:1402 — comment claimed SqlExpression literals are NOT normalized by the
   naming policy; appendStringExpr(literal,false) DOES normalize identifier tokens (verified vs
   AbstractQueryBuilder.normalizeColumnName). Comment corrected. Comment-only.
4. cassandra/CqlBuilder.java:551/:647/:655 — 3 IllegalStateException messages interpolated the
   internal OperationType enum ("a QUERY statement", "a ADD statement") instead of CQL keywords;
   added private opCqlKeyword() (ADD→INSERT, QUERY→SELECT, else name()) + article fix. No test
   pinned the old strings (grep-verified). Message-only.
