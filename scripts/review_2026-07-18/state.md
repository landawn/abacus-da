# Review round 2026-07-18 — ledger

Tree: HEAD `97adef4` + uncommitted 2026-07-17 r1+r2 fixes + USER pom bumps
(abacus-common 7.8.7→7.8.8, abacus-query 4.8.8→4.8.9).
Process: 18 read-only Explore agents in waves of 4, shared BRIEFING.md;
main agent verifies every finding before editing; main agent sole editor.

## Pre-wave finding (main agent, from baseline run)
**[WS1][P1] FIXED — abacus-query 4.8.9 `from()` SELECT-only gate broke CQL DELETE-with-columns.**
Baseline `mvn -o test` showed CqlBuilderTest 2 errors + CassandraExecutorBaseTest 6 errors:
`ISE: Invalid operation for from(): DELETE. Expected QUERY` from the new private
`checkCanAppendFrom()` in AbstractQueryBuilder 4.8.9 (4.8.8 documented but did not enforce).
CQL legitimately supports `DELETE col1, col2 FROM tbl` (delete(cols).from(...) is this builder's
documented API; prepareDelete's propNamesToDelete branch uses it). FIX in local CqlBuilder:
overrode `from(Class)`, `from(Class,String)`, `from(String,Class)` bypassing the parent gate via
new private `checkCanAppendCqlFrom()` (allows QUERY+DELETE; keeps closed-builder, double-from,
columns-for-SELECT checks); added the double-from guard + CQL-keyword op message to the local
`appendOperationBeforeFrom` so the ungated String-funnel keeps parity.
Verified: CqlBuilderTest 524/0/0 + CassandraExecutorBaseTest 90/0/0 (both RED before).
TODO this round: edge-case tests for double-from ISE + insert().from() message.

Baseline (before fix): 3428 tests, 0 F, 154 E (live-Cassandra + suite static-init + the 8 above),
15 skipped. After fix expect only environmental errors.

## Slices
| # | Agent | Files | Status |
|---|-------|-------|--------|
| 1 | A | aws/dynamodb/DynamoDBExecutor.java (v1 sync) | DONE — 1 P3 WS2 fix APPLIED: extractData(QueryResult/ScanResult,int,int) gained missing @throws NPE (mirrors the 2026-06-25 v2 fix; verified deref-before-check). Rest CLEAN; {} placeholder in N.checkArgument verified OK vs 7.8.8 format() |
| 2 | B | aws/dynamodb/v2/DynamoDBExecutor.java (v2 sync) | DONE — 1 P3 WS2 fix APPLIED: 6 fixed-arity asItem/asUpdateItem `@throws IAE if attrName null` claims are PHANTOM (N.asMap 7.8.8 accepts null keys; asKey validates, asItem deliberately doesn't — matches v1+varargs) → dropped the 6 false @throws lines, kept "Must not be null" @param caveats. Rest CLEAN |
| 3 | C | aws/dynamodb/AsyncDynamoDBExecutor.java + v2/AsyncDynamoDBExecutor.java | DONE — both CLEAN (delegation fidelity + NPE/IAE doc split re-verified vs sync; thenRunAsync overloads javap-confirmed vs 7.8.8) |
| 4 | D | mongodb/MongoCollectionExecutor.java | DONE — production CLEAN; 1 P3 test gap FILLED: testAggregateDefaultDocumentRowTypePreservesRowContent (default Document rowType routes through toEntity/readRow with no raw short-circuit; was never fed a real row). Sync 181/0/0 |
| 5 | E | mongodb/reactivestreams/MongoCollectionExecutor.java | DONE — CLEAN (matched-but-missing divergence, $expr count==0, Document short-circuits, mapNotNull migration, toBson visibility comment all re-verified) |
| 6 | F | mongodb/AsyncMongoCollectionExecutor.java + mongodb/MongoDB.java + reactivestreams/MongoDB.java | DONE — all 3 CLEAN (~180 delegations arg-order-verified; ContinuableFuture claims re-verified vs 7.8.8 sources; Callable casts necessary; checkArgNotNull message format confirmed) |
| 7 | G | mongodb/MongoCollectionMapper.java + reactivestreams/MongoCollectionMapper.java | DONE — sync CLEAN; 2 P3 WS2 fixes APPLIED in reactive mapper: 5-arg list(:1047)/query(:1910) @throws gained ", offset is negative, or count is negative" (executeQuery enforces eagerly; all siblings documented it) |
| 8 | H | mongodb/MongoDBBase.java + aws/AnyUtil.java + hbase/annotation/ColumnFamily.java + 11 stubs + 16 package-infos | DONE — all 30 units CLEAN; 1 P3 WS5 cleanup APPLIED: 3 dead commented-out blocks removed from MongoDBBase (extractData prior-version block, getObjectIdSetMethod duplicate condition, GeneralCodecRegistry parent-registry block); compile exit 0 |
| 9 | I | hbase/HBaseExecutor.java | DONE — 1 P3 WS2 fix APPLIED: toEntity single-value doc now notes byte[]/ByteBuffer targets get raw cell bytes (getCellValue special-cases them, verified); + test testToEntity_singleCellResult_byteArrayAndByteBufferGetRawCellBytes (83/0/0). Rest CLEAN |
| 10 | J | hbase/AsyncHBaseExecutor.java + AnyQuery + AnyOperation + AnyOperationWithAttributes | DONE — all 4 CLEAN (47 delegations arg-order-verified; ContinuableFuture/AsyncExecutor overloads re-verified vs 7.8.8; pool-sizing claim matches) |
| 11 | K | hbase/AnyScan.java + AnyGet.java | DONE — both CLEAN (all @throws/edge docs re-verified vs hbase-client 2.6.4 sources). SKIPPED below-bar: AnyScan.setBatch could doc driver IncompatibleFilterException — house convention omits driver runtime exceptions on delegating setters |
| 12 | L | hbase/AnyPut + AnyDelete + AnyIncrement + AnyAppend + AnyMutation + AnyRowMutations | DONE — 5 of 6 CLEAN (driver @throws splits re-verified vs 2.6.4 bytecode); 2 P3 WS2 fixes APPLIED in AnyPut: false pool/interning class-doc claims removed/corrected (toFamilyQualifierBytes deliberately never shares arrays — live-map corruption) + of(Object,long,boolean) @throws gained null/empty-row IAE clause mirroring of(Object,boolean) |
| 13 | M | cassandra/CassandraExecutorBase.java | DONE — CLEAN; all prepareInsert/Update/Delete/Query builder sequences traced VALID under 4.8.9 clause-state rules (delete(cols).from covered by the new checkCanAppendCqlFrom override; appendIf(false,null) short-circuits safely) |
| 14 | N | cassandra/CassandraExecutor.java + cassandra/AsyncCassandraExecutor.java | DONE — both CLEAN. Agent's sole item (dead `else if (N.isEmpty(parameters))` :1653) = SETTLED leave-it (2026-07-04, symmetric v3/v4) — skipped. FP-note refinement: v4 execute((Statement)null) → IAE from RequestProcessorRegistry ("No request processor found"), driver-source-verified; current docs accurate |
| 15 | O | cassandra/AsyncCassandraExecutorBase.java + v3/AsyncCassandraExecutor.java | DONE — both CLEAN (map-unwrap semantics re-verified vs 7.8.8 ExceptionUtil). 1 P3 WS3 fix APPLIED (spans sync+async base, 8 sites): 'propNamesToDelete can't be null or empty' → "can't be empty (pass null to delete the entire row)" — null is documented-valid, throw only fires on empty; no test pinned the string |
| 16 | P | cassandra/v3/CassandraExecutor.java + cassandra/ResultSets.java | DONE — ResultSets CLEAN; v3 dead else-if = settled leave-it (skipped). 1 P3 WS3 fix APPLIED (message-class, 8 sites in 4 files: cassandra v4+v3, DDB v1+v2 readRow/createRowMapper): "Unsupported row/column type" → "Column count must be 1 to map a row to the single-value type: X, but the row has N columns" (matches BigQuery 2026-06-23 + MongoDBBase 2026-07-04 precedent; no test pinned). Compile+308 affected tests green |
| 17 | Q | cassandra/CqlBuilder.java + CqlMapper.java + ParsedCql.java | DONE — CqlMapper/ParsedCql CLEAN; the pre-wave from() overrides independently verified CORRECT+COMPLETE (no inherited entry point reaches the parent SELECT-only gate on DELETE; mutateAtomically covers closed-state on the String path). 1 P3 WS1 fix APPLIED: batch into() gained assertNotClosed() (built batch builder kept _propsList → NPE at _sb deref instead of documented ISE) + regression test test_batchInsert_intoAfterBuild_throwsIllegalStateException (528/0/0) |
| 18 | R | gcp/BigQueryExecutor.java + azure/CosmosContainerExecutor.java + neo4j/Neo4jExecutor.java | DONE — Cosmos+Neo4j CLEAN; BQ/Cosmos selectFrom/select().from() chains verified VALID under 4.8.9 gate (DML never calls from()). 2 P3 fixes APPLIED in BigQuery: class-doc interrupt "reset"→"restored" (matches code+sibling doc); getKeyNames/getKeyNameSet verbatim cache block extracted into private getKeyTuple (intra-file, zero behavior change). BigQueryExecutorTest 155/0/0 |

## FINAL RESULT
All 18 slices complete. Totals: 1 P1 WS1 (pre-wave dependency regression, fixed+3 tests),
1 P3 WS1 (batch into() closed-state, fixed+test), 2 WS3 message-class fixes (16 sites),
9 WS2 doc fixes, 2 WS5 cleanups, 6 new tests overall. All findings main-agent-verified before
edit; 3 agent items REFUTED/SKIPPED as settled-leave (dead else-if v3+v4 ×2, setBatch driver
exception doc).
Final full `mvn -o test -pl abacus-da-all` = **3435 tests, 0 Failures, 147 Errors, 15 Skipped** —
errors are all environmental: live-Cassandra static-init classes (v4 81E + v3 93E as counted
per-class; suite aggregation differs) + 1 known flaky live-Mongo `test_update_async` (passes
1/0/0 isolated); 15 skips = Cosmos/DynamoDB-Local emulators not running.

## Findings
(recorded as they arrive; VERIFIED/REFUTED/APPLIED by main agent)

## Fixes applied
1. (pre-wave) cassandra/CqlBuilder.java — from() overrides restoring CQL DELETE-with-columns
   under abacus-query 4.8.9 (see above).
