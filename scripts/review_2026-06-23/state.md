# Review ledger — 2026-06-23

STATUS: COMPLETE. All 22 agents reported (51/51 files, no gaps/overlap). 11 fixes applied across 3 workstreams + 2 regression tests. Full suite **3387** (baseline 3385 + 2), 0 failures / 0 errors.

Multi-agent line-by-line review of all 51 classes under `abacus-da-all/src/main/java`.
Three workstreams: (1) bug fixes [HIGH risk], (2) Javadoc/comment corrections [comment-only],
(3) logging/exception-message improvements [LOW risk].

- Source root: `abacus-da-all/src/main/java` (the repo root has no `src/main/java`; module is `abacus-da-all`).
- Working tree at start: content-identical to HEAD (all `M` flags are EOL/CRLF-only; `git -c core.autocrlf=false diff` is empty). Files are LF on disk, `core.autocrlf=true`.
- Build: `mvn -o clean compile -pl abacus-da-all` = BUILD SUCCESS (baseline).
- Ledger path NOT git-ignored (scripts/ is tracked). Treated as a working artifact.

## Prior-review context (do NOT re-litigate — see memory ledgers)
This module has had 6+ deep multi-agent passes (2025-H1 .. 2026-06-10). Mature; expect very few new bugs.
Documented deliberate-design / known false-positives (do NOT re-flag) — consolidated:
- Cassandra v3+v4 `prepareStatement`: null multi-param bind → `N.defaultValueOf(type)` (deliberate, not CQL NULL).
- v4 `CassandraExecutor` missing `javaClass==null` guards are UNREACHABLE (protocolCodeDataType total over 26 codes).
- `prepareStatement` writes converted values back into caller's Object[] (aliasing) — accepted, identical v3+v4.
- HBase `Any*` hashCode/equals delegate to wrapped object; compareTo row-key-only (intentionally inconsistent with equals, mirrors HBase). Get/RowMutations/Increment equals = row-key-ONLY; Scan/Query/OWA = reference identity.
- `AnyMutation.getFamilyCellMap` / `AnyRowMutations.val` live-map exposure + by-ref `of(...)` are documented contracts.
- `AnyOperationWithAttributes.getAttributesMap` "read-only view" is a live unmodifiable view (correct).
- ContinuableFuture `thenRunAsync` HAS a `Consumer<? super T>` overload (DynamoDB v1 async value-consuming examples correct).
- `MongoDBBase.objectId2Filter` examples use `{"_id":{"$oid":"…"}}` — genuine Document.toJson() extended-JSON; recurring FALSE POSITIVE, leave `$oid`.
- DynamoDB IAE-for-null SDK convention kept package-wide (do NOT mass-rewrite to NPE).
- Cosmos `count>0→limit` dead branch (only caller passes count=0) — moot, left.
- Mongo `groupBy/groupByAndCount(Collection,Class)` build an unused local `groupFields` — dead, noted, not a bug.
- N.convert(Object,Type) SHORT-CIRCUITS on assignable raw container (element pollution persists); forced element conversion uses `type.valueOf(N.toJson(value))` in DynamoDB v1 toEntity + BigQuery toEntity (already applied).
- DynamoDB list/query auto-pagination treats Limit as page size (documented).
- Mongo sync `queryForSingleValue` matched-but-missing-field → present `Nullable.of(null)`; reactive → completes empty. Each Javadoc correct for its own variant.
- Empty stubs (NO logic): search/{Elasticsearch,Lucene,Solr}, aws/{AWSRDSUtil,AWSS3Util}, hadoop/{HDFSUtil,HadoopUtil}, blink/*, spark/*.

## Baseline test count
- Full suite (`mvn -o test -pl abacus-da-all`, 2026-06-23): **Tests run: 3385, Failures: 0, Errors: 0, Skipped: 2**. Live services WERE reachable this run (0 environmental errors). Target after changes: 3385 + new regression tests, still 0 failures/0 errors (or only the known live-service errors if services drop).

## File partition & coverage accounting (51 files → 22 agents, no overlap, no gaps)

| Agent | Files | Status |
|---|---|---|
| A STUBS | search/{Elasticsearch,Lucene,Solr}Executor, aws/{AWSRDSUtil,AWSS3Util}, hadoop/{HDFSUtil,HadoopUtil}, blink/{DataSetUtil,DataStreamUtil}, spark/{DStreamUtil,DatasetUtil} (11) | pending |
| B HBASE-MUT-BASE | hbase/{AnyOperation,AnyOperationWithAttributes,AnyMutation,AnyRowMutations} (4) | pending |
| C HBASE-PUT | hbase/{AnyPut,AnyAppend,AnyIncrement} (3) | pending |
| D HBASE-GET | hbase/{AnyQuery,AnyGet,AnyDelete}, hbase/annotation/ColumnFamily (4) | pending |
| E HBASE-SCAN | hbase/AnyScan (1) | pending |
| F HBASE-EXEC | hbase/{HBaseExecutor,AsyncHBaseExecutor} (2) | pending |
| G CASS-V4-SYNC | cassandra/{CassandraExecutor,CassandraExecutorBase} (2) | pending |
| H CASS-ASYNC | cassandra/{AsyncCassandraExecutor,AsyncCassandraExecutorBase} (2) | pending |
| I CASS-V3 | cassandra/v3/{CassandraExecutor,AsyncCassandraExecutor} (2) | pending |
| J CASS-CQL | cassandra/{CqlBuilder,ParsedCql,CqlMapper,ResultSets} (4) | pending |
| K DYN-V1-SYNC | aws/dynamodb/DynamoDBExecutor (1) | pending |
| K2 DYN-V1-ASYNC | aws/dynamodb/AsyncDynamoDBExecutor (1) | pending |
| L DYN-V2-SYNC | aws/dynamodb/v2/DynamoDBExecutor (1) | pending |
| M DYN-V2-ASYNC | aws/dynamodb/v2/AsyncDynamoDBExecutor (1) | pending |
| N MONGO-SYNC-EXEC | mongodb/MongoCollectionExecutor (1) | pending |
| O MONGO-SYNC-MAP | mongodb/{MongoCollectionMapper,AnyUtil} (2) | pending |
| O2 MONGO-ASYNC | mongodb/AsyncMongoCollectionExecutor (1) | pending |
| P MONGO-BASE | mongodb/{MongoDBBase,MongoDB} (2) | pending |
| Q MONGO-REACT-EXEC | mongodb/reactivestreams/MongoCollectionExecutor (1) | pending |
| R MONGO-REACT-MAP | mongodb/reactivestreams/{MongoCollectionMapper,MongoDB} (2) | pending |
| S BIGQUERY | gcp/BigQueryExecutor (1) | pending |
| T COSMOS-NEO4J | azure/CosmosContainerExecutor, neo4j/Neo4jExecutor (2) | pending |

Coverage: 11+4+3+4+1+2+2+2+2+4+1+1+1+1+1+2+1+2+1+2+1+2 = 51 ✓

## Findings ledger
Status of agents: J✓ K✓ N✓ S✓ G✓ L✓ done. Remaining: A B C D E F H I K2 M O O2 P Q R T.

Raw findings (pre-verification by main agent):
- [J/CqlBuilder] CqlBuilder.java ~462 (usingTimestamp(long)/(Date)): Math.multiplyExact can throw ArithmeticException; no `@throws` documented. WS2, low. CANDIDATE-DOC.
- [K/DynV1] DynamoDBExecutor.java:1421 dead `rowClass==null` branch after checkArgNotNull. Non-bug (known). SKIP.
- [N/MongoSyncExec] distinct(String,Class) ~4123 + distinct(String,Bson,Class) ~4151: `@throws IAE ... if rowType is null` but driver NPEs; matches house style (watch(Class)/aggregate(List,Class) same). WS2, low. EVALUATE (likely house-style → skip or align).
- [S/BigQuery] BigQueryExecutor.java:776 misleading msg "Unsupported row/column type" for scalar target w/ multi-column row; sibling createRowMapper:888 says "Field count must be 1 for type". WS3, low. CANDIDATE-MSG.
- [S/BigQuery] BigQueryExecutor.java:1687 exists(Class,Condition) Javadoc "only PK columns projected" untrue for keyless class (emits SELECT *). WS2, low. CANDIDATE-DOC.
- [G/CassV4] CassandraExecutorBase.java ~441 entityToCondition missing-key message under-reports when multiple keys missing. WS3, low (msg clarity). EVALUATE.
- [L/DynV2] DynamoDBExecutor.java:1425 dead branch (known) + 2978 redundant dead-store copy. Non-bugs. SKIP.

Batch 3 (F,H,M,Q):
- [F/HBaseExec] HBaseExecutor.java ~2819/2872/2924 coprocessorService @throws Exception prose "wrapped in a new Exception" but only wraps non-Exception Throwables (3 copy-paste blocks). WS2, low. CANDIDATE-DOC (verify).
- [H/CassAsync] AsyncCassandraExecutorBase.java ~2845 abstract execute(String,Object...) edge example claims future-deferred/ExecutionException-wrapped but concrete impls prepare synchronously. WS2, low, suspected. EVALUATE.
- [M/DynV2Async] CLEAN, 0 findings.
- [Q/MongoReactExec] aggregate(List,Class) ~3791 + mapReduce(...,Class) @throws "rowType is null" but no rowType guard (returns Object[] rows). WS2, low, confirmed. CANDIDATE-DOC or align-with-sync-guard. (Pairs with N-F1 distinct.)

Batch 4 (B,C,D,E):
- [B/HBaseMutBase] CLEAN (4 files, verified vs HBase 2.6.4 bytecode).
- [C/HBasePut] AnyPut.java:434/440 of(Object,boolean) null key → NPE (Put(byte[],boolean) derefs row.length) but doc says IAE. WS2, low, likely. CANDIDATE-DOC (verify vs jar). AnyAppend/AnyIncrement CLEAN.
- [C/HBasePut] AnyPut.java:1352 stray "; throws IOException" on add(Cell) SUCCESS-path example. WS2, low, confirmed. CANDIDATE-DOC (same class as prior AnyIncrement fix).
- [D/HBaseGet] CLEAN (4 files, verified vs HBase 2.5.6 source).
- [E/HBaseScan] AnyScan.java:52-53 class-header "Reversed scans" bullet INVERTS start/stop-row semantics vs method docs (isReversed/setReversed) + HBase. WS2, MED, confirmed. CANDIDATE-DOC (strong).

Batch 5 (I,K2,O,O2):
- [I/CassV3] CLEAN (2 files).
- [K2/DynV1Async] scan(...) ×4 overloads (~1860,1922,2071,2112): @param scanFilter "must not be null" + @throws IAE wrong; sync sibling documents scanFilter nullable ("may be null to apply no filter"), no null check. WS2, low, confirmed. CANDIDATE-DOC (4 spots).
- [O/MongoSyncMap] distinct(String) :3112 + distinct(String,Bson) :3159: @throws IAE if fieldName null/empty but distinctPipeline route never validates fieldName (executor native distinct + reactive mapper DO validate). WS1/contract, low, confirmed. CANDIDATE-FIX (add checkArgNotEmpty). AnyUtil CLEAN.
- [O2/MongoAsync] CLEAN.

Batch 6 (P,R,T,A):
- [P/MongoBase] MongoDBBase:1319 extractData(Collection,List,Class) "Other objects: ...and type conversion" overstates (N.newDataset = bean extraction only, no rowType conversion for non-Map first elem). WS2, low, likely. CANDIDATE-DOC.
- [R/MongoReactMap] CLEAN. Reactive distinct does NOT share sync gap (reactive executor validates fieldName eagerly :3695/:3722). Reactive aggregate/mapReduce have NO rowType param → no doc mismatch. So distinct fix is SYNC-MAPPER-ONLY.
- [T/Cosmos+Neo4j] Cosmos streamItems @throws CosmosException deferred-not-eager (optional, low). Otherwise CLEAN.
- [A/STUBS] 11/11 confirmed empty, cross-@see all valid. 0 findings.

## Triage decision (main agent)
FIX (WS2 comment-only): AnyScan:52-53 reversed-scan header (MED); AnyPut:1352 stray throws-IOException; AnyPut:434/440 NPE-not-IAE; DynV1-async scan ×4 scanFilter nullable; BigQuery:1687 keyless projection; HBaseExecutor coprocessor @throws wording ×3; MongoDBBase:1319 "and type conversion"; CqlBuilder usingTimestamp ArithmeticException @throws.
FIX (WS3 message): BigQuery:776 "Unsupported row/column type" → field-count message.
FIX (WS1 contract + regression test): MongoCollectionMapper(sync) distinct(String)/distinct(String,Bson) add checkArgNotEmpty(fieldName).
EVALUATE then decide: reactive executor aggregate(List,Class)/mapReduce(...,Class) rowType (guard vs doc); Mongo sync exec distinct(String,Class)/(String,Bson,Class) rowType-NPE @throws wording.
SKIP (low value / house-style / suspected): DynamoDB dead-branches; AsyncCassandraExecutorBase abstract-execute edge example; Cosmos streamItems deferred note; CassandraExecutorBase entityToCondition missing-key message.

## Edits applied (all verified against source)

### Workstream 2 — Javadoc/comment-only
1. hbase/AnyScan.java (class-header, ~52) — "Reversed scans" bullet inverted start/stop-row semantics; now matches isReversed/setReversed method docs + HBase (start=upper bound, walk down to stop). [MED]
2. hbase/AnyPut.java (~1352) — removed stray "; throws IOException" on the add(Cell) SUCCESS-path example (cell row matches Put row → succeeds).
3. cassandra/CqlBuilder.java (usingTimestamp(long) ~462 + usingTimestamp(Date) ~433) — added `@throws ArithmeticException` (real: Math.multiplyExact(timestamp,1000) overflows).
4. gcp/BigQueryExecutor.java (exists(Class,Condition) ~1686) — "only the primary-key columns ... are projected" → "...(or all columns, if it declares no key)..." (keyless class emits SELECT *).
5. mongodb/MongoDBBase.java (extractData(Collection,List,Class) ~1319) — "Other objects: ...and type conversion" → "Extracts the selected properties directly (no per-row conversion to the target type)" (else-branch = N.newDataset, no rowType conversion).
6. aws/dynamodb/AsyncDynamoDBExecutor.java (v1, 4 scan overloads ~1860/1922/2071/2112) — scanFilter "must not be null"→"may be null to apply no filter"; @throws dropped scanFilter (sync sibling documents it nullable, no guard).
7. hbase/HBaseExecutor.java (coprocessorService + 2× batchCoprocessorService @throws Exception, ~2819/2872/2924) — "wrapped in a new Exception" → "rethrown as-is if already an Exception, otherwise wrapped; Error rethrown unchanged" (matches `e instanceof Exception ? e : new Exception(e)`).
8. mongodb/MongoCollectionExecutor.java (sync, distinct(String,Class) ~4120 + distinct(String,Bson,Class) ~4148) — @throws dropped the "rowType is null"→IAE over-claim (rowType→driver NPE; reactive sibling house-style doesn't claim it).

### Workstream 3 — logging/exception message (code edit)
9. gcp/BigQueryExecutor.java (readRow scalar else-branch ~776) — message "Unsupported row/column type: X" → "Field count must be 1 to map a row to the single-value type: X, but the row has N columns" (matches createRowMapper:888; the type IS supported, the column count is wrong).

### Workstream 1 — bug/contract fix (code + regression tests, RED→GREEN verified)
10. mongodb/MongoCollectionMapper.java (sync, distinct(String) + distinct(String,Bson)) — added `N.checkArgNotEmpty(fieldName,"fieldName")` to honor the documented `@throws IAE` (previously routed through the $group/$project pipeline unvalidated: null→"$null" key, empty→invalid "$" path). Matches the executor's native distinct + the reactive mapper. Test: `MongoCollectionMapperTest#testDistinct_nullOrEmptyFieldName_throws`.
11. mongodb/reactivestreams/MongoCollectionExecutor.java (aggregate(List,Class) ~3794) — added `N.checkArgNotNull(rowType,"rowType")` matching the sync executor (4207) + the reactive executor's own groupBy(Collection)/groupByAndCount(Collection) guards + its documented `@throws IAE`. Previously null rowType silently yielded Object[] rows. Test: reactive `MongoCollectionExecutorTest#testAggregateWithNullRowTypeThrowsIAE`.

RED→GREEN: with guards reverted both new tests FAIL (mapper: "nothing was thrown"; reactive: NPE not IAE); with guards GREEN. Targeted GREEN: MongoCollectionMapperTest (101) + reactive MongoCollectionExecutorTest (172) = 273, 0F/0E.

## REJECTED candidate (new false positive — add to deliberate list)
- hbase/AnyPut.java of(Object,boolean) null-key: agent claimed null→NPE (doc says IAE) assuming Put(byte[],boolean)→Put(byte[],int,int,boolean) derefs row.length. DECOMPILED hbase-client Put: `Put(byte[],boolean)`→`Put(byte[],long,boolean)`→`checkRow(row)` (IAE on null) BEFORE any length deref (rowIsImmutable=true stores row directly; =false does Bytes.copy AFTER checkRow). So null→**IAE**; existing doc is CORRECT. Do NOT change. (Distinct from the single-arg Put(byte[]) which does deref→NPE.)

## SKIPPED (low value / house-style / suspected — left as-is)
- DynamoDB v1/v2 dead branches (rowClass==null after checkArgNotNull; redundant newQueryRequest copy): harmless, consistent across variants, removal = refactor not bugfix.
- AsyncCassandraExecutorBase abstract execute(String,Object...) edge example (deferred-vs-sync): low confidence, abstract contract; concrete overrides document the synchronous throw correctly.
- Cosmos streamItems @throws CosmosException "deferred-not-eager": optional; sibling queryItems already documents iteration-time; low value.
- CassandraExecutorBase entityToCondition missing-key message reports last missing key only: message-clarity nit, not a correctness issue.
