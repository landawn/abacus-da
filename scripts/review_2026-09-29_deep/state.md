# Review round 2026-09-29 (deep) — state / ledger

- Task (user): multi-agent thorough line-by-line review of all classes under `./src/main/java`
  (= `abacus-da-all/src/main/java`) for bug fixes + javadoc improvements; unit tests for fixes;
  comment complex/unusual fixes (none for parameter validation).
- HEAD `9671b2b`, clean tree. Deps abacus-common 8.1.0, abacus-query 4.9.4 (unchanged since 09-27 round).
- Process: 21 review+fix agents, each OWNS a disjoint slice of main files + its test classes; ≤6 concurrent.
  Main agent verifies each report against the diff, then runs the full suite and compares to baseline.

## Slices (paths relative to com/landawn/abacus/da/)
| # | Main files | Test classes |
|---|---|---|
| A | aws/dynamodb/DynamoDBExecutor | aws/dynamodb/DynamoDBExecutor01Test, DynamoDBExecutorTest, aws/dynamodb/JavadocExampleTest |
| B | aws/dynamodb/AsyncDynamoDBExecutor, aws/AnyUtil, aws/AWSRDSUtil, aws/AWSS3Util, aws/package-info, aws/dynamodb/package-info | aws/dynamodb/AsyncDynamoDBExecutorTest |
| C | aws/dynamodb/v2/DynamoDBExecutor | DynamoDBExecutorV2Test, DynamoDBExecutorV2_2Test |
| D | aws/dynamodb/v2/AsyncDynamoDBExecutor, v2/package-info | AsyncDynamoDBExecutorV2Test |
| E | mongodb/MongoCollectionExecutor | mongodb/MongoCollectionExecutorTest, mongodb/MongoValidationOrderTest, mongodb/MongoNullValidationTest (shared E/G/H) |
| F | mongodb/reactivestreams/MongoCollectionExecutor | reactivestreams/MongoCollectionExecutorTest, reactivestreams/MongoValidationOrderTest, reactivestreams/MongoNullValidationTest (shared F/I) |
| G | mongodb/AsyncMongoCollectionExecutor, mongodb/MongoDB, mongodb/package-info | AsyncMongoCollectionExecutorTest, mongodb/MongoDBTest, MDBTest, MongoDBExecutorTest, mongodb/MongoNullValidationTest (shared) |
| H | mongodb/MongoCollectionMapper, mongodb/MongoDBBase | MongoCollectionMapperTest, MongoDBBaseTest, mongodb/AnyUtilTest, mongodb/JavadocExampleTest, mongodb/MongoNullValidationTest (shared) |
| I | reactivestreams/MongoCollectionMapper, reactivestreams/MongoDB, reactivestreams/package-info | reactivestreams/MongoCollectionMapperTest, reactivestreams/MongoDBTest, reactivestreams/MongoNullValidationTest (shared) |
| J | hbase/HBaseExecutor, hbase/annotation/ColumnFamily, hbase(+annotation)/package-info | HBaseExecutorStaticTest, HBaseExecutorTest, HBaseExecutorToValueTest, HBaseMapperTest, HBaseExceptionContractTest, HBaseNullValidationTest (shared J/K/L/M) |
| K | hbase/AsyncHBaseExecutor, hbase/AnyScan | AsyncHBaseExecutorTest, AnyScanTest, HBaseNullValidationTest (shared) |
| L | hbase/AnyPut, AnyGet, AnyQuery | AnyPutTest, AnyGetTest, AnyQueryTest, HBaseNullValidationTest (shared) |
| M | hbase/AnyDelete, AnyIncrement, AnyAppend, AnyMutation, AnyRowMutations, AnyOperation, AnyOperationWithAttributes | the matching Any*Test, HBaseNullValidationTest (shared) |
| N | cassandra/CassandraExecutorBase, cassandra/CassandraExecutor | CassandraExecutorBaseTest (shared w/ O), cassandra/CassandraExecutorTest, CassandraExecutor01Test, CassandraValidationOrderTest, cassandra/CassandraNullValidationTest |
| O | cassandra/AsyncCassandraExecutorBase, cassandra/AsyncCassandraExecutor, cassandra/ResultSets, cassandra/package-info | cassandra/AsyncCassandraExecutorTest, CassandraExecutorBaseTest (shared w/ N) |
| P | cassandra/v3/CassandraExecutor, v3/AsyncCassandraExecutor, v3/package-info | v3/CassandraExecutorTest, v3/AsyncCassandraExecutorTest, v3/CassandraNullValidationTest |
| Q | cassandra/CqlBuilder | CqlBuilderTest |
| R | cassandra/CqlMapper, cassandra/ParsedCql | CqlMapperTest, ParsedCqlTest |
| S | gcp/BigQueryExecutor, gcp/package-info | BigQueryExecutorTest, BigQueryExecutorTest2 |
| T | azure/CosmosContainerExecutor, azure/package-info | CosmosContainerExecutorTest, CosmosContainerExecutor2Test |
| U | neo4j/Neo4jExecutor, neo4j/package-info, cs.java, search/*, hadoop/*, blink/*, spark/* | Neo4jExecutorTest, search/*Test, ExceptionContractTest |

## Launch order (rolling, ≤6 concurrent)
Q R N P S A | E F C H O J | I D B G T K | L M U

## Baseline
Clean HEAD in a git worktree (`$SCRATCH/base-wt`) via `tools/base-run-all.sh`: **4171 found, 4034 succeeded, 7 failed, 130 aborted**
(aborted = unreachable-service assumption skips). The 7 failures are the known environmental ones:
HBaseExecutorTest ×4 (live HBase static init), Neo4jJdbcTest#test_01 (no driver), Neo4jOGMTest ×2 (live Neo4j).

## Findings
(filled in as agents report)
- Launched: Q R N P S (while baseline runs).
- A DONE (DDB v1): WS1 P2 read-only inherited getter prop → UOE failed whole item on read → skip (field==null && SERIALIZE_ONLY); WS1 P3 convertValue bracketed non-JSON text → ParsingException → fallback N.convert (9671b2b regression). + 1 doc. Tests DynamoDBExecutor01Test sliceA block. 245/245 + 21/21 live. Diff reviewed OK. Cross-cut → BRIEFING.
- Launched E.
- S DONE (BigQuery): WS1 P2 typed-array row targets (Long[]/byte[][]/Instant[]/Map[]) → ArrayStoreException in readRow + createRowMapper → new toArrayElement helper; test BigQueryExecutorTest#testTypedArrayRowTargetConvertsCellsToComponentType. + docs (typed-array rows, "SELECT *" wording ×3). PROPOSED: delete/exists(Class, null id) → `IS NULL` (lenient, left); STRUCT → List<Long> prop raw strings; TIMESTAMP → Year. Diff reviewed OK. Resumed for read-only-prop cross-cut.
- Q DONE (CqlBuilder): WS1 P2 deleteFrom(..).from(..) silently retargeted / appended 2nd statement → ISE in checkCanAppendCqlFrom; WS1 P3 set(Object/Class,Set) 9671b2b overrides checked arg before state → delegate non-bean to super. Tests CqlBuilderTest#test_deleteFrom_thenFrom_isRejected, #test_set_stateCheckedBeforeEntityArgument. 556/556. Diff reviewed OK.
- P DONE (Cass v3): WS1 P2 9671b2b regression: named-map container check containsKey on TreeMap<Integer,..> → CCE → containsKeySafely; test v3 AsyncCassandraExecutorTest#testSliceP_sortedMapWithNonStringKeys... + 3 docs. v4 twin → forwarded to N. Resumed for read-only-prop cross-cut.
- Launched F.
- R DONE (ParsedCql/CqlMapper): WS1 P2 9671b2b regression #{ids[0]} name masked → unmaskBrackets; WS1 P3 slice `m[:a..:b]` one token → `..` separator; WS1 P3 keep-comments mode block comments scanned as CQL → skip; WS1 P2 CqlMapper.saveTo(File) truncated file before DOM build (DOMException/NoClassDefFoundError) → toDocument() before open. 4 tests RED→GREEN, 213/213, differential corpus unchanged. Diff reviewed OK.
- Launched H.
- S follow-up: WS1 P2 getter-only prop → UOE in toEntity (reachable via generated SELECT and user SELECT *) → skip; test BigQueryExecutorTest#testGetterOnlyPropertyColumnIsSkippedOnRead. 306 found/224 ok/82 aborted (emulator down).
- Launched O.
- P follow-up: WS1 P2 getter-only prop UOE in v3 toEntity + UDTCodec deserialize → isReadOnlyProperty skip; test v3 AsyncCassandraExecutorTest#testSliceP_getterOnlyInheritedPropertyIsSkippedOnRead. 143/143 live. v4 twin forwarded to N.
- Launched J.
- N DONE (Cass v4): WS1 P3 UDTCodec decode by name case-insensitive → by position; WS1 P3 empty Map/Collection/array for parameterless query → IAE → binds nothing; WS1 P2 containsKeySafely (P twin); WS1 P2 getter-only prop skip in toEntity + UDT bean decode. Tests CassandraExecutor01Test sliceN block. 216/216 offline, 81/81 live. Base CLEAN. Diff reviewed OK. v3 parity for empty-container → P resumed.
  PROPOSED/out-of-scope: Dataset.toList (abacus-common) same UOE for query(...).toList(Entity); insert/update write computed getter-only column (same as settled AnyPut item — not applied).
- Launched I.
- C DONE (DDB v2): WS1 P2 getter-only prop skip (A twin); WS1 P2 convertValue ParsingException fallback (A twin); WS2 consumed-capacity docs ×3. Tests DynamoDBExecutorV2Test sliceC block. 435/435 (DDB Local up).
- Launched D.
- E DONE (sync Mongo exec): 0 code; WS2 groupBy/groupByAndCount row-shape docs (live-probed). PROPOSED: (1) distinct(field, Object/Number.class) throws BsonInvalidOperationException on non-string values (GeneralCodec decodes only strings) → forwarded to H as WS1 candidate; (2) scalar read with Bson projection on doc lacking field returns _id in stream/findFirst vs null in list (heuristic needed — left). 201/201.
- Launched B.
- P follow-up 2: empty container for parameterless query mirrored from v4; test v3 AsyncCassandraExecutorTest#testSliceP_emptyParameterContainerForParameterlessQueryBindsNothing. 144/144.
- Launched G.
- F DONE (reactive Mongo exec): 0 code; 5 WS2 doc groups (replace `$`-field IAE via publisher, groupBy dotted nesting/null-key drop, groupByAndCount scalar rowType fails, Bson-projection scalar _id fallback, findOneAndX/mapReduce rowType raw doc). All live-probed. 199/199. PROPOSED: Code/Symbol deprecated BSON types treated as beans (low — left). Sync doc parity → E resumed.
- Launched T.
- J DONE (HBaseExecutor): 0 code; 3 doc (mapper put/delete id wording, ColumnFamily nested-bean flattening). 9671b2b read-only skip verified complete. 180/180.
- Launched K.
- O DONE (Cass v4 async): WS1 P2 ResultSets wrapper getAvailableWithoutFetching threw UOE → rs.map(f).all() failed on async results → currentResultSet.remaining(); test AsyncCassandraExecutorTest#testWrappedResultSet_mapAllAndAvailableWithoutFetching_sliceO. Docs: execute(String,Map) empty-map example updated to N/P behavior; page-fetch notes on 6 stream overloads. memoize verified. 151/151.
  NOTE for user: N/P's empty-container change reverses the 07-26 doc decision, but the ORIGINAL doc (pre-07-26) said an empty map "returns the result set", so N/P restore the author's intent. Kept; flagged in the final report.
- Launched L.
- E follow-up: doc-only — Bson-projection scalar _id fallback (5 overloads), groupBy null-key group (live-probed). `$`-field IAE already on all 8.
- Launched M.
- B DONE (DDB v1 async + aws misc): 0 code; 2 doc (per-request retry claim, returnConsumedCapacity param). 40/40. Parity verified.
- Launched U (last).
- H DONE (MongoDBBase/sync mapper): WS1 P2 stream(MongoCursor, Object/Bson/Map/Document) → readRow scalar fallback IAE/single-value → Document short-circuit; WS1 P2 GeneralCodec.decode non-string BSON → BsonInvalidOperationException (distinct(field, Object/Number.class)) → BsonValueCodec + DocumentCodec decode. Tests MongoDBBaseTest sliceH block; live-probed. + 1 mapper doc. 242/242 + 709 regression. Read-only-prop: Beans.mapToBean tolerant → no failure. Diff reviewed OK.
- I DONE (reactive mapper + MongoDB): 0 code; 6 doc groups (distinct null/array unwinding, groupBy null-key, groupByAndCount scalar IAE, findOneAndX upsert AFTER, projection plus _id). Live-probed. 109/109.
- G DONE (async Mongo + MongoDB): 0 code; 1 doc (4 untyped groupBy/groupByAndCount row shapes). 111 lambda delegates javap-verified. 236/236.
- D DONE (DDB v2 async): 0 code; 3 docs (putItem example capacity/condition, Mapper.batchGetItem multi-table, updateItem primitives). 2 pin tests (inherit C fixes). 93/93. Sync-parity proposal checked by main: the "null properties ignored" wording exists only in v2 async → nothing to do.
- K DONE (AsyncHBase + AnyScan): 0 code; 3 docs (coprocessorService missing table no RPC, of(start,start) get-scan vs replacement, of(Get) shared family map). 123/123.
- M DONE (HBase mutations): 0 code; 3 docs (Append/Increment server timestamp max(existing+1, now) per hbase-server 2.6.6 reckonDelta; AnyDelete default ts also from setTimestamp ×5). 227/227.
- U DONE (Neo4j + cs + stubs): 0 code; 1 doc (cs class doc lists unused checkArg methods). cs: 161 consts all value==name, all used. 103/103.
- L DONE (AnyPut/AnyGet/AnyQuery): 0 code; 3 docs (@Column nested bean → JSON cell, rowIsImmutable only for byte[], AnyGet.of(byte[]) keeps reference). 153/153.
- T DONE (Cosmos): 0 code; 2 docs (default SNAKE_CASE vs SDK default serializer camelCase — live-verified; projection example). 114 ok/19 aborted. PROPOSED: default constructor CAMEL_CASE (public default change — for user); dedupe selectPropNames (low).
## All 21 slices done. Final verification below.
- FINAL: all-classes run on modified tree = **4195 found, 4087 succeeded, 7 failed, 101 aborted** (baseline 4171/4034/7/130).
  Same 7 environmental failures; +24 tests; fewer aborts because slice T revived the Cosmos emulator (stale Postgres lock).
  `mvn -o -q compile` (all modules) exit 0. CRLF/LF preserved in all 41 changed files. Nothing committed.
- Post-review follow-up (user-approved fixes of an external review):
  #1 BigQuery toArrayElement REPEATED cell → typed array component (Instant[][], byte[][][]) bypassed TIMESTAMP/BYTES decoding → route through convertRepeatedValue; test BigQueryExecutorTest#testTypedArrayRowTargetDecodesRepeatedTimestampAndBytesElements (RED DateTimeParseException → GREEN).
  #3 CqlMapperTest invalid-attribute test only hit NoClassDefFoundError (no Jakarta API) → added jakarta.xml.bind-api 4.0.5 test dep (root dependencyManagement + abacus-da-all); test now asserts DOMException INVALID_CHARACTER_ERR; new round-trip test (removed old TODO); new missing-runtime test via child-first loader hiding jakarta.xml.bind. Both RED on HEAD classes.
  #2 (dotted path to getter-only prop) NOT fixed here — root cause in abacus-common BeanInfo.setPropValue path chain (ParserUtil 8.1.0:1959-1963 lacks the read-only skip that 1936 has).
  Full run: 4198 found, 4090 ok, same 7 env failures, 101 aborted. mvn surefire on both classes green.
