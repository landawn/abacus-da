# Review round 2026-10-02 (deep) — state / ledger

- Task (user): multi-agent thorough line-by-line review of all classes under `./src/main/java`
  (= `abacus-da-all/src/main/java`) for bug fixes; unit tests for fixes; comment complex/unusual fixes
  (none for parameter validation).
- HEAD `b462b7d` (= 09-29 round's fixes), clean tree. Deps abacus-common 8.1.0, abacus-query 4.9.4 (unchanged).
- Process: 21 review+fix agents, each OWNS a disjoint slice of main files + its test classes; ≤4–5 concurrent
  (memory-limited this time). Main agent verifies each report against the diff, then runs the full suite and
  compares to baseline.

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

## Launch order (rolling)
Q R N P S A | E F C H O J | I D B G T K | L M U

## Baseline
(clean HEAD in a git worktree `$SCRATCH/base-wt` via `tools/base-run-all.sh`; JVMs capped -Xmx because the
Windows commit limit was nearly exhausted by other sessions — first attempt died with malloc failure)
**4198 found, 4090 succeeded, 7 failed, 101 aborted** — identical to the 09-29 final. The 7 failures are the known
environmental ones: HBaseExecutorTest ×4 (live HBase), Neo4jJdbcTest#test_01, Neo4jOGMTest ×2.

## Findings
(filled in as agents report)
- Launched: Q R N P S (after baseline), then A.
- R DONE (ParsedCql/CqlMapper): BUG P3 MyBatis marker glued to map/UDT field separator (`{street:#{street}}`) not rewritten
  (paramCount 1 not 3, raw `#{..}` sent to driver) → split token at `:#{` at brace depth>0 (indexOfGluedLiteralIbatisParameter);
  BUG P2 CqlMapper.saveTo(File) serializer failure (namespace-prefixed attr `p:t`) after truncation → file emptied →
  serialize to ByteArrayOutputStream first. Tests ParsedCqlTest#testParse_IbatisMarkerGluedToLiteralFieldSeparator_isRewritten,
  CqlMapperTest#testSaveToFile_SerializerFailure_keepsExistingFile. 110/110; differential corpora byte-identical. 09-29 hunks OK.
  PROPOSED: namespace-prefixed attrs can't be saved (URI not kept on load); null attr value saved as "". Diff reviewed OK.
- Launched E.
- A DONE (DDB v1): MSG P3 mapper(Class) no-@Table / Mapper ctor non-bean messages rendered "class class X" →
  ClassUtil.getCanonicalClassName; test DynamoDBExecutor01Test#testMapper_ErrorMessagesNameClassOnce. 246/246. 09-29 read-only
  skip & convertValue fallback verified (32 bracketed strings, nested beans). PROPOSED: zero-attribute item (projection of a
  missing attr) read into scalar type → IAE "Column count must be 1" (documented @throws; behavior change → left). Diff OK.
  Cross-cut to C/D (v2 twins).
- ENV: all Docker containers stopped externally at 07:06 local (FinishedAt 14:06Z; no agent ran docker — grep of transcripts).
  Not restarted. mongod (Windows service) still up.
- S DONE (BigQuery): BUG P2 REPEATED STRUCT → List<Bean>/Bean[] went through JSON codec → raw TIMESTAMP/BYTES text into bean →
  row failed → decodeRepeatedElement maps via toEntity(subFields); BUG P3 TIME "HH:mm:ss.ffffff" → java.sql.Time failed (abacus
  parse) → LocalTime.parse + millis; MSG P3 update(entity) keyless → "primaryKeyNames cannot be null" → "No key names defined".
  Tests BigQueryExecutorTest#testRepeatedStructColumnMapsBeanElementsWithDecodedFields, #testTimeCellWithFractionalSecondsReadsIntoSqlTime,
  #testUpdateEntityWithoutKeyPropertiesReportsMissingKeyNames. + 4 docs (primitive valueClass NULL → default). 310 found/228 ok/82 aborted.
  PROPOSED: bean props dropped from generated SQL / `address.city AS home.city` (abacus-query sub-entity, like settled Cosmos item);
  STRUCT bean prop reflective getSchema; single-value REPEATED → String[] (settled root cause). Diff reviewed OK.
- Launched F, C.
- Q DONE (CqlBuilder): BUG P2 implicit SET / column-DELETE exclusion used QueryUtil.idPropNames only → registerKeys keys kept
  (Cassandra rejects SET/DELETE of PK cols) → keyPropNames = CassandraExecutorBase.getKeyNames (falls back to idPropNames);
  BUG P2 update(Class).usingTTL/Timestamp(..) before set(..) expanded implicit SET at once → explicit set appended (duplicate
  cols / unbound ?) → appendUsingOption defers expansion, init override expands at where()/build(); BUG P3 failing Dsl
  factory calls never released builder → parent activeStringBuilderCounter leak (WARN>512/ERROR>1024) → Dsl.setUpOrRelease in
  14 factories. Tests CqlBuilderTest#test_sliceQ_registeredKeys_areExcludedLikeIdProperties, #test_sliceQ_updateEntityClass_usingBeforeSet_explicitSetReplacesImplicitList,
  #test_sliceQ_failedDslFactory_releasesBuilder. 559/559 + 116 related. PROPOSED: select(..).into(..) INSERT…SELECT (no such
  CQL; renders `(a, b)SELECT` without space) → reject in checkCanAppendCqlInto (into() doc allows it — left). Diff reviewed OK.
- Launched H.
- N DONE (Cass v4): BUG P3 single GregorianCalendar/ByteBuffer param treated as named-param bean (Beans.isBeanClass true) → IAE
  "Missing required parameter" → isBeanParameter = isBeanClass && N.typeOf(cls).isBean(); BUG P3 CQL time LocalTime (fractional
  sec) → java.sql.Time failed (N.convert text parse) → convertValue/toPropValue Time.valueOf+millis (toEntity, UDT, single-value,
  typed arrays); MSG UDTCodec "class X". Tests CassandraExecutor01Test#testSliceN_singleBeanLikeValueTypeParameterIsBoundPositionally,
  #testSliceN_udtCodecUnsupportedJavaTypeMessageNamesTheClass, #testSliceN_timeColumnWithFractionalSecondReadsIntoSqlTime. 220/220 offline.
  Base CLEAN; 09-27/09-29 hunks verified. PROPOSED: prepareStatement conversion loop doesn't catch DateTimeParseException
  (LocalDateTime → Instant) — judgment, left. v3 twins forwarded to P. Diff reviewed OK.
- Launched O.
- P DONE (Cass v3): BUG P2 CQL time decoded by driver 3 as Long nanos-of-day → N.convert/setPropValue read as epoch millis →
  wrong LocalTime/java.sql.Time (live: 10:00 → 09:00) → timeOfDayValue helpers by column/UDT field TIME type across toEntity, UDT,
  extractData, readRow/createRowMapper single+typed array, readFirstColumn (sync queryForSingleValue/NonNull now via readFirstColumn);
  BUG P3 isBeanParameter (N twin); MSG UDTCodec class name. Tests v3 AsyncCassandraExecutorTest#testSliceP_timeColumnReadIntoLocalTimeTargetsUsesNanosOfDay,
  #testSliceP_singleValueTypeParameterWithGettersIsBoundAsValueNotBean, #testSliceP_udtCodecUnsupportedJavaTypeMessageNamesTheClass. 51/51.
  PROPOSED: v3 bind of LocalTime/LocalDate → CodecNotFoundException (feature); Row/Object target multi-column findFirst/stream/gett
  IAE (v4 same). NOTE for user: leftover probe table `simplex.slice_p_time_probe` (DROP timed out as Docker went down). Diff OK.
- Launched J.
- F first report (reactive Mongo exec): 0 code; DOC aggregate(List, Class) single-value rowType skips empty/null outputs (live).
  PROPOSED BUG P3 groupByAndCount with a group field named "count" + non-Document rowType: project.append(fieldName,"$_id") then
  append(_COUNT,1) overwrites → key silently lost (live). Main decided: APPLY (IAE eager, same message in sync+reactive) —
  F resumed, E asked for sync twin.
- C DONE (DDB v2): 0 bugs; MSG ×2 (A twin class name; toItem/toUpdateItem "Unsupported type" wording → v1's, incl. Object[]).
  Tests DynamoDBExecutorV2Test#testMapper_ErrorMessagesNameClassOnce, #testToItem_UnsupportedTypeMessageListsAcceptedShapes. 242/242.
  09-29 skip/fallback verified. PROPOSED: SS/NS returned as SDK unmodifiable list (v1 mutable); toAttributeValue(AttributeValue) → S
  debug string (extension of settled SdkBytes item); java.util.Date written without millis (abacus-common default format). Diff OK.
- Launched D.
- F DONE (follow-up): BUG P3 groupByAndCount "count" group field → checkGroupFieldNotCountColumn IAE on the count=true non-Document
  projection path (both groupByPipeline overloads); @throws on 2 public overloads. Test reactive MongoCollectionExecutorTest#testGroupByAndCountRejectsGroupFieldNamedCountForNonDocumentRowType_sliceF
  (RED→GREEN; Map/Object/bean; Document + groupBy unchanged). 290/290. Diff reviewed OK. Mapper doc twin → I.
- Launched I.
- E DONE (sync Mongo exec): BUG P2 query(Collection selectPropNames, …)→Dataset: dotted select name ("address.city") column ALL null
  (server returns nested; newDataset reads flat key/top-level prop) → dotted columns filled via getPropValueByPath, Dataset.columns
  rebuild (array path → CCE like list/findFirst); BUG P3 groupByAndCount "count" (F twin). Tests MongoCollectionExecutorTest#testQueryWithDottedSelectPropNameFillsNestedColumn_sliceE,
  #testQueryWithDottedSelectPropNameEmptyAndArrayResults_sliceE, #testGroupByAndCountRejectsGroupFieldNamedCountForNonDocumentRows_sliceE.
  307/307 + 356 regression. PROPOSED: bulkWrite null element message "'requests' cannot be null" (cs constant) — left. Diff OK.
  Twins: reactive Dataset bug → F resumed; mapper docs (groupByAndCount IAE, query CCE/dotted) → H; async query(Collection) ×4
  CCE doc → G (in G's prompt).
- J DONE (HBaseExecutor): CLEAN, 0 changes. 09-27 read-only skip complete on all read paths; 40-type × 3 naming round-trip probe.
  174/174. PROPOSED: java.util.Date encoded via N.stringOf → whole-second UTC text → millis lost (also Date row keys) — storage
  format change → user decision (same root cause as C's DDB FYI: abacus-common JUDateType default format).
- Launched G.
- F follow-up 2: BUG P2 reactive query(Collection…)→Mono<Dataset> dotted columns (E twin) → collectList().map(extractDataWithDottedColumns);
  CCE via onError; prose doc on 4 overloads. Test reactive MongoCollectionExecutorTest#testQueryDatasetResolvesDottedSelectNamesFromNestedDocuments_sliceF
  (RED [null,null] → GREEN). 292/292. Diff OK. Mapper doc twin → I.
- Launched B.
- O DONE (Cass v4 async + ResultSets): 0 code; DOC delete(Class, Condition) ×2 examples used non-PK filter (Cassandra rejects at
  prepare) → Filters.in("id", …). 2 pin tests (multi-page ResultSets contract; async facade reads all pages + page-fetch failure).
  46/46. 09-29/09-27 hunks verified; 80/80 method parity; all one-shot maps memoized.
- Launched T.
- D DONE (DDB v2 async): MSG ×2 (A twin); BUG P3 list/query futures were thenComposeAsync dependents → cancel()/orTimeout()
  never reached the page chain → all remaining pages fetched (probe: 100 pages after cancel) → separate result future completed
  via completeWith; page helpers stop when resultFuture.isDone(); failure passed as-is (get/exceptionally unchanged). Tests
  AsyncDynamoDBExecutorV2Test#testMapper_ErrorMessagesNameClassOnce, #testListAndQueryStopPaginatingOnceReturnedFutureIsCancelled
  (RED→GREEN), pin #testListAndQueryForwardPagesAndFailuresToReturnedFuture. 96/96. Diff reviewed OK.
- Launched K.
- I DONE (reactive mapper + MongoDB): 0 code; DOC twins (groupByAndCount IAE ×2, dotted query ×4, aggregate null-skip),
  DOC reactive MongoDB.collectionExecutor/collectionMapper(MongoCollection): supplied collection keeps its own codec registry →
  nested-bean writes fail unless from db() (live). Pin test reactive MongoCollectionMapperTest#testGroupByAndCountRejectsGroupFieldNamedCountForEntityMapper_sliceI. 110/110.
- B DONE (DDB v1 async + aws misc): CLEAN; 2 pin tests (async scan empty attributesToGet; getter-only skip via async getItem). 42/42.
- G DONE (async Mongo + MongoDB): 0 code; DOC dotted query ×4 (CCE via future), queryForSingleValue UTC note (H's change).
  123 delegates bytecode-verified. 236/236 + 68 live.
- K DONE (AsyncHBase + AnyScan): CLEAN, 0 changes. 47 delegates; ~50 AnyScan doc claims probed; offline ConnectionImplementation harness. 123/123.
- T DONE (Cosmos): BUG P2 `??` coalesce counted as 2 placeholders → IAE "parameter count mismatch" when combined with a bound
  condition → skip adjacent `??`; BUG P3 raw `LIKE 'a!%' ESCAPE '!'` → `c.escape` → ESCAPE keyword when a literal follows.
  Tests CosmosContainerExecutorTest#testCoalesceOperatorIsNotCountedAsPlaceholders_sliceT, #testLikeEscapeKeywordIsNotAliasQualified_sliceT. 79/79.
  ~220-query differential: only coalesce lines changed. PROPOSED: ternary `?` in raw expr (no safe heuristic); raw subqueries
  garbled by alias qualifier (doc?); SCREAMING_SNAKE_CASE renders `c.ID`/`c._TS` for system props (behavior change); dead IS ? branches. Diff OK.
- H DONE (MongoDBBase + sync mapper): BUG P1 toEntity: typed container props kept decoded element types (List<Address> of
  Documents, List<Long> of Integer, List<Enum> of String, Map<Integer,..> String keys) → CCE on element access; embedded
  subdocument arrays unreadable as beans (live) → normalizeDecodedProperties/toDeclaredType/toDeclaredElement copy-on-write pass;
  BUG P2 LocalDate/LocalDateTime/LocalTime read in JVM zone but driver Jsr310 codecs write UTC → day shift west of UTC (live) →
  convertBsonValue reads Date in UTC; BUG P2 GeneralCodecRegistry.get(clazz, registry) ignored calling registry → nested-bean UUID
  "uuidRepresentation has not been specified" (live) → codec bound to calling registry + OverridableUuidRepresentationCodec;
  BUG P2 GeneralCodec isEntityClass via isBeanClass only → GregorianCalendar/ByteBuffer written as {timeZone..}/{position,limit}
  (live) → + N.typeOf(cls).isBean(); ByteBuffer → BSON binary. Tests MongoDBBaseTest#testToEntityConvertsTypedContainerElementsToDeclaredTypes,
  #testToEntityElementConversionKeepsSpecialContainersAndUninstantiableElements, #testJavaTimeLocalTypesAreReadInUtcLikeTheDriverCodecsWriteThem,
  #testNestedBeanUuidFollowsTheCallingRegistryUuidRepresentation, #testValueTypesWithBeanAccessorsAreNotEncodedAsBeanDocuments;
  MongoCollectionMapperTest#testGroupByAndCountOnCountFieldRejectedForBeanMapper. Mapper doc twins + distinct null docs.
  1052/1052 across 16 Mongo classes. PROPOSED: toDocument(bean) getter-only (public-field beans IAE/dropped; doc says getter/setter);
  BsonTimestamp → Long NFE. Diff reviewed OK.
- MAIN applied doc twins: reactive MongoCollectionExecutor.queryForSingleValue UTC sentence (G's note); sync MongoDB.collectionExecutor/
  collectionMapper(MongoCollection) codec-registry paragraph + db()-based examples (I's proposal; same withCodecRegistry wiring).
- Launched L, M, U (last).
- INTERIM full run (18 slices + main twins): 4236 found, 3843 ok, 184 failed, 209 aborted. Failures = 7 known env + 177 live
  Cassandra (v4 80 + v3 95 + 2 ExceptionInInitializerError — static init, Cassandra container down since 07:06). Aborts up
  because DynamoDB Local / Cosmos / BigQuery emulators down. No code-caused failures.
- L DONE (AnyPut/AnyGet/AnyQuery): CLEAN, 0 changes. All delegations checked vs hbase-client 2.6.6 sources; probes L5/L6. 159/159.
- U DONE (Neo4j + cs + stubs): CLEAN, 0 changes; cs cross-check OK (all refs exist, 161 used). 103/103. PROPOSED doc caveat:
  save() on a cleared pooled session doesn't delete relationships removed from an entity loaded by an earlier call (OGM
  deleteObsoleteRelationships uses the mapping context; offline EntityGraphMapper probe) → MAIN APPLIED the caveat to
  save(Object) and save(Object, int) docs (omission of a surprising data-semantics gotcha).
- M DONE (HBase mutations): CLEAN, 0 main changes; 1 pin test AnyDeleteTest#testDefaultTimestamp_comesFromSetTimestamp_sliceM
  (09-29 doc claim). ~200 claims probed. 228/228.
## All 21 slices done. Final verification below.
- CRLF: all 38 changed files consistent CRLF (3 test files lack a final newline — same as HEAD).
- FINAL all-classes run on modified tree: **4237 found, 3844 succeeded, 184 failed, 209 aborted**.
  Like-for-like baseline re-run on clean HEAD in the SAME environment (Docker services down): **4198 / 3805 / 184 / 209** —
  identical failing classes (Cassandra v4 ×81 + v3 ×96 static-init, HBaseExecutorTest ×4, Neo4j ×3) and aborts.
  Net: +39 tests, all passing; no regressions. (Morning baseline with services up: 4198/4090/7/101.)
- `mvn -o -q compile` (all modules) exit 0. Baseline worktree removed. Nothing committed.
- Totals: 22 code bug fixes (1 P1, 11 P2, 10 P3) all with RED→GREEN tests; ~9 exception-message fixes; doc fixes in ~12 files.

## VERIFICATION PASS (user request: independently re-review ALL local changes line by line)
- Briefing: `VERIFY_BRIEFING.md`. HEAD classes rebuilt at `$SCRATCH/out/head` (worktree `$SCRATCH/base-wt` recreated);
  `tools/test-head.sh` runs current tests against HEAD classes for RED proofs.
- Reviewers (independent of the authors): verifyMB (MongoDBBase), verifyME (Mongo executors/mappers/MongoDB ×7),
  verifyQB (CqlBuilder), verifyCS (Cass v4/v3/async base), verifyBQ (BigQuery), verifyDD (DDB v1/v2/v2-async),
  verifyPC (ParsedCql/CqlMapper), verifyCN (Cosmos/Neo4j/AnyDeleteTest).
- Launched: MB ME QB CS BQ DD.
- Launched: PC CN (all 8 running).
- verifyPC DONE: ParsedCql split core OK (index/size++ correct; strings/$$/comments not split); INCOMPLETE fixed: guard skipped
  tokens STARTING with a marker → `{#{k}:#{v}}`-style key+value left `#{v}` → guard `indexOf("#{", 1) < 0`; DOC Known-limitation
  paragraph of parameterizedCql() was false for MyBatis after `,` (worked on HEAD too) → rewritten. CqlMapper.saveTo OK (exceptions
  identical, bytes identical). Tests ParsedCqlTest#testParse_IbatisMarkerGluedAfterMarkerKey_isRewritten (RED),
  #testParse_IbatisMarkerGluedToLiteralFieldSeparator_allShapes (RED), #testParse_GluedIbatisLookalikeInStringOrComment_isNotSplit (pin);
  CqlMapperTest#testSaveToFile_SerializerFailure_createsNoParentDirOrFile (RED), #testSaveToFile_CreatesParentDirsAndReplacesLongerFile (pin).
  115/115; corpora: only intended lines differ. PROPOSED: `{ #{k }:#{v}}` (space in key marker) still unconverted; `{#{x}}` documented limitation.
- verifyCN DONE: Cosmos `??` OK (abacus-query never emits adjacent placeholders; left-to-right pairing = ParsedSql's count;
  ~580-line corpus only intended lines differ); ESCAPE OK (nextChar after whitespace; case-insensitive); Neo4j caveat verified vs
  OGM 5.0.10 + doclint clean; AnyDelete pin OK. COMMENT: `??` comment extended (raw sub-query bindings). Tests
  CosmosContainerExecutorTest#testCoalescePairingAndPlaceholderCountingEdgeCases_verifyCN, #testCoalesceNextToRawSubQueryBindingAndRawQuestionMarkMismatch_verifyCN,
  #testLikeEscapeKeywordSpacingAndNamingPolicies_verifyCN (all RED on HEAD); AnyDeleteTest pin strengthened (byte[] tombstone types).
  131/131 + Neo4j 82/82. PROPOSED: entity property named like a keyword (like/and/escape) gets alias-qualified inside raw exprs
  (upstream abacus-query; pre-existing).
- verifyBQ DONE (1st): REGRESSION fixed: REPEATED STRUCT → List<record> worked on HEAD (JSON path) but failed after slice S
  (toEntity stores raw text into immutable-bean ctor slots: "argument type mismatch"; root cause pre-existing for record row
  targets) → toEntity converts to jsonXmlType when entityInfo.isImmutable; REGRESSION fixed: isDecodedElementClass used
  Beans.isBeanClass → List<GregorianCalendar> silently got "now" calendars → N.typeOf(..).isBean(). TIME fix / update msg / docs OK.
  Tests BigQueryExecutorTest#testRecordTargetsConvertCellTextToComponentTypes (RED), #testRepeatedStructNonBeanElementTypesKeepPreviousMapping (pin),
  #testRepeatedStructBeanElementsInEveryContainerAndNesting (RED), #testTimeCellEdgeValuesAndParameterRoundTrip (RED). 183/183.
  Remaining regression vs HEAD (bean element with parameterized sub-prop Map<String,Long>, generic element bean G<Long> → raw
  Strings; HEAD's JSON path typed them) → MAIN asked verifyBQ to close it (resumed).
- verifyQB DONE: all 3 CqlBuilder fixes OK (49 orderings × 12 dialects; 22 factory sites; registerKeys-after-first-use works).
  0 main changes. Tests CqlBuilderTest#test_verifyQB_usingBeforeSet_everySetOverloadReplacesImplicitList (RED),
  #test_verifyQB_usingBeforeWhere_pendingImplicitListFinalizedByEveryClause (pin), #test_verifyQB_registerKeys_afterFirstUse_takesEffect (RED),
  #test_verifyQB_failedDslFactory_releasesBuilder_afterLaterSetUpStep (RED). 556/556.
- verifyDD DONE: all DDB fixes OK; 249-case HEAD-vs-tree differential (9 entry points × 25 page scripts) identical results,
  request counts, exception shapes (get/join/exceptionally/handle/whenComplete); 200k pages no SOE; stream/scan lazy, batchGet
  single. DOC: list(QueryRequest) Map overload + Mapper.list/query early-completion notes. Tests AsyncDynamoDBExecutorV2Test#
  testEveryPaginatingListAndQueryStopsWhenReturnedFutureCompletesEarly_verifyDD (RED 16 offenders), #testListAndQueryFailureShapesMatchPlainDependentStage_verifyDD (pin).
  628/628. PROPOSED: toKeyAttributeValue array message uses getName(); post-cancel in-flight page still converted (CPU only).
- MAIN: DDB v1+v2 toKeyAttributeValue "must be scalar, not [Ljava.lang.String;" → ClassUtil.getCanonicalClassName (verifyDD's nit;
  same class as the round's message cross-cut). Tests DynamoDBExecutor01Test/DynamoDBExecutorV2Test#testAsKey_NonScalarMessageNamesArrayTypeReadably
  (RED on HEAD, GREEN).
- verifyME DONE: all Mongo executor/mapper/MongoDB changes OK; live HEAD-vs-tree corpus (6 row types × 13 name shapes, sync/
  reactive/mapper/async) only intended diffs; groupByAndCount matrix identical sync/reactive. 0 main changes. Tests: sync+reactive
  executor (dotted all overloads × Map/LinkedHashMap/Document/bean, CCE ×4, rowType matrix for "count"), sync+reactive mapper
  delegation, async (ExecutionException←CCE), live MongoDBExecutorTest, MongoDBTest ×2 codec-registry pins. 867/867; 11 RED on HEAD.
  PROPOSED/FOR MAIN: public MongoDBBase.extractData(selectPropNames, …) helpers still leave dotted columns null (pre-existing;
  generic helper — left for user); optional dedupe of sync inline block vs reactive extractDataWithDottedColumns + getPropValueByPath.
- verifyBQ follow-up DONE: (a) REPEATED STRUCT → generic element bean (List<G<Long>>) back on JSON codec (isDecodedElementClass
  takes container Type; toEntity path only for non-parameterized beans); (b) readStructProperty/isTypedContainer: STRUCT → typed
  container prop (Map<String,Long>, List<Long>, Map<String,Bean>…) via propType.valueOf(N.toJson(plain)) in toEntity + bean Dataset
  (supersedes settled 09-29 item — round made it a regression). Tests #testRepeatedStructGenericElementBeanResolvesTypeArgument (pin vs
  HEAD), #testRepeatedStructElementTypedContainerPropertiesMatchHead (pin vs HEAD), #testStructIntoTypedContainerPropertyConvertsElementTypes (RED).
  186/186. Remaining minor diffs vs HEAD: generic-bean SUB-property of an element bean (C.gen: G<Long>) now String v (HEAD Long;
  trade-off vs TIMESTAMP decoding — left); element List<Long> sub-prop typed (HEAD NFE); List<Object> sub-prop now [3,"bee"] like top level.
- verifyCS DONE: all 6 Cassandra hunks OK (isBeanParameter ~75-class probe; v4 LocalTime→Time 4013 times × 7 zones identical where
  HEAD worked, + whole-minute times fixed; v3 timeOfDayValue all read paths incl. stream/async). BUG fixed (write-side twin of P):
  v3 binding java.util.Date/Time/Timestamp/Calendar to a `time` column stored epoch millis as nanos (10:15:30.123 → 00:00:00.0657)
  → N.convert(v, LocalTime).toNanoOfDay(). COMMENT/DOC corrections (whole-minute LocalTime; prepareStatement value-type params).
  Tests CassandraExecutor01Test#testVerifyCS_valueTypeParametersWithGettersAreBoundAsValues, #testVerifyCS_timeColumnReadsIntoSqlTimeOnEveryReadPath,
  v3 AsyncCassandraExecutorTest#testVerifyCS_dateOrCalendarBoundToTimeColumnBindsTheTimeOfDayInNanos, #testVerifyCS_valueTypeParametersWithGettersAreBoundAsValues,
  #testVerifyCS_timeColumnEdgeValuesUdtStreamAndAsyncReadPaths (all RED on HEAD). 278/278. PROPOSED: v3 Instant/ZDT/ODT/LDT → time
  column still millis-as-nanos (with P's LocalTime-bind proposal); v3 list<time> elements Long; Date target for time column.
- verifyMB DONE: REGRESSION fixed: toDeclaredElement treated every array element type as abstract (Class.getModifiers) →
  Integer[][] from [[1L,2L],[3]] ArrayStoreException (HEAD OK), List<String[]>/List<int[]> stayed Lists, Map<Integer, byte[]>
  values stayed Binary → arrays/ByteBuffer convertible; REGRESSION fixed: null key + Date value → IAE in binaryPropertyInfo →
  null name returns null; PERF: per-row full container copy + re-check → fast path for typed non-containers, copy-on-first-change
  (3000 → 580 ns/row on large typed containers; HEAD 115). Element-conversion failures now throw (same exception HEAD throws for a
  scalar prop — documented). Records now readable. UTC/codec-registry/value-type encoding verified (44 types; HEAD-written docs read
  identically). 8 tests, all RED on HEAD; 98/98, 16 Mongo classes 1072/1072. Note: UTC tests are RED on HEAD only on a non-UTC JVM
  (abacus AbstractTemporalType.DEFAULT_ZONE_ID is cached at class init — can't force a zone in-process).
- verifyCS follow-up DONE: v3 toTimeOfDay(Object): LocalTime as-is; LocalDateTime/OffsetDateTime/ZonedDateTime/OffsetTime →
  toLocalTime(); Instant → systemDefault zone; Date/Calendar → N.convert. HEAD: LDT/ODT/ZDT/Instant silently millis-as-nanos
  (~00:26:17.98), LocalTime/OffsetTime CodecNotFoundException. Registered codecs still take precedence. Test renamed
  v3 AsyncCassandraExecutorTest#testVerifyCS_dateTimeValueBoundToTimeColumnBindsTheTimeOfDayInNanos (RED on HEAD). 54/54.
## All 8 verifiers done. Final verification below.
- VERIFY FINAL: full run on tree **4286 found / 3893 ok / 184 failed / 209 aborted**; clean HEAD in the same environment
  **4198 / 3805 / 184 / 209** — identical failing classes (Cassandra live ×177 static init, HBaseExecutorTest ×4, Neo4j ×3) and
  aborts. +88 tests vs HEAD, all passing. `mvn -o -q compile` (all modules) exit 0. `-Xdoclint:all/protected` on all 20 changed
  main files: 0 warnings/errors. No unused imports / debug output / disabled tests in the diff. CRLF intact. Worktree removed.
  Verification pass applied: 4 regression fixes (BigQuery record lists + value-type elements + typed-container/generic typing;
  MongoDBBase array elements + null-key Date), 2 completeness fixes (ParsedCql marker-key glue; v3 Date/Calendar/java.time → time
  column bind), 1 perf fix (MongoDBBase copy-on-change), DDB scalar-key message, doc/comment corrections, ~50 new tests.

## 2026-10-03 external-review follow-up (user approved "go ahead")
Issues raised by an external reviewer, assessed by main (SDK 2.72.0 bytecode: REPEATED cell = FieldValueList.fromPb(list, null) →
schema-less; record elements inside REPEATED schema-less):
1. [P2, regression] BigQuery nested STRUCT inside repeated bean elements: readStructProperty → readRow(struct) needs attached schema.
2. [P2, pre-existing] BigQuery dispatch `instanceof FieldValueList` before List → real REPEATED values treated as RECORD everywhere.
3. [P3, incompleteness] MongoDBBase toDeclaredElement maps generic bean elements by raw class (G<Long>.value stays Integer).
4. [P3, incompleteness] ParsedCql glued `:#{v}` after a multi-token key marker (metadata/whitespace) left verbatim.
Pre-fix snapshot `$SCRATCH/out/pre1003`; `tools/test-on.sh` (MAINTAG=pre1003) for RED proofs. Agents: fixBQ (#1+#2), fixMB (#3), fixPC (#4).
- fixPC DONE (#4): shared helper splitGluedLiteralIbatisParameter; applied at top of loop AND to every token pulled in by the
  MyBatis reconstruction (before updateLiteralState → brace state exact). Fixes metadata/whitespace keys, multiple pairs, nested
  `{ #{ k }:{a:#{x}} }`; outside braces / `[...]` / quotes / `$$` / kept comments not split. Broken value markers after a multi-token
  key now rejected like the single-token form. 5 tests RED on pre1003 + HEAD; 1 pin. 686/686 (ParsedCql/CqlMapper/CqlBuilder/NullValidation).
  Corpora (1324 inputs): only intended lines changed. PROPOSED (pre-existing): `{ #{k}:$$x$$ }` whole token flagged dollar-quoted.
- fixBQ DONE (#1+#2): isRecordCell/isRepeatedCell (FieldValue.getAttribute) at every dispatch site (toEntity, unwrapRepeatedValue,
  decodeRepeatedElement, toMapValue, readRow collection/single, toArrayElement, createRowMapper ×2, extractData, readSingleValue);
  schema threaded from the enclosing Field (subFieldsOf, new readRow(FieldList, row, class), readStructProperty, readRepeatedCell,
  readRecordCell). Real-client-decoded probe (FieldValueList.fromPb in package com.google.cloud.bigquery): 106/126 cases threw on
  pre1003 AND HEAD → all work. verifyBQ corpora (plain-List shape) byte-identical; SDK-shape rebuild matches plain shape.
  Intended change: single-value reads of a REPEATED column now return unwrapped element values (supersedes settled 09-27
  "single-value REPEATED unwrap" — real payloads threw on HEAD). 8 tests (7 RED on pre1003, 6 RED on HEAD, 1 pin). 194/194.
  Left: user code calling N.convert directly on a raw REPEATED FieldValueList (no wrapper → can't tell from schema-less record).
- fixMB DONE (#3): typeArgumentPropTypes(Type) via ParserUtil.getBeanInfo(type.reflectType()) (abacus resolves prop types against
  ParameterizedType args) — cached (ConcurrentHashMap for parameterized, ClassValue for plain); props resolving to Object skipped
  (raw/G<?>/G<Object> unchanged). normalizeDecodedProperties converts type-argument props for any value kind; nested generic bean
  props built via toDeclaredElement; arrays use type.elementType(). Also fixed bound type variables (LongBox extends Box<Long> —
  broken on HEAD too). 4 tests (3 RED on pre1003; all RED on HEAD). 12 Mongo classes 1060/1060. Perf: plain rows within noise.
  Left: dotted key into type-arg prop; _id into superclass-bound type-variable id. MAIN asked fixMB (resumed) to also convert
  scalar values for immutable beans (records: Integer → long component "argument type mismatch", pre-existing; BigQuery twin fixed).
- fixMB follow-up DONE: immutable beans (records) convert EVERY value to its declared type (PropConversions cache: typeArgumentTypes,
  convertEveryValue=isImmutable, idPropInfo) — Integer→Long/short/float/BigDecimal, Date→Instant, String→enum, ObjectId→String all
  failed on HEAD with "argument type mismatch"; `_id` now fills a record's String/ObjectId id component (unless the doc has it).
  Tests #testToEntityConvertsEveryValueOfARecord_fixMB, #testToEntityPassesIdToTheIdComponentOfARecord_fixMB (RED pre1003 + HEAD).
  1062/1062 Mongo. Perf A/B vs pre1003 within noise.
- FINAL (10-03): full run on tree **4306 found / 3913 ok / 184 failed / 209 aborted**; clean HEAD same env **4198 / 3805 / 184 / 209**
  — identical failing classes (Cassandra live ×177 static init, HBaseExecutorTest ×4, Neo4j ×3) and aborts; +108 tests vs HEAD, all
  passing. `mvn -o -q compile` exit 0; doclint all/protected on 20 changed main files: 0. CRLF/imports/debug checks clean. Worktree removed.
- 10-03 external review #2: [P2 regression of fixBQ] REPEATED cell read into an explicit FieldValueList target (queryForSingleValue/
  queryForSingleNonNull, single-column row mapper, FieldValueList[] array-row element) -> readRepeatedCell unwrapped and N.convert
  tried to rebuild FieldValueList: "No default constructor found". MAIN fixed in readRepeatedCell: FieldValueList-assignable target
  that the raw payload is an instance of -> return raw (same rule as FieldValueList bean props); class Javadoc + comments updated.
  Test BigQueryExecutorTest#testSdkRepeatedColumnReadIntoFieldValueListKeepsRawPayload (RED on the pre-fix build out/final3 with the
  reported message; GREEN). 195/195. doclint clean.
- 10-03 external review #3: [P3 doc] class Javadoc (and the new test's comment) claimed a FieldValueList single-column ROW gets the
  raw REPEATED payload, but toList(result, FieldValueList.class) takes the collection-row branch first (N.newCollection → "Not able
  to create instance for collection"; pre-existing). MAIN narrowed both to bean property / queryForSingleValue|NonNull /
  FieldValueList[] array-row element and stated the row type is unsupported. Not implemented: raw-row passthrough for a
  FieldValueList row type (separate design choice). 195/195, doclint clean.
- 10-04 external review #4: [P2/P3 regression] REPEATED STRUCT element LongBox (extends Box<Long>) routed through toEntity (element
  type not parameterized) → INT64 text left as String in erased Object field → CCE (HEAD JSON path gave Long); same for a Box<Long>
  STRUCT property of a non-generic element bean (verifyBQ's "trade-off"). Root cause: toEntity relies on PropInfo.setPropValue,
  which converts only on assignment FAILURE; and readStructProperty/decodeRepeatedElement map by raw class. Agent fixBQ2: up-front
  N.convert(value, propInfo.jsonXmlType) when not an instance of the resolved clazz + type-aware toEntity via
  ParserUtil.getBeanInfo(type.reflectType()) for parameterized beans; drop JSON special-case for generic elements.
- fixBQ2 DONE (#4): toEntity converts up front (N.convert(value, jsonXmlType)) when the value isn't an instance of the resolved
  propInfo.clazz (immutable beans: always; CharSequence props left to setPropValue for isJsonRawValue); private toEntity(fields,row,
  BeanInfo); readBean(schema, struct, Type) via ParserUtil.getBeanInfo(type.reflectType()) for parameterized bean props/elements;
  JSON special case for generic elements dropped (G<Instant>/G<byte[]> now decoded). Corpora: only intended `gen` lines (String→Long).
  7 tests (5 RED on pre1004 + HEAD, 2 pins). 202/202. Perf: typed rows ~680→88 ms/100k (setPropValue failure exceptions avoided).
- FINAL (10-04): Docker services were back up → strongest run so far. Tree **4314 found / 4206 ok / 7 failed / 101 aborted**; clean
  HEAD same env **4198 / 4090 / 7 / 101** — identical 7 env failures (HBaseExecutorTest ×4, Neo4j ×3) and aborts; live Cassandra v3/v4,
  DynamoDB Local, Cosmos/BigQuery emulator tests all green. +116 tests. mvn compile OK; CRLF OK; worktree removed.
- 10-04 external review #5: [P2 regression of fixBQ2] the up-front conversion skipped every CharSequence-typed property (to keep
  setPropValue's isJsonRawValue JSON handling), but a type variable bound to a text type has an erased Object field:
  List<Box<StringBuilder>> held a String, List<Box<String>> fed by a REPEATED STRING held the ArrayList (both typed via JSON before
  fixBQ2); SbBox extends Box<StringBuilder> / Box<StringBuilder> STRUCT prop also raw (subclass case pre-existing). MAIN fixed: the
  text exclusion now applies only when the field/setter itself would reject the value (isStoredAsIs: declared field/setter type
  doesn't accept it → setPropValue's raw-JSON/convert path runs as before); erased storage converts. Test
  BigQueryExecutorTest#testTypeVariablesBoundToTextTypesAreConverted (RED on fixBQ2 build pre1004b; also RED on pre1004 via the
  pre-existing subclass case). 203/203; BigQueryExecutorTest2 + ExceptionContractTest 58 ok / 82 aborted (emulator) unchanged; doclint 0.

## 2026-10-04 TEST-COVERAGE pass (user: "add unit tests for each fix if not yet or missed fix")
Briefing `COVERAGE_BRIEFING.md`. 6 agents: coverageBQ (BigQuery + conversion matrix test), coverageMB (MongoDBBase + matrix),
coverageME (Mongo executors/mappers + end-to-end of MongoDBBase changes), coverageCS (Cassandra v3/v4 + live round-trips; drop
leftover simplex.slice_p_time_probe), coverageCQ (CqlBuilder/ParsedCql/CqlMapper + live prepare check), coverageDC (DynamoDB v1/v2/
v2-async + Cosmos + Neo4j + AnyDelete; live DDB Local / Cosmos emulator checks). Services up except BigQuery emulator.
- coverageMB DONE: 10 fixes mapped (F1–F10); gaps filled: per-read-path proofs (readRow, stream(cursor/iterable), bean Dataset),
  codec-registry overrides beyond UUID, GregorianCalendar alone, null-key per value kind, isNarrowedTypeVariable setter-only/container
  branches, record id variants. 9 tests (7 RED on HEAD, 2 pins) incl. 244-cell BSON-kind × position matrix (150 cells RED on HEAD).
  No main changes. 124/124. FOR USER: upstream abacus-common 8.1.0 ParserUtil resolves array props by SIMPLE name →
  java.util.Date[] prop typed java.sql.Date[] (toInstant() UOE), java.time.Duration[] → abacus Duration[]; same on HEAD; not worked around.
- coverageDC DONE: 7 fixes mapped; gaps filled (Mapper-ctor message sites never proven — earlier tests stopped at @Table assertion;
  toUpdateItem message; exclusiveStartKey single-page pin; ESCAPE "-literal branch; coalesce shapes). MISSED TWIN applied: second
  toKeyAttributeValue message "received [B" / `$` nested names → ClassUtil.getCanonicalClassName (v1+v2). LIVE: DynamoDB Local
  (proxy-counted requests: cancel/orTimeout during page 2 stops at 2 requests on all 7 entry points; HEAD 3), Cosmos emulator
  (`??` + bound condition; LIKE…ESCAPE + bound condition — HEAD 400). 13 tests (12 RED on HEAD, 1 pin). All 9 classes green.
- coverageCS DONE: mutation check (34 single-site reverts) — 33 caught; survivor v3 row-mapper later-row branch (v3:1279) → new
  multi-row test. 8 tests (4 offline + 4 LIVE v3/v4 round-trips: time-column binds of LocalTime/Time/Date/Timestamp/Calendar/java.time,
  single Calendar/ByteBuffer params, time reads on every sync/async path + UDT) — all RED on HEAD. 464/464 across 9 classes (live incl.).
  No main changes. simplex.slice_p_time_probe already gone (DROP IF EXISTS run). FOR USER: v4 java.time → time column fails loudly
  (DateTimeParseException; settled sliceN item); a Long bound to a time column = epoch millis in v4 vs nanos-of-day in v3 (CQL meaning).
- coverageCQ DONE: Q1–Q3, P1, M1 mapped; gaps filled (sliceQ registered-keys test stopped at first site on HEAD → assertAll over all
  sites/overloads/dialects; USING overloads usingTTL(String)/usingTimestamp(Date)/usingTimestampMicros; glued-marker key kinds &
  statement shapes; saveTo write failure now UncheckedIOException (HEAD: UncheckedException(TransformerException)); LIVE prepare+apply
  of the fixed renderings on Cassandra 5.0 — HEAD fails with the exact server errors). ParsedCql mutation check 15 mutants, 10 killed,
  5 equivalent on well-formed input. 765/765. No main changes. FOR USER/MAIN: Cassandra rejects bind markers inside set/list/map
  literals → map-literal glued-marker shapes only turn a syntax error into a server error (UDT shape works). MAIN fixed the false
  ParsedCql Javadoc sentence "(tags + {?} works)" → UDT literals/subscripts/bind whole collection (`tags = tags + ?`).
- coverageBQ DONE: all BigQuery fixes mapped; gaps filled (single-value/typed-array/N.convert for REPEATED STRUCT beans & records;
  TIME on converter/NonNull/stream/Time[]; schema threading into scalar/Object[]/raw Dataset + width check). Conversion MATRIX:
  18 kinds × 12 positions (208 combos) × 2 payload shapes × 4 read paths = 1664 exact-type checks; RED: HEAD 1086, pre1004 340,
  pre1004b 54, tree-before-fix 16. MISSED FIX applied (pre-existing on HEAD): REPEATED STRUCT element into a Collection/array element
  type (List<List<Long>>, Long[][], List<Object[]>…) unwrapped as Map → NFE/JSON text/HashMap → unwrapRepeatedValue(…,
  recordsAsValueLists) gives the value list; Object/Map/raw-List keep Maps (pinned). 6 tests. 209/209 (+Test2/ExceptionContract
  267 ok / 82 aborted emulator). LEFT for user: STRUCT<k STRUCT<a,b>> into Map<String, List<Long>> / nested STRUCT into List<Map>
  (HEAD silently stored HashMap; tree throws NFE).
- coverageME DONE: own fixes already covered (18/18 RED on HEAD); gap = MongoDBBase read-side changes never exercised through an
  executor → 20 tests (16 RED on HEAD, 4 pins): every sync/reactive/async/mapper read path × typed containers, records, generic
  subclasses, UTC Local* single values/rows; live MongoDBExecutorTest for all executor kinds + write-side codec (nested UUID,
  GregorianCalendar, ByteBuffer) + reactive dotted query/groupByAndCount. 904/904 across 12 classes. No main changes.
## Coverage pass done (6/6). Final verification below.
- FINAL (coverage pass, services up): tree **4376 found / 4268 ok / 7 failed / 101 aborted**; clean HEAD same env **4198 / 4090 / 7 / 101**
  — identical 7 env failures (HBaseExecutorTest ×4, Neo4j ×3) and aborts (BigQuery emulator etc.). +178 tests vs HEAD, all passing
  (+61 this pass). `mvn -o -q compile` and `mvn -o -q -pl abacus-da-all test-compile` exit 0; doclint all/protected on 20 changed main
  files: 0; CRLF/imports/debug checks clean; worktree removed. Main changes this pass: BigQuery REPEATED STRUCT → collection/array
  element (coverageBQ), DDB toKeyAttributeValue 2nd message twin (coverageDC), ParsedCql Javadoc collection-literal sentence (main).
- 10-05 external review #6 (2 issues, both judged real regressions vs HEAD for REPEATED STRUCT element beans; both pre-existing at top level):
  (1) isTypedContainer treats Map<String,String>/List<String> as untyped → nested REPEATED/STRUCT sub-field values (ArrayList/Map)
  left in String containers → CCE (HEAD JSON codec gave JSON text); (2) readBean/toEntity up-front N.convert ignores
  @JsonXmlField(dateFormat/numberFormat) → "03/10/2026" IAE (HEAD JSON codec used PropInfo.readPropValue). Agent fixBQ3: codec for
  String-typed containers only when the sub-schema has REPEATED/RECORD fields; String cell text → propInfo.readPropValue(String).
