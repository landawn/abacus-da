# Review round 2026-09-22 (deep) — state / ledger

- Task (user): multi-agent thorough line-by-line review of all classes under `./src/main/java`
  (= `abacus-da-all/src/main/java`) for bug fixes + javadoc improvements; unit tests for each fix
  added to existing test classes.
- HEAD `3a72a70`, clean tree. Deps abacus-common 8.0.1, abacus-query 4.9.3.
- Process: 21 review+fix agents (general-purpose), each OWNS a disjoint slice of main files and
  its test classes; parallel-safe private compile/test tools in the session scratchpad.
  Main agent reviews the final diff + runs the full suite.
- Note: `mvn test` runs only `AbacusDATestSuite` (tag-filtered: base-test/2025 → 98 tests).
  Real baseline = every test class via scratchpad `tools/test.sh` (see Baseline).

## Slices
| # | Main files | Test classes |
|---|---|---|
| A | aws/dynamodb/DynamoDBExecutor | aws/dynamodb/DynamoDBExecutor01Test, DynamoDBExecutorTest, aws/dynamodb/JavadocExampleTest |
| B | aws/dynamodb/AsyncDynamoDBExecutor, aws/AnyUtil, aws/AWSRDSUtil, aws/AWSS3Util, aws/package-info, aws/dynamodb/package-info | aws/dynamodb/AsyncDynamoDBExecutorTest |
| C | aws/dynamodb/v2/DynamoDBExecutor | DynamoDBExecutorV2Test, DynamoDBExecutorV2_2Test |
| D | aws/dynamodb/v2/AsyncDynamoDBExecutor, v2/package-info | AsyncDynamoDBExecutorV2Test |
| E | mongodb/MongoCollectionExecutor | mongodb/MongoCollectionExecutorTest, mongodb/MongoValidationOrderTest |
| F | mongodb/reactivestreams/MongoCollectionExecutor | reactivestreams/MongoCollectionExecutorTest, reactivestreams/MongoValidationOrderTest |
| G | mongodb/AsyncMongoCollectionExecutor, mongodb/MongoDB, mongodb/package-info | AsyncMongoCollectionExecutorTest, mongodb/MongoDBTest, MDBTest, MongoDBExecutorTest |
| H | mongodb/MongoCollectionMapper, mongodb/MongoDBBase | MongoCollectionMapperTest, MongoDBBaseTest, mongodb/AnyUtilTest, mongodb/JavadocExampleTest |
| I | reactivestreams/MongoCollectionMapper, reactivestreams/MongoDB, reactivestreams/package-info | reactivestreams/MongoCollectionMapperTest, reactivestreams/MongoDBTest |
| J | hbase/HBaseExecutor, hbase/annotation/ColumnFamily, hbase(+annotation)/package-info | HBaseExecutorStaticTest, HBaseExecutorTest, HBaseExecutorToValueTest, HBaseMapperTest, HBaseExceptionContractTest |
| K | hbase/AsyncHBaseExecutor, hbase/AnyScan | AsyncHBaseExecutorTest, AnyScanTest |
| L | hbase/AnyPut, AnyGet, AnyQuery | AnyPutTest, AnyGetTest, AnyQueryTest |
| M | hbase/AnyDelete, AnyIncrement, AnyAppend, AnyMutation, AnyRowMutations, AnyOperation, AnyOperationWithAttributes | the matching Any*Test |
| N | cassandra/CassandraExecutorBase, cassandra/CassandraExecutor | CassandraExecutorBaseTest (shared w/ O), cassandra/CassandraExecutorTest, CassandraExecutor01Test, CassandraValidationOrderTest |
| O | cassandra/AsyncCassandraExecutorBase, cassandra/AsyncCassandraExecutor, cassandra/ResultSets, cassandra/package-info | cassandra/AsyncCassandraExecutorTest, CassandraExecutorBaseTest (shared w/ N) |
| P | cassandra/v3/CassandraExecutor, v3/AsyncCassandraExecutor, v3/package-info | v3/CassandraExecutorTest, v3/AsyncCassandraExecutorTest |
| Q | cassandra/CqlBuilder | CqlBuilderTest |
| R | cassandra/CqlMapper, cassandra/ParsedCql | CqlMapperTest, ParsedCqlTest |
| S | gcp/BigQueryExecutor, gcp/package-info | BigQueryExecutorTest, BigQueryExecutorTest2 |
| T | azure/CosmosContainerExecutor, azure/package-info | CosmosContainerExecutorTest, CosmosContainerExecutor2Test |
| U | neo4j/Neo4jExecutor, neo4j/package-info, cs.java, search/*, hadoop/*, blink/*, spark/* | Neo4jExecutorTest, search/*Test, ExceptionContractTest |

## Waves
- W1: E, F, G, J, A, C, O
- W2: N, H, I, U, S, T, Q
- W3: B, D, K, L, M, P, R

W1 launched 21:15 (E a5ef218f61b3bb0e7, F a8a528fdb2f1d272a, G a4a086b5dabeab576, J a53f1ee3b784bbc79,
A a31ab3a82719f12ed, C a33699a036011134c, O ab19cf532d8199b8b).

## Baseline
All test classes via `tools/test-full.sh` (pre-edit): **3937 found, 3829 succeeded, 7 failed, 101 aborted**
(aborted = assumption-skips for unreachable services). The 7 failures are all environmental:
HBaseExecutorTest ×4 (live HBase static init: `UnsupportedOperationException: getSubject is not supported`
on JDK 21+ security manager), Neo4jJdbcTest ×1, Neo4jOGMTest ×2 (live Neo4j).

## Findings
(filled in as agents report)
- F (reactive Mongo exec) DONE: 1 WS2 doc (queryForSingleValue primitive valueType emits default, not empty) + pin test `testQueryForSingleValueMissingFieldWrapperVsPrimitive_sliceF`. 190/190 green.
- W2 N launched (a0e928471248c137d).
- E (sync Mongo exec) DONE: 3 WS2 docs (watch(Class)/watch(List,Class) iterate ChangeStreamDocument<T> not T, example fixed; stream() comment). 185/185 green.
  PROPOSED: (1) getPropValueByPath CCE on dotted path through array (sync+reactive); (2) aggregate/mapReduce/findOneAndX/groupBy(…, Object.class) lack Document-assignable short-circuit → IAE on multi-field docs (sync+reactive); (3) toBson plain-field BsonDocument update not $set-wrapped (sync+reactive).
- W2 H launched.
- A (DDB v1 sync) DONE: WS1 P2 FIX — B (binary) attr → byte[] prop/target silently null (N.convert(ByteBuffer, byte[]) = null) → private convertValue helper (duplicate+copy) in toEntity + toValue; tests DynamoDBExecutor01Test#testToEntity_BinaryAttributeToByteArrayProperty, #testGetItem_SingleBinaryAttributeToByteArray (RED→GREEN; main-agent diff-reviewed). + 4 WS2 docs (scan example missing category projection; class Validation para exception for pass-through overloads; consumed-capacity claims ×3) + 1 WS3 msg (Unsupported type lists Object[]). 255/255 green.
- W2 I launched.
- G (async Mongo + MongoDB) DONE: 4 WS2 docs (deleteOne collation example needs SECONDARY strength for case-insensitive — live-probed; query example Dataset.forEach gives DisposableObjArray not bean → toList(Product.class); class doc ExecutionException wrapping + range checks via future). TEST FIX: MongoDBExecutorTest un-awaited deleteMany cleanup = root cause of the long-known flaky test_update_async (+test_query_async_2) → .get(). 295/295 ×3.
  CROSS-CUT TODO: grep other files for `dataset.forEach(x -> <bean-method>)` sample pattern.
- W2 U launched.
- C (DDB v2 sync) DONE: WS1 P1 FIX — B attr → ByteBuffer target/prop returned buffer with pos==lim (abacus ByteBufferType write-mode) → reads empty, get→put round trip stores EMPTY binary. New private convertValue wraps byte[] (main-agent verified: Object/Serializable targets short-circuit earlier via isAssignableFrom, so no byte[]→ByteBuffer regression). Tests DynamoDBExecutorV2Test#testToEntity_BinaryAttributeToByteBufferProperty_IsReadable, #testGetItem_ByteBufferTargetClass_IsReadable (RED→GREEN). + WS2 batchGetItem consumed-capacity docs ×4 sites, mapper/ctor @throws id fallback, toAttributeValue ByteBuffer remaining bytes. 507/507 green (DDB Local up).
  PROPOSED: SdkBytes value → toAttributeValue stores debug string as S (additive branch); List<ByteBuffer> from BS → base64 IAE (low).
- W2 S launched.
- O (Cass v4 async base) DONE: WS1 P2 FIX — abacus-common 8.0.1 ContinuableFuture.map re-runs func on EVERY get() (main-agent verified in sources :1501-1508) while v4 AsyncResultSet currentPage iterator is one-shot → 2nd get() returned empty/different results (get/findFirst empty, list empty, gett dup replayed as row). Added package-private `memoize(func)` in AsyncCassandraExecutorBase, wrapped 10 base + 6 AsyncCassandraExecutor map sites. Tests AsyncCassandraExecutorTest#testRepeatedGet_* ×4 (3 RED→GREEN). + 4 WS2 docs (queryForSingleNonNull ExecutionException, stream mapping thread, stream page-fetch failures, class-doc exception timing). 165/165.
  PROPOSED: v3 AsyncCassandraExecutor stream(BiFunction) same issue (→ slice P, own private copy); async findFirst primitive targetClass NULL → Optional.of(0) vs sync NPE (edge, left).
  NOTE: machine hit OOM with 7 concurrent agents → run ≤6 at once. Stray hs_err/replay logs in mongodb src dir → delete at end.
- J (HBase exec) DONE: WS1 P2 FIX getFamilyColumnFieldNameMap dropped a prop's own name when an earlier-declared prop claimed it (declaration-order-dependent silent cell loss on read) → keep own name. WS1 P3 FIX toValue EMPTY_QUALIFIER fallback: scalar+nested bean sharing a family lost nested cells → search owning nested bean. Tests HBaseExecutorToValueTest#test_toEntity_* ×4 (RED→GREEN). WS3 mapper() msg "class class" → canonical name (+HBaseMapperTest test). WS2 delete(List) example Arrays.asList → HTable.delete mutates list (UOE after applying) → ArrayList + doc; scan tableName lazy validation doc. 633/633.
  PROPOSED: delete(List) defensive copy (loses HBase retry-list contract); eager TableName validation in scan(); AsyncHBaseExecutor:1510 same Arrays.asList example (→ slice K).
  FACT: N.asList/asSet/asMap in 8.0.1 return Immutable* (verified CommonUtil:20628/20903/20315). Added cross-cutting section to BRIEFING.
- I (reactive Mongo mapper + MongoDB) DONE: 0 code; 7 WS2 docs (queryForSingleValue primitive default (live-probed); queryForString defaultIfEmpty wording; 7 examples Flux<T>→Flux<User>; groupBy/groupByAndCount $project+count stage; findOneAndUpdate/Delete @return empty; insertMany conversion; MongoDB.readRow @throws). 107/107.
- W2 Q launched. T launched earlier (ae14420b08e41d8b4).
- U (Neo4j + cs + stubs) DONE: 0 code; 5 WS2 docs (delete example "one round-trip" false — OGM one DELETE per entity; delete prose no-native-id reload; load @throws IAE OGM cases; findOnly null→empty; cs example checkArgNotNull). cs: 152 consts all value==name, all used. 96/96.
- W3 P launched.
- H (sync Mongo mapper + MongoDBBase) DONE: WS1 P2 FIX toJson(Bson) driver-built Bson (Filters/Sorts/BsonDocument) serialized BsonValue wrappers as beans (`{"a":{"value":1}}`) → decode via DocumentCodec to Document first (std JSON kept); test MongoDBBaseTest#testToJsonDriverBuiltBsonRendersPlainValues. WS1 P2 FIX toList scalar projection sampled ONE value → mixed int32/int64 left Integer in List<Long> → per-row convert; test #testToListScalarConvertsEveryRowNotOnlyWhenSampleNeedsIt. + 5 WS2 docs (stream copy-paste, aggregate example needs $project, query(Bson proj) Dataset columns, mapper distinct doesn't unwind arrays, negative offset). 219/219 + 449 regression.
  PROPOSED: ByteBuffer readRow/toEntity exhausted buffer (write side unsupported anyway) — left. NOTE objectIdToFilter examples now read ObjectId("…") + $oid note since 08-01 (accurate; left).
- W3 K launched.
- S (BigQuery) DONE: WS1 P1 FIX — BYTES (base64) / TIMESTAMP (epoch secs or micros text) cells couldn't be read into byte[]/ByteBuffer/date-time targets (NFE / parse IAE / micros-as-millis / empty buffer) in toEntity/readRow/createRowMapper/extractData/queryForSingleValue(+NonNull) → private decodeTypedValue via FieldValue.getBytesValue/getTimestampValue (main-agent diff-reviewed). 5 RED→GREEN tests in BigQueryExecutorTest. WS1 P2 FIX ByteBuffer param bound as empty STRING → BYTES (test testBuildQueryParameterValue_ByteBufferBindsRemainingBytes). + WS2 whereClause null wording. 166/166; Test2 82 aborted (emulator down) — NOT live-verified.
  PROPOSED: BigDecimal binds as BIGNUMERIC (NUMERIC column coercion unverified); REPEATED BYTES/TIMESTAMP element decoding.
- W3 L launched.
- N (Cass v4 sync) DONE: WS1 P1 FIX BLOB loss both directions (N.convert ByteBuffer↔byte[] + setPropValue BB→byte[] null) → package-private CassandraExecutor.convertValue at ~10 sites (read + prepareStatement bind + UDT). Tests CassandraExecutor01Test#testSliceN_bindByteArrayToBlobMarkerKeepsAllBytes, #testSliceN_blobColumnReadIntoByteArrayTargetsKeepsBytes. WS1 P2 prepareInsert all-null entity threw builder ISE vs documented IAE → rethrow IAE (cause ISE); test CassandraExecutorBaseTest#testSliceN_insert_entityWithNoInsertableProperties_throwsIAE. WS3 idsToCondition keyless msg; test. WS2: delete(... props) "null or empty" docs ×3, SimpleStatement.builder(cql,x) nonexistent ×2, toList sample reused consumed RS. 187/187.
  PROPOSED: Condition with own LIMIT → ISE in findFirst/gett/exists/queryForXxx; single-marker bean arg vs registered TypeCodec (codecFor(colType) ignores registered codecs); execute(query, emptyMap) on no-param query → IAE. v3 BLOB lead sent to slice P.
- W3 M launched.
- K (AsyncHBase + AnyScan) DONE: 0 code (no .map in code); 3 WS2 docs (delete(List) sample Arrays.asList + list-mutation contract; 6 broken `.get(); //` sample lines leaking stray text; class doc map re-runs per get()). AnyScan CLEAN (probe-verified). 118/118.
- W3 B launched.
- Q (CqlBuilder) DONE: 3 WS1 from abacus-query 4.9.3 parent changes — (P1) parent into(Class)/into(String,Class) now bypass overridable into(String): batchInsert(..).into(Account.class) rendered SQL multi-row VALUES, into("t u", Class) skipped table check → new overrides; (P2) batch into(String) not atomic → partial "BEGIN BATCH ..." left on render failure → mutateAtomically; (P2) new parent from(CqlBuilder,alias) derived table → invalid CQL → override rejects with IAE. Tests CqlBuilderTest#test_intoEntityClass_batchInsert_rendersCqlBatch, #test_intoTableNameAndEntityClass_rejectsAlias, #test_batchInto_renderFailure_leavesBuilderUnchanged, #test_fromDerivedTable_isRejected. + WS3 opCqlKeyword in into msgs; WS2 @Override assertNotClosed, stale escaping comment, from(String,String) doc. 56/56 expected-CQL examples byte-match. 536/536.
  PROPOSED: reject select(..).into(t) (contract change); onlyIf rollback named-param counters.
  SETTLED-NEW: CqlBuilder overrides into(Class), into(String,Class), from(CqlBuilder,String) are REQUIRED by 4.9.3.
- W3 D + R launched (last two).
- P (Cass v3) DONE: WS1 P2 memoize in v3 AsyncCassandraExecutor stream(BiFunction) ×2 (private copy); tests testStream_*_RepeatedGetDoesNotReReadResultSet ×2. WS1 P1 BLOB loss both directions (same as v4) → private convertValue at ~10 sites + toEntity/UDT; tests AsyncCassandraExecutorTest#testSliceP_* ×3 + live CassandraExecutorTest#test_sliceP_blobByteArrayRoundTrip (Cassandra live, RED→GREEN). 126/126 live.
  PROPOSED: StringCodec unchecked throws clauses (style); UDTCodec.serialize byte[] into blob field → CodecNotFoundException (loud); dotted nested-path setPropValue BB edge.
- M (other HBase Any*) DONE: 0 code; WS2 AnyRowMutations class doc — RowMutations with Increment/Append IS accepted by mutateRow/checkAndMutate and by Table.batch when cell blocks on (default) — main-agent VERIFIED in hbase-client 2.6.4 RequestConverter.buildNoDataRegionActions (hasIncrementOrAppend) + MultiServerCallable:110-120 → SUPERSEDES the settled "RowMutations Put/Delete-only" note (only the non-cellblock buildRegionAction path throws). + AnyIncrement.toString doc; AnyMutation.setDurability param d→durability (matches cs label). 213/213.
- L (AnyPut/Get/Query) DONE: WS1 P2 AnyPut.create nested bean with public fields wrote ZERO cells (getPropGetters empty) → iterate propInfoList; WS1 P3 top-level public-field prop NPE on getMethod.equals → null-safe. Tests AnyPutTest#testCreate_nestedBeanWithPublicFields_writesNestedCells, #testCreate_topLevelPublicFieldProperty_writtenAndReadBack. + WS2 10 NPE @throws "no getter" clause dropped; AnyQuery example byte[] qualifier; class Entity Mapping Rules; create(NamingPolicy). Write-side round-trip 18/18 layouts×policies. 284/284.
- T (Cosmos) DONE: WS1 P2 abacus-query 4.9.3 renders isTrue/isFalse as literal `IS TRUE` → Cosmos 400 (emulator-confirmed) → rewrite to = true/!= true etc.; WS1 P3 SCREAMING_SNAKE uppercases literals TRUE/NULL → 400 → lowercase standalone literals; WS1 P3 udf.fn( calls wrongly c.-qualified. Tests CosmosContainerExecutorTest#testBooleanIsPredicatesUseCosmosComparisonSyntax, #testLiteralKeywordsAreLowerCasedForCosmos, #testUdfCallsAreNotAliasQualified. + WS2 streamItems @throws non-bean targetClass ×4, rewrite semantics paragraph. 75/75; emulator 112 ok/19 known skips.
  PROPOSED: IS NULL/IS NOT NULL vs missing-property semantics (IS_DEFINED); raw-expression gaps (?: ??, ESCAPE, EXISTS subquery aliases, explicit c. under SCREAMING); non-bean projection names unqualified.
- B (DDB v1 async + aws misc) DONE: 0 code; 4 WS2 docs / 14 sites (class threading: stream sends NO request until iteration; batchWriteItem(Map) consumed capacity; unprocessed.size() counts tables; 11 write methods "completes exceptionally" conversion clause). Verified on 8.0.1: thenRunAsync BiConsumer gets UNWRAPPED ex (ex instanceof X correct), get() → ExecutionException(cause). 40/40.
- D (DDB v2 async) DONE: 0 code; 3 WS2 groups (mapper/Mapper @throws id-fallback; consumed-capacity claims ×6 methods; Mapper.updateItem "only changed attrs" false). +2 pin tests (id fallback; ByteBuffer readable via sync helpers). 87/87.
- R (CqlMapper + ParsedCql) DONE: WS1 P2 FIX ParsedCql mangled CQL dollar-quoted constants $$..$$ (SqlParser collapses whitespace, treats #/--//* as comments → truncation, quotes inside open a literal swallowing the rest) → mask/restore placeholders around tokenize (main-agent code-reviewed); differential 2181 inputs: only 13 $$-inputs changed. Tests ParsedCqlTest#testParse_DollarQuoted* ×4. + WS2 parameterizedCql comment/whitespace collapse + known-limitation reasons; CqlMapper stale sample comment; CqlMapper runtime requirement jakarta.xml.bind (abacus-common 8.0.1 declares it provided; poms only have javax jaxb-api → NoClassDefFoundError on loadFrom/saveTo). 87/87.
  PROPOSED: add jakarta.xml.bind-api to poms; lift `[` bind-marker limitation; CqlMapper.saveTo UncheckedIOException parity with SqlMapper.
- ALL 21 SLICES DONE. mvn -o -pl abacus-da-all test-compile = exit 0. Stray hs_err/replay logs deleted. No MIXED line endings.

## FINAL
All test classes (tools/test-full.sh): **3984 found, 3876 succeeded, 7 failed, 101 aborted** vs baseline 3937/3829/7/101
→ +47 new tests, identical 7 environmental failures (HBaseExecutorTest ×4 getSubject, Neo4jJdbcTest ×1, Neo4jOGMTest ×2).
