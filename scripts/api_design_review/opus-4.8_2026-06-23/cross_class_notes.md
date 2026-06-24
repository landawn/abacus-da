# Cross-class consistency notes (running log)

Jot observations the moment they appear; the SUMMARY is built from this.

## Established conventions to confirm/measure as we go
- (to fill) null/empty return convention: Optional vs null across executors
- (to fill) async return type: ContinuableFuture vs CompletableFuture
- (to fill) naming: `find`/`query`/`list`/`get`/`stream` semantics across executors
- (to fill) `gett` vs `get` (N-library convention) usage
- (to fill) v1 vs v2 DynamoDB API divergence
- (to fill) blocking mongodb vs reactivestreams mongodb divergence
- (to fill) cassandra v3 vs v4 executor divergence

## Observations

### HBase package (Wave 1 — verified)
- ASYNC RETURN CONVENTION: HBase async uses `ContinuableFuture<T>` / `ContinuableFuture<Void>`, single shared AsyncExecutor (no per-call Executor param). Compare to Cassandra/Mongo/DynamoDB async executors to confirm library-wide.
- NULL vs OPTIONAL: HBaseExecutor + AsyncHBaseExecutor return bare T/Result (null/default), NO Optional anywhere. Consistent within HBase. Check whether other executors (Mongo/Cassandra/Dynamo) use Optional — if so that's a cross-package inconsistency.
- PARAM NAMING targetType vs targetClass: HBaseExecutor mixes both (14x targetType / 39x targetClass); AsyncHBaseExecutor uses targetClass only. Watch for the same `targetClass`/`targetType`/`targetType` drift in other executors. Canonical = targetClass.
- CHECKED-EXCEPTION POLICY: HBase Any* wraps IOException->UncheckedIOException (toJson) but LEAKS checked DeserializationException (getAuthorizations/getCellVisibility). Inconsistent. Watch how other wrappers handle provider checked exceptions.
- SELF-TYPED FLUENT BASES: AnyOperation chain uses self-type returns so subclasses need no overrides; AnyScan needlessly re-overrides setColumnFamilyTimeRange (AnyGet doesn't). Pattern to check in other fluent builders (CqlBuilder).
- FACTORY `of(...)` / `create(...)` CONVENTION: `of(...)` = wrap/from-key, `create(...)` = from-entity. The four AnyMutation subclasses have divergent `of` surfaces + slice param naming (rowOffset/rowLength vs offset/length) — cosmetic because toRowKeyBytes/toValueBytes treat byte[] as identity. Watch `of` vs `create` vs `valueOf` naming across all classes.
- REDUNDANT byte[] OVERLOADS: setAttribute(String,byte[]) on AnyIncrement/AnyAppend is observationally equivalent to inherited setAttribute(String,Object) (toValueBytes identity for byte[]). General pattern: watch for byte[] "fast path" overloads that duplicate an Object overload.
- ACCESSOR-NAME ASYMMETRY: AnyGet.familySet() vs AnyScan.getFamilies() for the same concept. Watch get-prefix vs no-prefix accessor naming.
- STATIC toGet(Collection)->List<Get> singular-name-returns-List naming nit; AnyScan lacks the parallel toScan. Watch pluralization of collection-returning statics.
- `.val()` is the package-wide unwrap convention (Any* wrapper -> native HBase object).

### MongoDB package (Wave 2 — verified)
- ASYNC RETURN: AsyncMongoCollectionExecutor uses `ContinuableFuture<T>`/`<Void>` — SAME as HBase async. Two-for-two on ContinuableFuture. (Cassandra/Dynamo async still to check.)
- RETURN-TYPE DISCIPLINE (mongo blocking executor): clean 4-way Optional / Nullable / null(`gett`) / empty + List vs DataSet vs Stream. This is the library's richest return vocabulary — hold other executors to it. `gett()` = "return null instead of empty Optional" (double-t convention, matches N library).
- REACTIVE TWIN return mapping: single->Mono<T>, multi->Flux<T>, scalar->Mono<Boxed>, query->Mono<DataSet>, writes->Mono<Result>. Mono collapses Optional/Nullable/null/gett distinctions (by design).
- SYNC/ASYNC SYMMETRY GAP pattern: async mirror missing ops the sync has (stream() no-filter, estimatedDocumentCount, typed groupBy/groupByAndCount) + missing @Beta annotations that sync carries. WATCH the same one-directional gap in Cassandra/Dynamo async twins.
- ACCESSOR-NAME DRIFT across twins: blocking mongoCollection()/mongoCollectionExecutor() vs reactive coll()/collectionExecutor(). Same role, different names. General theme: accessor naming not standardized across sync/async/reactive trios.
- INSERT RETURN INCONSISTENCY: blocking insertOne/insertMany return void, bulkInsert returns int; reactive returns Mono<Result> for all. Returning the driver result (ids/counts) is the better design.
- UNSAFE Object-vs-varargs OVERLOADS: MongoDBBase toBson/toDocument/toBSONObject/toDBObject each have (Object) + (Object...) pair → Object[] silently binds to varargs "name/value pairs" form. SAME ambiguity class to watch in CqlBuilder / executors (e.g. batch(Object...) vs batch(List)).
- DUAL-MEANING 2nd PARAM: updateOne/updateMany/findOneAndUpdate (Bson,Object) vs (Bson,Collection<?>) — single update spec vs list-of-update-operations dispatched on static type. Footgun pattern.
- BLOCKING vs REACTIVE BEHAVIORAL DIVERGENCE: mapper.distinct() — blocking uses $group/$project pipeline workaround (native distinct throws BsonInvalidOperationException on scalar fields); reactive delegates to native distinct → errors on scalar fields. The only real behavioral (not cosmetic) twin divergence.
- VALIDATION ASYMMETRY: MongoDB.collection() no null-check while collectionExecutor()/collectionMapper() validate; collectionMapper(Class) throws NPE vs IAE elsewhere. Watch fail-fast consistency in other factories.
- MISPLACED CLASS: mongodb/AnyUtil used ONLY by aws/dynamodb/v2/DynamoDBExecutor → cross-package coupling smell; relocate.
- FACTORY NAMING: `of(...)`=wrap/from-key, `create(...)`=from-entity (HBase); mongo uses `db()/collection()/collectionExecutor()/collectionMapper()`. Compare factory verbs across packages in SUMMARY.

### Cassandra package (Wave 3 — verified)
- ASYNC RETURN = ContinuableFuture (3rd family: HBase + Mongo + Cassandra all ContinuableFuture). STRONG library-wide async convention. Cassandra async uses native driver executeAsync (no Executor param), like Mongo; HBase uses a shared AsyncExecutor — minor model difference, both fine.
- @Beta SYNC/ASYNC MISMATCH = CONFIRMED CROSS-CUTTING: CassandraExecutorBase has 20 @Beta, AsyncCassandraExecutorBase has 0. Same as Mongo (sync queryForXxx @Beta, async not). => SUMMARY P-item: async twins systematically drop @Beta annotations. Also CassandraExecutorBase has internal @Beta inconsistency (queryForString/Date @Beta but delegate to non-@Beta queryForSingleValue).
- RETURN DISCIPLINE matches Mongo: get=Optional / gett=null / Nullable / empty, + primitive queryForXxx collapses null->default, + queryForSingleNonNull throws NPE on matched-null. These 3 semantics are now confirmed IDENTICAL across Mongo + Cassandra => intentional library convention (report once in SUMMARY, not per-class noise).
- v3 PACKAGE DEPRECATION GAP: cassandra/v3/CassandraExecutor + v3/AsyncCassandraExecutor have @deprecated JAVADOC but NO @Deprecated ANNOTATION → compiler never warns. P1 fix.
- SHARED DOC DRIFT: execute(Statement) javadoc claims StatementSettings applied, but body doesn't (only prepareStatement helpers apply). Present in BOTH v3 and v4 → accidental copy. (Pattern: watch for javadoc that overclaims vs body.)
- MUTABLE-INTERNAL-COLLECTION LEAK: ParsedCql.namedParameters() returns live mutable map of a POOLED/CACHED instance (corrupts shared state). Same class as HBase AnyMutation.getFamilyCellMap + MongoDBBase getFamilyCellMap. => SUMMARY: audit accessors returning internal collections for defensive immutability. CqlMapper.getAttributes() correctly returns ImmutableMap (the right model).
- SPLIT OVERLOAD CONTRACT: CqlMapper.add(id,ParsedCql) upserts+returns-previous vs add(id,String) rejects-dup+returns-void. Same name, opposite duplicate policy + different return type. (Pattern: watch overload families with inconsistent return/throw contracts.)
- NAMING-ACCURACY: CqlBuilder.parse(Condition,Class) actually RENDERS (mislabel); SQLBuilder calls it renderCondition. (Pattern: `parse` used for a render/build op.)
- FACTORY NAMING: cassandra executors have NO static factories (telescoping ctors + async()/sync() accessors). Differs from the rest — but internally consistent across v3/v4.
- UDTCodec.serialize/deserialize (v4) are non-@Override v3-migration shims that bloat surface; native driver v4 uses encode/decode.

### aws/gcp/azure/neo4j/hadoop (Wave 4 — verified)
- **ASYNC RETURN-TYPE BREAK (headline):** v2/AsyncDynamoDBExecutor returns native CompletableFuture (193x, 0 ContinuableFuture). EVERY other async executor — AsyncHBaseExecutor, AsyncMongoCollectionExecutor, AsyncCassandraExecutorBase, AND v1 AsyncDynamoDBExecutor (126x ContinuableFuture, 0 CompletableFuture) — uses ContinuableFuture. => P2 DECISION: wrap v2 to ContinuableFuture, or move library to CompletableFuture and document. This is THE biggest cross-class inconsistency.
- SDK-MIRROR RETURN STYLE: DynamoDB (v1+v2) and HBase intentionally return SDK result types / bare nullable T (NOT get=Optional). Mongo/Cassandra/BigQuery use the Optional/Nullable/gett discipline. Two legitimate "schools" split by whether the executor mirrors a vendor SDK closely. Document the rationale; don't force-converge.
- v1<->v2 NAMING DRIFT (Dynamo): toAttributeValue/toAttributeValueUpdate (v1) vs attrValueOf/attrValueUpdateOf (v2) — same semantics, accidental drift. Plus v2 missing async() bridge + toMap(Object[]); v1 missing toEntity(Result,Class). Pairwise parity gaps.
- PHANTOM-<T> MICRO-PATTERN (recurring): method declares <T> bound only to a Class<T> param but returns untyped Dataset/Nullable -> dead type var. Confirmed: Cassandra query(Class<T>,Condition), v1 AsyncDynamoDB query(QueryRequest,Class<T>):1674, BigQuery queryForSingleValue:1767. Fix = Class<?>. (v2 dynamo + v2 sync already use Class<?> correctly.) => SUMMARY consolidation.
- NULL-CONTRACT INCONSISTENCY within a class: BigQuery insert(Object) checkArgNotNull (IAE) vs update/delete NPE on entity.getClass(). Same pattern as Mongo MongoDB.collection() validation gap. => audit single-entity DML for uniform fail-fast.
- COSMOS (@Beta) thin library path: only streamItems over Condition; MISSING get->Optional point-read (404 throws), exists, count, paged/findFirst Condition variants. Round out before GA.
- NEO4J query()->Stream collides with the family convention query()->DataSet / stream()->Stream. (OGM verbs load/loadAll are intentional.)
- EMPTY PUBLIC PLACEHOLDERS: AWSRDSUtil, AWSS3Util, HadoopUtil, HDFSUtil — public, zero members, zero refs. Architectural: prune / @Beta / package-private. (search/blink/spark placeholders are already package-private = fine.)
- @Beta GRANULARITY: Cosmos streamAllItems has method @Beta but Condition streamItems don't. Plus the systemic sync-has-@Beta / async-lacks-@Beta gap (Mongo + Cassandra). => library-wide @Beta audit.
