# Source review — 2026-10-18

## Scope and method

Reviewed the production sources under `abacus-da-all/src/main/java` (69 Java files: 52 classes/interfaces/annotations and 17 package descriptions; there is no root-level `src/main/java`). Independent reviewers covered disjoint slices: DynamoDB v1/base, DynamoDB v2, DynamoDB async/utilities, MongoDB sync, MongoDB mapper/base, MongoDB async, MongoDB reactive, Cassandra v4, CqlBuilder/Cassandra v3, BigQuery/Cosmos, HBase, and Neo4j/utilities. Each reviewer read its files line by line and checked driver/library behavior against the installed jars and sources.

Every reported finding was reproduced or checked independently before it was accepted. Code fixes are covered by regression tests. A reviewer's unrequested public-API move (deleting `com.landawn.abacus.da.aws.AnyUtil` and its test) was discarded and both files were restored to HEAD before any fix was applied.

Baseline on a clean HEAD: **4,507 tests, 0 failures, 0 errors, 0 skipped** using JDK 21. (JDK 25 cannot run this suite: Mockito's inline mock maker cannot self-attach there; the suite was run with the explicit `mockito-core` agent on JDK 21.)

## Findings and fixes

### Cosmos — invalid alias after a FROM-less scalar subquery (P2)

`CosmosContainerExecutor` recognizes FROM-less scalar subqueries so that the surrounding alias-qualification pass does not rewrite their local identifiers. A parenthesized projection was not recognized (`isCosmosFromLessScalarSubquery` handled only `[`, `-`, and `+`), and `abacus-query` had already qualified mapped properties in the raw expression before the pass ran. The generated SQL could be `... (SELECT (c.id) AS c.id) ...`: the `AS c.id` alias is invalid Cosmos SQL.

- Added a post-render normalization that removes the table alias from an `AS` alias anywhere in the rendered condition. An alias is a name, not a property path; `AS c.id` is never valid, and the projection `(c.id)` remains a valid correlated reference.
- A first attempt also treated a parenthesized projection as a subquery scope. Review showed that this suppressed qualification of names that `abacus-query` leaves bare (`category`, `bonus`), producing SQL Cosmos DB rejects. That recognition was removed; the normal qualification pass now prefixes those names with `c.`, and only the alias is normalized afterwards.
- Regression coverage: `CosmosContainerExecutorDeepReview2026SubqueryScopeTest#parenthesizedFromLessProjectionKeepsAliasBare` (mapped `id`, both aliased and unaliased) and `#parenthesizedFromLessProjectionQualifiesNamesLeftBareByAbacusQuery` (`category`, and `id + bonus`). The full class passes 68/68.

### DynamoDB v2 — SS/NS returned as immutable SDK collections (P3)

`toValue` returned `x.ss()` / `x.ns()` directly. AWS SDK v2 documents those lists as unmodifiable, while the `L`, `M`, and `BS` branches return mutable `ArrayList`/`LinkedHashMap` values and the v1 executor returns the model's mutable set lists. Calling `add` on a converted set therefore threw `UnsupportedOperationException` only for v2.

- Both branches now copy into `new ArrayList<>(...)`.
- Regression coverage: `v2.DynamoDBMappingTest#testToValue_SetResultsAreMutableCopies` mutates both converted lists and asserts the source `AttributeValue` is unchanged. The full class passes 96/96.

### MongoDB — non-Document map rows bypassed bean conversion (P3)

`MongoDBBase.extractData(List, Class)` dispatched to a converter only when the first row was a `Document` (or a `Map` target with a `Map` first row). A list of `BasicDBObject` rows requested as a bean fell through to the final branch and was returned as raw maps, contradicting the method's "each object is converted to the target row type" contract.

- Added a bean-target branch that maps each `Map` row through `readRow(new Document(...), rowType)`, copies already-typed rows, and preserves nulls.
- Regression coverage: `MongoDBBaseTest#testExtractDataFromBasicDBObjectListConvertsToBean` converts two `BasicDBObject` rows to `NoIdEntity` and checks their values. The full class passes 114/114.

### Async MongoDB — a failing close could throw from `Future.cancel` (P3)

`CloseOnCancelFuture.cancel()` and `onResult()` called `Stream.close()` directly. `Stream.close()` runs close handlers and may throw; that exception escaped `Future.cancel()` after the cancellation had already succeeded (and escaped the task for a late stream), violating the boolean `cancel` contract.

- Both cleanup sites now use a private `closeQuietly` helper that intentionally swallows a close-handler failure: cancellation has already succeeded and there is no consumer for the cleanup error.
- Regression coverage: two new methods in `AsyncMongoCollectionExecutorDeepReview2026StreamCancellationTest` use a mock `Stream` whose `close()` throws and assert that `cancel` returns true, `isCancelled` is true, and a late publication does not escape. The class passes 44/44.

### DynamoDB — nested formatted numeric wrappers are not writable

A bean containing a nested bean or collection whose numeric property uses `@JsonXmlField(numberFormat = ...)` on a wrapper (`Optional`, `Nullable`, `Holder`) fails while `toAttributeValue` serializes that nested value: `abacus-common` applies the number format to the wrapper object, producing `IllegalArgumentException: Cannot format given Object as a Number`. The read path supports the same property. This originates in the dependency's bean writer and cannot be corrected locally without replacing the serialization path, so it is now documented as unsupported in the `toAttributeValue` contracts (both SDKs). A direct bean property write still works.

## Javadoc corrections

- `DynamoDBExecutor` / `v2.DynamoDBExecutor` (`toAttributeValue`): a nested `ByteBuffer` is rendered from bytes before its position; a read-mode buffer becomes `""`, while a write-mode buffer contributes Base64 text. The previous text claimed every nested buffer became an empty string. The same paragraph now records the formatted-wrapper limitation above.
- `MongoCollectionMapper`: the generated `_id` is not written back for bean entities, but a `Document` entity is passed to the driver unchanged, so the driver does populate the generated `_id` on that same instance. The class summary and the `insertOne`/`insertOne(options)`/`insertMany` method documentation now say this consistently.
- `MongoDBBase.extractData(List, Class)`: the dispatch bullets now state that Map rows (not only `Document` rows) are converted when the target is a bean, and that other objects are copied to a bean target rather than always bypassing conversion.
- `aws.dynamodb.AsyncDynamoDBExecutor`: "non-blocking analogues of every public operation" was false (v1 mapper operations are synchronous); the claim is now limited to direct DynamoDB operations.
- `v2.AsyncDynamoDBExecutor.Mapper`: the v1 mapper reference pointed at a type that does not exist (`aws.dynamodb.AsyncDynamoDBExecutor.Mapper`); it now points to `aws.dynamodb.DynamoDBExecutor.Mapper`.
- `BigQueryExecutor`: a direct read-only (getter-only) column is skipped only when its exposure is `SERIALIZE_ONLY` (the default); a dotted column skips every read-only terminal property. The previous sentence claimed all read-only matches were ignored, which is false for a getter-only property explicitly exposed as `DESERIALIZE_ONLY`.

## Clean review areas

The following slices were read against the installed driver/library sources and produced no substantiated defect:

- **Cassandra v4** (`CassandraExecutorBase`, `CassandraExecutor`): batch construction/settings, paging and row decoding, prepared statements, binding order, UDT/collection codecs, `fromTimeOfDay`/`decodeValue`, and the documented primitive-vs-wrapper NULL behavior.
- **CqlBuilder and Cassandra v3**: `into()`/identifier rules, quoting, batch CQL, driver-3 UDT/TIME access, and `ResultSet` iteration were checked against driver 3.11.5.
- **HBase**: the changed mapping logic, timestamp/time-range handling, `byte[]`/`ByteBuffer` copies, scanner ownership, the `Any*` wrapper validation, and the async facade/`AnyDelete` gap. A late claim that `AnyDelete.toString()` documents nonexistent output was rejected by execution: HBase's inherited `Operation.toString()` returns JSON containing the row key (`{"totalColumns":0,"row":"rowKey",...}`), so the documentation is accurate.
- **MongoDB reactive and sync paths**: `toBson` update construction (including immutable `RawBsonDocument`), primitive/null group-key pipelines, single-field scalar reads, paging/cursor closure, and blocking-twin parity.
- **Neo4j and utilities**: the OGM rollback/`COMMIT_PENDING` cleanup, `cs` constants, `AnyUtil`, the AWS utility placeholders, the search/hadoop/blink/spark placeholders, and all package descriptions.

Coverage caveats: the reviews were timeboxed. Every slice received a targeted pass, but very large files (Cassandra executors, MongoDB executors, HBase executor, Neo4j overloads) were checked at the changed logic, conversion/transaction paths, and sync/async/reactive twins rather than at every boilerplate overload.

## Verification

- Baseline (clean HEAD): 4,507 tests, 0 failures, 0 errors.
- Focused suites after the corrections: 429 tests, 0 failures (Cosmos subquery scope 68, DynamoDB v2 mapping 96, async Mongo cancellation 44, `MongoDBBaseTest` 114, `MongoCollectionMapperTest` 107).
- Full suite from a clean build: **4,516 tests, 0 failures, 0 errors, 0 skipped**; the `abacus-da-all` artifact compiled and tested successfully.
- `git diff --check` passes.
- Javadoc doclint passes on every changed source file (`javac -Xdoclint:all,-missing`, exit 0). Full public-Javadoc generation was blocked because the workspace ACL carries the sandbox's expected `DeleteSubdirectoriesAndFiles` deny, and the Javadoc tool refuses any destination under it; the diagnostic skill confirms that deny is expected and is not removed by repair.

## Files changed

| File | Change |
| --- | --- |
| `azure/CosmosContainerExecutor.java` | Normalize qualified `AS` aliases; keep bare names in parenthesized projections qualified |
| `aws/dynamodb/v2/DynamoDBExecutor.java` | Copy SS/NS; correct `toAttributeValue` Javadoc |
| `aws/dynamodb/DynamoDBExecutor.java` | Correct `toAttributeValue` Javadoc |
| `mongodb/MongoDBBase.java` | Convert non-Document map rows to bean targets |
| `mongodb/AsyncMongoCollectionExecutor.java` | Guard stream cleanup during cancellation |
| `mongodb/MongoCollectionMapper.java` | Correct generated-`_id` Javadoc |
| `aws/dynamodb/AsyncDynamoDBExecutor.java` | Correct async API-scope Javadoc |
| `aws/dynamodb/v2/AsyncDynamoDBExecutor.java` | Correct v1 mapper reference |
| `gcp/BigQueryExecutor.java` | Correct read-only property Javadoc |
| Four test classes | New regression methods for every code fix |

## Process note

The initial reviewer prompts requested read-only findings. One reviewer applied an unrequested public-API move; it was detected from the working tree, all such edits were restored to HEAD, and the remaining reviewers were stopped and asked to report findings without further writes. All code changes in this report were then verified and applied by the root, with red/green test evidence for the behavior fixes.
