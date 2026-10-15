# Source review — 2026-10-12

## Scope and method

Reviewed all 68 Java source files under `abacus-da-all/src/main/java`, the shared production source directory used by the Maven modules. This includes 52 class/interface/annotation files and 16 package descriptions. There is no root-level `src/main/java` directory.

Four agents divided the source by subsystem: AWS, Cassandra, MongoDB, and the remaining packages. Reviews covered executable lines, public contracts, conversion paths, paging, resource lifetimes, allocation behavior, and existing tests. Changes were checked against installed dependency sources where driver behavior mattered. Independent follow-up reviews checked the Cassandra and DynamoDB changes and the BigQuery fix. Existing workspace changes were preserved.

## Fresh review of all local changes

The follow-up request triggered a new line-by-line review of all 12 changed production files, their callers, and the changed or added tests. Four agents rotated responsibilities so the original authors did not provide the only review. The review compared behavior with both the saved original production classes and the first-pass implementation.

Additional findings and corrections:

- **MongoDB map construction:** the mixed-row refactor had bypassed the original capacity-aware map factory for `Document` rows. Restored `IntFunctions.ofMap`, preserving registered creators and maps with only an integer-capacity constructor. A new test passed against the original baseline, failed against the first-pass change, and passes with the correction. Added mixed document/map/bean coverage with leading nulls.
- **DynamoDB numeric inference:** recursive native conversion had bypassed JSON inference for `Number` keys and `Optional`, `Nullable`, or `Holder` chains containing `Number`. Restored that inference without serializing native map values. The fallback is limited to types needing abstract-number inference. Tests cover both SDKs, integer/fractional/null values, nested wrappers, malformed keys, and retained binary values.
- **DynamoDB mapping contexts:** direct native beans and bean arrays historically use database mapping rules; beans in parameterized containers use JSON read rules. Restored those distinctions for exposure annotations, date formats, dotted property names, nested beans, and arrays. Baseline compatibility tests expose the first-pass regressions. Null `Holder` elements also retain the difference between direct native arrays and JSON container reads.
- **DynamoDB native binary wrappers:** the completeness check found pre-existing corruption in optional/nullable/holder binary values. Convert their declared element types before constructing wrappers, preserving empty binary data, remaining buffer slices, generic bean properties, and source positions. Tests include direct properties, nested beans, collections, map values, nested wrappers, and null defaults in both SDKs.
- **DynamoDB container construction:** restored the JSON reader's construction rules for newly recursive containers. Registered suppliers could otherwise merge distinct map keys or alter collection contents. Nested immutable containers now retain their declared wrapper type, enum maps retain their key metadata even when empty, and copy-on-write lists use bulk construction to avoid repeated array copies. The original immediate native-binary supplier path remains intact. Tests cover factories, declared container types, order, nulls, duplicates, and binary contents; independent baseline comparisons also cover collection/queue/deque/sorted-set interfaces and concrete queues.
- **Contracts and edge coverage:** corrected Cassandra Javadocs for incremental row consumption, conversion failure behavior, batch settings, and TIME codec precedence. Added TIME boundary, null, timezone, and codec-failure controls. BigQuery Javadocs now state locale-independent TIME binding, with additional pre-epoch tests. HBase timestamp Javadocs explicitly distinguish family-map construction from `setTimestamp`, backed by tests for map/cell identity and unchanged cell timestamps. Reviewed the existing `CqlBuilder`, `AnyScan`, and `cs` documentation edits against their implementations.

The new conversion helpers are identical between the DynamoDB SDK implementations. Comments explain context propagation, wrapper conversion, abstract-number inference, map factory preservation, and the driver-specific behavior.

## Accepted review-comment corrections — 2026-10-13

- **Cassandra v4 UDT TIME:** after a failed field-codec lookup, TIME fields now convert `Date` values (including `java.sql.Time` and `Timestamp`) and `Calendar` values using the existing ordinary-parameter conversion. Registered codecs retain priority, and their serializer exceptions propagate unchanged. Dates use the default time zone; calendars use their own zone. Milliseconds and timestamp nanoseconds are retained. Other input types and CQL field types retain their prior rejection behavior.
- **Regression coverage:** six new v4 methods cover bean/map/collection and format round trips, pre-epoch dates, timestamp nanoseconds, calendar offsets, source-value preservation, native TIME boundaries/nulls, registered codecs, serializer errors, and original lookup errors for unsupported inputs and non-TIME fields. Two methods fail against the pre-fix classes; four compatibility controls pass. All six pass with the fix. Two new v3 methods explicitly verify that String and Integer TIME values preserve the original lookup exception, including its CQL/Java type metadata, through serialization and formatting; all seven v3 TIME methods pass.
- **Documentation:** corrected the four class-based CQL `from`/`insertInto` overloads, both v3 UDT BLOB-only exception descriptions, the v4 UDT conversion contract and three exception descriptions, and the stale batch-helper reference in the v4 class overview. The required protected batch-method override is retained.

An independent agent reviewed the final implementation, tests, and contracts and replayed all 13 focused TIME tests successfully. These corrections address the accepted parts of review comments 1, 4, 5, 6, and 7; the DynamoDB getter/setter annotation claim was not reproducible because `JsonXmlField` is field-only in the installed dependency.

Final verification for these corrections:

- Full reactor: **4,403 tests run, 4,384 passed, 0 failures, 0 errors, 19 unchanged Cosmos emulator skips**. All seven artifact modules and the parent built successfully.
- All **eight methods added in this follow-up** passed. Across the complete review, 69 methods have been added: 67 in eleven new regression classes and two in existing HBase classes.
- Independently checked all **81 fresh Surefire XML reports** rather than relying on Maven's failure-ignore configuration.
- Forced fresh Javadoc generation with **`doclint=all`**; generation succeeded and the output is newer than the final source. `git diff --check` passed.

Evidence: `target/review-2026-10-13-reactor.log`, `target/review-2026-10-13-summary.json`, `target/review-2026-10-13-javadoc.log`, and focused red/green and independent-review logs under `target/review-2026-10-13/`.

## EnumMap error reporting and documentation cleanup — 2026-10-13

- **Raw EnumMap rejection:** both DynamoDB executors now check the resolved key class before recursively constructing an `EnumMap`. An unresolved enum key produces a descriptive `ParsingException` identifying the resolved type. The previous JSON reader already rejected raw enum maps in nested list/map bean contexts; the recursive converter had replaced that useful error with a constructor NPE. Property metadata resolves a raw declaration to `EnumMap<Object, Object>`, so the failure was not limited to the non-parameterized branch, and it occurred before insertion even for an empty map.
- **Regression coverage:** four new methods exercise both SDKs across list, map, JDK Optional, Abacus Optional, Nullable, and Holder contexts. The 24 empty/populated rejection cases fail with the constructor NPE before the fix and pass with the descriptive exception afterward. Null and absent raw-map properties remain null in all six contexts, with typed wrapper access also checked. Existing typed EnumMap controls retain empty and populated enum-map support.
- **Helper contracts:** documented the private DynamoDB bean/scalar/wrapper helpers and expanded the container-conversion contract to explain context propagation, native versus JSON construction policies, concrete enum key requirements, nested immutable finalization, and bulk copy-on-write list construction. Both public `toEntity` overloads in each SDK document the scoped `ParsingException`. Complex inline rationale remains beside the relevant code; the enum-key validation needs no additional inline comment.
- **Other documentation:** restored the specific MongoDB `toList` `ClassCastException` tag with reachable field-name, map-key, and conversion cases. Moved the Cassandra batch-builder rationale to a single shared comment above all five batch methods. These edits do not change runtime behavior. The field-only `JsonXmlField` annotation finding remains withdrawn.

Focused evidence: `target/review-2026-10-13/enum-map-fix/red.log`, `green.log`, and `factory-controls.log`; baseline/current diagnostic comparison: `target/review-2026-10-13/raw-enum-map/`.

Final verification for this follow-up:

- Full reactor: **4,407 tests run, 4,388 passed, 0 failures, 0 errors, 19 unchanged Cosmos emulator skips**. All seven artifact modules and the parent built successfully.
- Independently audited all **82 fresh Surefire XML reports**. All four new methods and all four existing factory/typed-EnumMap controls passed without skips. The complete review now adds 73 methods: 71 in twelve new regression classes and two in existing HBase tests.
- Forced fresh public Javadoc generation with **`doclint=all`**. Also generated private Javadocs for both DynamoDB executors with **`-Xdoclint:all,-missing`** to check helper syntax and references without requiring documentation on unrelated private members. Both checks passed, and every affected generated page is newer than its final source.
- Independent agents reviewed the final implementation, helper contracts, and new tests without further findings. The sole runtime change in this follow-up is the enum-key guard in the two SDK mirrors. `git diff --check` passed.

Final evidence: `target/review-2026-10-13-enum-map-reactor.log`, `target/review-2026-10-13-enum-map-summary.json`, `target/review-2026-10-13-enum-map-javadoc.log`, and `target/review-2026-10-13-enum-map-private-javadoc.log`.

## Typed conversion, container compatibility, and codec review — 2026-10-14

The latest external review found real gaps despite the earlier green suite. Compared the affected paths against exact HEAD (`38782f5b101b6d3fe3921c1920202f127122606f`) in isolated class directories before implementing corrections. The implementation author and another agent independently reviewed the final DynamoDB production code and new tests; the root reviewed Cassandra and the Cassandra agent reviewed the allocation checks.

- **DynamoDB declared types:** direct native beans and bean arrays now retain full property types, including `Optional<Long/Date/Enum>`, Abacus Optional, Nullable, Holder, Pair, Map.Entry, and enum CODE metadata. Invalid enum codes are rejected instead of being interpreted as ordinals. Resolved `Gen<Number>` properties infer numeric values; HEAD's incorrect String assignment is not restored. Top-level typed wrappers also retain their declared contents when reading native values or the JSON text produced by `toItem`.
- **Three mapping contexts:** distinguish the outer item mapper, direct map-to-bean conversion, and JSON reads. Preserve their different exposure, dotted-name, formatting, null, scalar-to-container, and factory policies. Direct bean containers retain dependency key-collision checks and descriptive rejected-entry errors. Empty Boolean keys retain their null representation instead of overwriting a real false key. Direct bean string lists preserve literal text; JSON-context lists keep JSON parsing. Unsupported date formats on Number fields are rejected.
- **Container construction:** add mutable stand-ins for `ImmutableCollection` and `ImmutableBiMap`, preserve declared immutable results, and restore size-aware native `ArrayBlockingQueue` construction, including empty input. Preserve binary values and source-buffer state while recursively resolving bean/container types. Top-level `CopyOnWriteArrayList<Object>` now joins the existing nested bulk-construction path. Factory-registration tests assert registration succeeds before using their dedicated fixtures.
- **Conversion cost:** scalar and nested typed containers are traversed directly rather than serialized and parsed separately for every bean. A retained warmed benchmark is available at `scripts/performance/DynamoDBConversionBenchmark.java`, with reproduction instructions alongside it. In the second sequential JVM fork, conversion of 10,000 beans containing List/Map/Set properties measured 13.45 ms at HEAD versus 8.36 ms after correction for v1, and 13.19 ms versus 11.18 ms for v2. Across both forks, allocation fell from roughly 20.2–21.1 MB to 11.0–11.6 MB per conversion. Timings varied with machine load and are diagnostic, not test thresholds.
- **Cassandra v3 TIME codec priority:** resolve codecs by actual value through the UDT's current registry, so a Date codec accepts `java.sql.Time` and Timestamp subclasses. Separate lookup from serialization so all serializer exceptions propagate unchanged, including a CodecNotFoundException matching the field/runtime type. Driver 3 exposes no public registry getter; a lazily initialized reflected accessor caches only the Method, never the registry. Tests cover registry replacement, map/list/bean inputs, literal formatting, fallback precision, native nanoseconds, unchanged non-TIME failures, and a forked zone-change probe.
- **Cassandra documentation and validation:** Date conversion uses the default zone captured when Abacus's temporal type initializes; v3 Instant conversion uses the current default. UDT fallback support is enumerated without claiming general parity with scalar parameter binding. Driver 4 consumes encoded rows; the old anonymous wrapper retained an emptied page queue's backing storage and iterator state, not consumed row payloads. The static wrapper still avoids that retention. Validation-order tests now verify that neither `iterator()` nor `all()` is called on invalid input. The required batch-method override remains; no unsupported negative-row-count behavior was added.
- **Allocation regression tests:** all five Cassandra batch factories and both SDKs' top-level copy-on-write conversion compare prebuilt 512- and 2,048-element inputs using warmed per-thread allocated bytes. Four times the input must allocate at most six times as much memory. All five original batch loops fail this check; current builders pass. Both pre-fix copy-on-write paths fail, allocating about 8.4 MB at 2,048 elements, while corrected paths pass at about 25 KB. Unsupported/disabled counter environments abort these three tests cleanly; that path was independently verified. No wall-clock assertion was added.

Evidence is retained under `target/review-2026-10-14-fixes/`; comparison probes against HEAD and the pre-correction implementation are under `target/review-2026-10-14-opinion/`.

Final verification for this follow-up:

- Full reactor: **4,438 tests run, 4,419 passed, 0 failures, 0 errors, 19 unchanged Cosmos emulator skips**. All seven artifact modules and the parent built successfully. Independently audited **86 fresh Surefire XML reports** because Maven's configured failure-ignore behavior makes its success status insufficient.
- Added **31 test methods** in this follow-up: 22 DynamoDB functional methods, three allocation methods, and six Cassandra v3 methods. All passed without skips. Before correction, 17 of the 22 new DynamoDB methods failed and five compatibility controls passed; four substantive Cassandra codec checks failed. The allocation checks independently fail against the original repeated-copy paths. The complete review now adds 104 methods: 102 in sixteen new regression classes and two in existing HBase classes.
- Independent focused replay passed all **63 DynamoDB functional regression methods**, **20 Cassandra TIME/validation methods**, and **58 existing Cassandra compatibility methods**. The capability-disabled allocation replay aborted all three allocation tests with zero failures, confirming portable skip handling without changing global state in the normal suite.
- Fresh public Javadocs passed with **`doclint=all`**. Targeted private Javadocs for both DynamoDB executors, both Cassandra executors, and package-private `ResultSets` passed with **`-Xdoclint:all,-missing`**. The broader private check exposed two existing method-level heading errors in `ResultSets`; both were corrected. That comment-only correction was the sole production edit after the full test run. All affected generated pages are newer than their final source; `git diff --check` passed.

Final evidence: `target/review-2026-10-14-fixes-reactor.log`, `target/review-2026-10-14-fixes-summary.json`, `target/review-2026-10-14-fixes-javadoc.log`, and `target/review-2026-10-14-fixes-private-javadoc.log`. Benchmark forks, baseline failures, isolated successes, and disabled-counter checks are under `target/review-2026-10-14-fixes/`.

## Changes

| Area | Finding and change | Regression coverage |
| --- | --- | --- |
| BigQuery | `java.sql.Time` parameter formatting used the default locale. Locales with non-ASCII digits produced text rejected by the BigQuery SDK. Format with `Locale.ROOT`. | Arabic formatting locale; zero, single-digit, and three-digit fractional milliseconds; restore the locale after the test. |
| MongoDB | Mixed `Document` and other `Map` rows could fail with `ClassCastException` depending on row order. Convert each row according to its actual type and reuse conversion metadata. Preserve `Document` normalization for other map implementations so their lookup semantics cannot alter `_id` handling. | Both row orders, `BasicDBObject`, null rows, `_id` conversion, concrete map targets, unchanged source maps, and identity-based map keys. |
| DynamoDB v1/v2 | JSON round trips corrupted native binary values inside nested containers and beans. Reuse native property conversion recursively, preserving binary values, typed map keys, and generic bean bindings. JSON-stored generic bean properties also retain their declared types instead of putting an `Integer` into a `Long` slot. | Nested maps/lists/arrays, untyped slots, ordinary/generic beans, null/empty binary values, positioned read-only buffers, numeric and optional types, generic keys/arrays, and direct bean round trips. Compatibility controls preserve date/timezone/number formats, enum codes, exposure rules, aliases, raw JSON, defaults, and existing string-array leniency. |
| DynamoDB v1 | Paginated query aggregation appended subsequent pages into the client's first response. Copy the first page's item list when aggregation is needed. | Reusing the same first response across two queries leaves it unchanged and does not duplicate rows. |
| Cassandra paging | The anonymous synchronous wrapper captured the initial async page in a synthetic field, retaining it after advancing. Use a static nested wrapper. | Deterministic strong-reference inspection after advancing, iterator identity, and execution metadata. |
| Cassandra v3/v4 conversion | `toList` and `extractData` first collected every encoded driver row and then converted them. Convert while consuming the iterator, avoiding the intermediate all-rows list. | Conversion before the next page is consumed; conversion failures leave subsequent rows unread; existing mapping tests retained with iterator-capable mocks. |
| Cassandra v4 batches | Repeated immutable `BatchStatement.add` copied prior statements on every iteration. Use a builder in all five batch factories, then apply statement settings. | Statement order, batch type, empty/single batches, consistency, serial consistency, page size, timeout storage, and tracing. |
| Cassandra v3 UDTs | UDT TIME fields lacked the existing scalar TIME fallback for `LocalTime` and `java.sql.Time`. Apply it only when the runtime-value codec is absent. | Bean/map/list inputs, nanosecond precision, SQL millisecond precision, and precedence of registered field codecs. |

Comments explain the non-obvious conversion, retention, and driver behavior. The final follow-up above records measured conversion costs and allocation regression checks in addition to the functional tests.

## Coverage inventory

Paths below are relative to `abacus-da-all/src/main/java/com/landawn/abacus/da`. Every `package-info.java` in these directories was also reviewed.

| Package | Files excluding package descriptions |
| --- | --- |
| root | `cs.java` |
| `aws` | `AWSRDSUtil`, `AWSS3Util`, `AnyUtil` |
| `aws/dynamodb` | `DynamoDBExecutor`, `AsyncDynamoDBExecutor` |
| `aws/dynamodb/v2` | `DynamoDBExecutor`, `AsyncDynamoDBExecutor` |
| `azure` | `CosmosContainerExecutor` |
| `blink` | `DataSetUtil`, `DataStreamUtil` |
| `cassandra` | `CassandraExecutor`, `CassandraExecutorBase`, `AsyncCassandraExecutor`, `AsyncCassandraExecutorBase`, `CqlBuilder`, `CqlMapper`, `ParsedCql`, `ResultSets` |
| `cassandra/v3` | `CassandraExecutor`, `AsyncCassandraExecutor` |
| `gcp` | `BigQueryExecutor` |
| `hadoop` | `HadoopUtil`, `HDFSUtil` |
| `hbase` | `HBaseExecutor`, `AsyncHBaseExecutor`, `AnyOperation`, `AnyOperationWithAttributes`, `AnyMutation`, `AnyQuery`, `AnyAppend`, `AnyIncrement`, `AnyRowMutations`, `AnyDelete`, `AnyGet`, `AnyPut`, `AnyScan` |
| `hbase/annotation` | `ColumnFamily` |
| `mongodb` | `MongoDBBase`, `MongoDB`, `MongoCollectionExecutor`, `MongoCollectionMapper`, `AsyncMongoCollectionExecutor` |
| `mongodb/reactivestreams` | `MongoDB`, `MongoCollectionExecutor`, `MongoCollectionMapper` |
| `neo4j` | `Neo4jExecutor` |
| `search` | `ElasticsearchExecutor`, `LuceneExecutor`, `SolrExecutor` |
| `spark` | `DatasetUtil`, `DStreamUtil` |

The Blink, Hadoop, Search, and Spark utility classes are currently placeholders. No additional substantiated fixes were identified in the unchanged packages. Documented eager Neo4j result materialization and the documented MongoDB custom-codec behavior were preserved.

## Verification before review-comment corrections (2026-10-12)

- Full final reactor: **4,395 tests run, 4,376 passed, 0 failures, 0 errors, 19 skipped**. All seven artifact modules and the parent aggregator built successfully.
- At this checkpoint the change set added **61 test methods**: 59 in ten new regression classes and two in the existing HBase tests. Every added method passed without skips. The 19 existing skips remain the local Cosmos emulator's unsupported SQL-query and partition-key-deletion operations.
- Independently parsed all **80 fresh Surefire XML reports**, excluding stale reports. This is necessary because the repository hardcodes `testFailureIgnore=true`; Maven's exit status alone does not establish that tests passed.
- Both DynamoDB SDKs pass all 37 focused regression methods. Independent comparisons with saved baseline classes cover factory policies, collection/queue/deque/sorted-set interfaces, concrete queues, nested immutable containers, empty and populated enum maps, mapping contexts, numeric inference, and wrapper null handling. The four new factory tests fail against the pre-correction implementation; the original baseline passes their compatibility controls and reproduces the existing binary corruption.
- A warmed local allocation probe for a 4,096-element copy-on-write list conversion measured **34,603,256 bytes before and 999,696 bytes after** per conversion. The probe is under `target`; no machine-dependent allocation or timing threshold was added to unit tests.
- Regenerated all shared-source public Javadocs with **`doclint=all`** and failure-on-error enabled. Generation succeeded, and the output was verified newer than the frozen final source. `git diff --check` passed.

Final commands were `mvn -o test -Dmaven.test.failure.ignore=false -Dgpg.skip=true` and `mvn -o -pl abacus-da-all -am javadoc:javadoc -DskipTests -Dgpg.skip=true -Dmaven.javadoc.failOnError=true`, with a fresh Javadoc stale-data path to force regeneration.

Verification artifacts for this checkpoint: `target/review-2026-10-12-followup-final-reactor.log`, `target/review-2026-10-12-followup-final-summary.json`, and `target/review-2026-10-12-followup-final-javadoc.log`. Focused regression and baseline logs are under `target/review-2026-10-12/`. Subsequent review-comment corrections are recorded above.

## Verification from the initial pass

- Saved pre-fix production classes passed all 4,315 runnable existing tests, with 19 aborted tests. That baseline run also included the three new MongoDB/BigQuery test methods: all three failed as expected (4,337 total methods).
- Cassandra: 975 existing tests and 12 new tests pass. Nine of the new methods fail against the saved baseline; the other three preserve existing batch settings and custom-codec behavior.
- DynamoDB: 638 existing tests and 20 new tests pass. Fifteen new methods fail against the saved baseline; the other five check existing conversion behavior.
- MongoDB: 134 focused tests pass, including four new methods. Both mixed-row regression methods fail against the baseline; two compatibility controls preserve special map semantics.
- Initial full reactor: **4,371 tests run, 4,352 passed, 0 failures, 0 errors, 19 skipped**. All seven artifact modules and the parent aggregator built successfully. All 37 added test methods in seven new classes passed without skips. Twenty-seven added methods reproduce failures against the saved baseline; ten are compatibility controls.
- The 19 skips are unchanged environment limitations: 18 Cosmos SQL-query tests and one partition-key deletion test are unsupported by the local emulator.
- Ran `mvn -o test -Dmaven.test.failure.ignore=false -Dgpg.skip=true`. The repository hardcodes Surefire's failure-ignore setting, so verification also independently parsed all 77 fresh XML reports and checked the new classes and exact failure/error counts. Final `git diff --check` passed.

Verification artifacts are in `target/review-2026-10-12-final-reactor.log`, `target/review-2026-10-12-final-summary.json`, and the subsystem logs under `target/review-2026-10-12/`. Existing Cassandra test doubles in two files were updated to expose iterators; their mapping assertions remain intact.

The installed Cassandra driver 4.19.3 has an independent defect: `DefaultBatchStatement.getTimeout()` returns `null` even when its timeout field is populated. The batch change avoids copying through that getter and preserves the stored setting. It does not change the driver's getter or request-timeout behavior.
