# Cassandra and Cosmos review — 2026-10-31

## Findings and changes

- Fixed scalar row mappers in both Cassandra driver versions retaining the first decoded value's assignability. An assignable value followed by a SQL/CQL NULL could bypass the target's empty Optional/Nullable default; changing decoded runtime types could also bypass conversion and return the wrong type. Each row now checks its own value. This also removes the mutable per-mapper assignability/type fields. The conversion rule is documented, and the unusual NULL interaction is explained next to the fix.
- Changed the legacy Cassandra UDT bean decoder to advance field metadata alongside field values. Previously, converting each time or collection property could scan the UDT's field list again from the beginning, causing quadratic metadata traversal. The decoder now uses one iterator per bean without retaining another metadata cache. Ignored/read-only properties still advance the iterator, and raw Long time values retain their original representation.
- Corrected CQL builder documentation that implied Cassandra generates missing primary keys or joins related entities when nested property paths are selected. Clarified that those paths must correspond to UDT fields in the selected table.
- Corrected Cosmos examples and contracts: If-None-Match does not make item creation conditional; If-Match on an upsert is checked only on replacement, so a concurrently deleted item may be recreated; partition deletion acceptance does not mean background cleanup has completed. The ETag statements were checked against the official [CosmosItemRequestOptions API for azure-cosmos 4.83.0](https://learn.microsoft.com/en-us/java/api/com.azure.cosmos.models.cosmositemrequestoptions?view=azure-java-stable), matching the current dependency. No Cosmos runtime change was needed.
- Added the two missing blank Javadoc lines after CqlMapper example blocks reported by the repository's documentation checks.
- Updated the existing empty-map assertion in `CassandraExecutorBaseStatementTest.test_verify2_insertEntityMapSubclassWritesItsEntries` to the exact current message, `entity map must not be empty`. Inspection of the local abacus-query 4.9.7 source confirms that `AbstractQueryBuilder.parseInsertEntity` now explicitly rejects an empty map with this message before checking keys; 4.9.6 delegated that rejection to the key checker. The executor's implementation is untouched, and the test retains its exact exception type/message and all successful insert assertions.

## File coverage

Reviewed all implementation paths and Javadoc contracts in the following production files, including nested and anonymous types. Reviewed concurrency, resource ownership, mapping/default values, container conversion, query construction, validation timing, and avoidable allocations where applicable. This is a source review, not a claim that every documentation example was executed against a database.

Paths below are relative to `abacus-da-all/src/main/java/com/landawn/abacus/da/`.

| File | Coverage and result |
| --- | --- |
| `cassandra/AsyncCassandraExecutor.java` | All async delegates, result paging, future conversion and memoized callbacks; no change |
| `cassandra/AsyncCassandraExecutorBase.java` | All overloads and async facades, argument validation, statement construction and stream ownership; no change |
| `cassandra/CassandraExecutor.java` | Constructors/pools, preparation/binding, scalar/container/bean mapping, batches, streaming, UDTCodec, StringCodec and StatementSettings; scalar mapping fix and Javadoc clarification |
| `cassandra/CassandraExecutorBase.java` | All synchronous overloads, CRUD/CQL construction, query results, collection materialization and statement delegation; no change |
| `cassandra/CqlBuilder.java` | All builder operations, parameter/literal rendering, entity metadata, aliases, unsupported operations, nested Dsl and anonymous builders; documentation corrections |
| `cassandra/CqlMapper.java` | XML loading/saving, identifier and attribute handling, mapper lifecycle, equality/copy semantics and documentation; example spacing correction |
| `cassandra/ParsedCql.java` | Full parser and cache paths, comments/quoted strings, markers, TokenReader and BracketMask; no change |
| `cassandra/ResultSets.java` | Async-page adaptation, PagedResultSet and its iterator, metadata and page-fetch behavior; no change |
| `cassandra/package-info.java` | Reviewed; no change |
| `cassandra/v3/AsyncCassandraExecutor.java` | All async delegates, driver-future adaptation and memoized callbacks; no change |
| `cassandra/v3/CassandraExecutor.java` | All driver-specific equivalents, temporal/container conversion, UDTCodec, StringCodec and StatementSettings; scalar mapping fix and linear UDT metadata traversal |
| `cassandra/v3/package-info.java` | Reviewed; no change |
| `azure/CosmosContainerExecutor.java` | All CRUD/query/count/paging delegates and SQL-condition rewrite helpers, including quoted text, brackets, aliases and nested/subquery scopes; documentation corrections |
| `azure/package-info.java` | Reviewed; no change |

## Regression tests and validation

Fifteen new test invocations cover the fixes:

- `com.landawn.abacus.da.cassandra.CassandraScalarNullMappingTest` and `com.landawn.abacus.da.cassandra.v3.CassandraScalarNullMappingTest`: six Optional-like target types in each driver version, alternating assignable values and NULLs across synchronous/asynchronous list and stream entry points.
- `com.landawn.abacus.da.cassandra.CassandraRowMappingTest` and `com.landawn.abacus.da.cassandra.v3.CassandraRowMappingTest`: one new test each for String/Long/NULL values in a single scalar result sequence.
- `com.landawn.abacus.da.cassandra.v3.CassandraUdtFieldTraversalTest`: one new test covering ignored-field alignment, scalar/list/map time conversion, raw Long preservation, and one metadata iterator after property resolution. The iterator assertion checks traversal complexity without a timing-sensitive performance loop.

Requested selector: `-Dtest=CassandraScalarNullMappingTest,CassandraRowMappingTest,CassandraUdtFieldTraversalTest,CassandraUdtTimeRegressionTest`. It intentionally selects both driver packages where names are shared. The primary agent's coordinated regression run recorded 195 tests across these seven suites, with zero failures, errors or skips. That earlier aggregate run had an unrelated BigQuery test error, handled by the primary agent; it is not reported here as an aggregate build pass.

Scoped `git diff --check` passed. Maven compilation, complete unit-suite execution and final Javadoc validation are coordinated by the primary agent, whose aggregate report records final results. This reviewer did not start a competing Maven build.

The full-suite run subsequently identified the stale dependency-message assertion described above. Requested follow-up selector: `-Dtest=CassandraExecutorBaseStatementTest`. Its rerun is coordinated by the primary agent.

## Limits

- No live Cassandra or Cosmos service/emulator integration test was run by this reviewer. Mapping tests use driver objects and mocked result values; cloud concurrency behavior in the documentation is grounded in the current SDK's official API contract.
- The UDT optimization removes repeated traversal and iterator allocation by inspection and a deterministic interaction assertion. No wall-clock or heap-size improvement is claimed without a benchmark.
- Source review and regression tests do not establish the absence of all defects, especially in server-dependent CQL/SQL semantics and unsupported concurrent use.
