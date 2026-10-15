# Unit-test coverage improvement — 2026-10-15

The offline suite now passes **4,200 test executions across 126 classes**, with zero failures, errors, or skips. This adds **169 executions in 22 test classes** and one shared Cassandra 3 fixture. Production code, dependency versions, coverage exclusions, and test-discovery configuration were not changed.

Coverage improved substantially, but **100% line and branch coverage was not reached**. One compiler-generated method remains uncovered.

| Metric | Original tests | Final tests | Remaining missed |
| --- | --- | --- | --- |
| Lines | 10,436 / 11,129 (93.77%) | 10,853 / 11,129 (97.52%) | 276 |
| Branches | 5,688 / 6,828 (83.30%) | 5,998 / 6,828 (87.84%) | 830 |
| Instructions | 55,712 / 59,766 (93.22%) | 58,063 / 59,766 (97.15%) | 1,703 |
| Methods | 2,836 / 2,911 (97.42%) | 2,910 / 2,911 (99.97%) | 1 |
| Classes | 90 / 91 (98.90%) | 91 / 91 (100.00%) | 0 |

The additions cover 417 previously uncovered source lines, 310 additional branch outcomes, and 74 additional methods. Method coverage includes generated lambdas; class coverage includes the 91 classes with executable code analyzed by JaCoCo.

## Measurement and validation

- Production revision: `b9db16d5df776b2448b7af7b91e9516d4f4e6ed1`.
- JDK 21, Maven 3.9.16, JaCoCo 0.8.15.
- Baseline: all 4,031 original unit executions, excluding newly added test classes, with a separate JaCoCo execution-data file. Zero failures, errors, or skips.
- Final: a clean build of every default unit test. Zero failures, errors, or skips. The final run completed in 1 minute 47 seconds.
- Baseline and final JaCoCo class IDs match for every class entry, so both measurements use identical production bytecode.
- Percentages above use JaCoCo's report-level XML counters. Summing per-class CSV line counters can double-count source lines shared with nested classes and differs slightly from the report-level total.
- All tests run offline. No database, emulator, or live cloud service was used.

Reproduce the final run from the repository root:

```text
mvn -o -pl abacus-da-all clean test jacoco:report
```

Offline mode assumes the Maven dependencies are already cached. The HTML report is at [abacus-da-all/target/site/jacoco/index.html](../abacus-da-all/target/site/jacoco/index.html), with test reports in `abacus-da-all/target/surefire-reports`.

Local measurement artifacts are retained under `target/coverage-improvement`: `baseline-current.log`, `baseline-current.xml`, `baseline-current.exec`, `full-verified.log`, `verified.xml`, and `verified-summary.json`.

## Added behavioral coverage

| Area | New executions | Contracts checked |
| --- | ---: | --- |
| Cassandra and CQL | 84 | Driver 3 batch values, order, settings and validation; parameter conversion and ownership; nested rows for both driver versions; selected-property queries; UDT binary compatibility and metadata resolution; asynchronous cursor reuse, deferred mapping and failure replay; parsed comments and literals; raw timestamp comments; builder state after rejected operations; mapper I/O failures and stream ownership. |
| HBase | 39 | Driver I/O causes and table closure; connection/admin shutdown failures; mapper coprocessor results and callbacks; object row-key encoding; wrapper boundaries; versioned cell write/read round trips. |
| DynamoDB | 17 | Bulk item/update conversion and naming policies; SDK 1 asynchronous entity results/failures; SDK 2 client futures; mapper table defaults while preserving complete request options. |
| Cosmos | 12 | Read-body results; 404 absence versus other service errors; quoted identifiers, brackets, literals, and null predicates. |
| MongoDB | 10 | Ordered array/list/map row conversion; primitive defaults; mixed scalar row validation; first-result conversion; legacy BSON updates, immutable identifiers, null field names, and sync/reactive parity. |
| BigQuery | 7 | Driver job failures; empty result pages and DML row counts; SQL null versus primitive defaults; scalar conversion rejection for multi-column rows. |

Mocks replace SDK clients, sessions, cursors, or driver metadata. Assertions cover exact payloads, query text, converted values, exception identity/causes, callback arguments, and resource closure. Shared fixtures and parameterized cases keep related inputs together without adding execution-order dependencies.

## Remaining gaps

[code-coverage-gaps.tsv](code-coverage-gaps.tsv) lists every source line with missed instructions or branches, including line numbers and both covered/missed counters. It records all 276 completely uncovered lines and all 830 missed branch outcomes, including misses on partially covered lines. This is a dated snapshot; regenerate the JaCoCo report after changing production code.

The largest remaining source-file gaps include nested classes:

| Source below com/landawn/abacus/da | Uncovered lines | Missed branch outcomes |
| --- | ---: | ---: |
| cassandra/v3/CassandraExecutor.java | 37 | 84 |
| aws/dynamodb/DynamoDBExecutor.java | 34 | 105 |
| hbase/HBaseExecutor.java | 29 | 57 |
| cassandra/CqlBuilder.java | 26 | 80 |
| gcp/BigQueryExecutor.java | 26 | 54 |
| cassandra/CassandraExecutor.java | 25 | 80 |
| aws/dynamodb/v2/DynamoDBExecutor.java | 22 | 102 |
| azure/CosmosContainerExecutor.java | 15 | 34 |
| cassandra/CassandraExecutorBase.java | 13 | 30 |
| aws/dynamodb/v2/AsyncDynamoDBExecutor.java | 10 | 17 |

Some gaps need additional tests, including Cassandra TIME/UDT variants, HBase family/version mapping shapes, DynamoDB conversion and validation combinations, and BigQuery alias/structured conversion paths. Other misses are defensive or depend on different SDK/runtime behavior. Examples include:

- The later empty-parameter guard in both Cassandra executors follows an earlier empty-parameter return; it cannot be reached through that method's normal input flow.
- HBase time-range wrappers catch a checked IOException that the installed driver's range-validation implementation does not produce for invalid bounds.
- BigQuery schema-reflection fallbacks and Cassandra 3 UDT registry-access fallbacks depend on different library layouts or reflection access rules.
- Unsupported naming-policy defaults are often preceded by constructor validation that rejects those policies.

These paths were neither excluded from measurement nor reached by corrupting private state. The remaining reachable cases and the compatibility fallbacks require separate work; they should not all be labeled dead code.

## Discovered converter mismatch

The only uncovered method is `CassandraExecutor.lambda$static$0`, registered for the driver 4 `Row` interface at [CassandraExecutor.java:275](../abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CassandraExecutor.java#L275).

The installed `com.landawn.abacus:abacus-common:8.2.0` implementation of `Converters.getConverter` searches an object's concrete class and registered superclasses, explicitly excluding interfaces. A concrete Row implementation therefore cannot dispatch to this interface registration through `N.convert`. An exploratory test confirmed that a Row containing a name did not populate the target bean through that API. The supported `CassandraExecutor.toEntity`, `toMap`, and `toList` paths are covered by the added tests.

Reaching this last method legitimately requires resolving the production/dependency converter contract. The exploratory failure and inspected library source are retained in `target/coverage-improvement/final-methods.log` and `converter-dispatch-evidence.txt`. No passing test was added that treats the incorrect conversion result as the desired behavior.

## Additional parser finding

Exploratory testing also found that passing a line-comment expression such as `SqlExpression.of("1 // USING fake\r\n")` as an inserted value loses the newline before CqlBuilder places trailing clauses. The resulting comment can swallow those clauses. This requires investigation of the expression normalization contract; it was not changed in this test-only work. The failure is recorded in `target/coverage-improvement/cql-first.log`.

The passing comment tests cover SQL-style comments normalized by SqlExpression, quoted/dollar-quoted literals, and CQL line/block comments passed through the raw timestamp option, whose documented contract preserves the supplied text.
