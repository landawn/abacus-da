# Unit-test cleanup

Implemented against the working-tree snapshot taken before this cleanup. Existing
staged production changes were preserved; this cleanup changes test sources,
Maven test configuration, and test documentation.

## Inventory and consolidation

| Measure | Before | After |
| --- | ---: | ---: |
| Java test-source files, including fixtures | 115 | 146 |
| Annotated test methods, including qualified annotations | 4,607 | 4,402 |
| Default offline unit classes | Explicit baseline selection | 104 |
| Offline unit executions in the validation run | 3,978 | 4,031 |
| Explicit integration classes | Mixed naming/discovery | 11 |
| Integration test methods | Mixed with unit methods | 380 |

The increase in file count comes from smaller suites and extracted helpers.
All 4,607 original annotated methods resolve to a retained method or parameterized
case. The net reduction of 205 methods consists of:

- 197 duplicate methods removed after identifying their surviving assertion.
- 12 equivalent data cases represented by four parameterized methods, reducing
  the method count by eight. An additional excess-parameter case strengthens the
  Cosmos query validation matrix.

The [consolidation ledger](unit-test-consolidation.tsv) contains the original
method, final owner, and reason for all 197 removals and all 12 parameterized
cases. Paths in that ledger are relative to
`abacus-da-all/src/test/java/com/landawn/abacus/da`.

SDK v1/v2 and Cassandra driver v3/v4 tests remain independent. Similar-looking
assertions against different implementations were retained. Three fully qualified
`@org.junit.jupiter.api.Test` null-validation methods were extracted from the
legacy DynamoDB examples into mock-based unit suites.

## Organization and behavior

- Surefire discovers `*Test` directly and fails on failures. Failsafe discovers
  `*IT` only through the integration profile. Both use the same runner version
  and preserve the JaCoCo agent arguments. The IDE suite uses the same unit-name
  convention without a tag filter or duplicate Maven execution.
- BigQuery tests are split into executor, query, mutation, parameter, mapping,
  structured mapping, generic mapping, mapping matrix, and builder lifecycle
  suites. DynamoDB is split into executor, mapper, mapping, and filters for each
  SDK. CQL builder suites are split by statement family and validation.
- Related unit methods are grouped in source by API responsibility, with fixture
  setup first and helper types after assertions. No execution-order annotations
  were added.
- Pure codecs, statement settings, parsed CQL, CQL mapper, and Mongo pipeline-shape
  checks no longer require live-service fixtures. The unique expression test from
  HelloTest belongs to the CQL update suite.
- The CQL profiler loop is replaced by a small repeatability contract asserting
  exact queries and parameters. The print-only delete example now checks the
  different row-delete and column-delete SQL plus their bound parameters.
- HBase cell and time-range checks assert values as well as shape. Mongo nested-ID
  fixtures use fixed timestamps. Cosmos SQL-rewrite and Cassandra codec cases use
  parameterized inputs with readable case names.
- Mockito annotation resources close after every test. Async pagination checks
  await completion with a timeout instead of sleeping. Integration fixtures own
  their created resources; Cassandra sessions close in JUnit teardown and the
  Cosmos certificate property is restored.

## Main suite moves

| Original | Current owner |
| --- | --- |
| `gcp/BigQueryExecutorTest2.java` | `gcp/BigQueryExecutorIT.java` |
| `azure/CosmosContainerExecutor2Test.java` | `azure/CosmosContainerExecutorIT.java` |
| `aws/dynamodb/DynamoDBExecutorV2_2Test.java` | `aws/dynamodb/v2/DynamoDBExecutorIT.java` |
| `aws/dynamodb/DynamoDBExecutorV2Test.java` | `aws/dynamodb/v2/DynamoDBExecutorTest.java` |
| `aws/dynamodb/AsyncDynamoDBExecutorV2Test.java` | `aws/dynamodb/v2/AsyncDynamoDBExecutorTest.java` |
| `aws/dynamodb/DynamoDBExecutorTest.java` | `aws/dynamodb/DynamoDBLegacyExamples.java` |
| `aws/dynamodb/DynamoDBExecutor01Test.java` | `aws/dynamodb/DynamoDBExecutorTest.java` |
| `cassandra/CassandraExecutorTest.java` | `cassandra/CassandraExecutorIT.java` |
| `cassandra/v3/CassandraExecutorTest.java` | `cassandra/v3/CassandraExecutorIT.java` |
| `cassandra/CassandraExecutor01Test.java` | `cassandra/CassandraExecutorTest.java` |
| `mongodb/MongoDBExecutorTest.java` | `mongodb/MongoDBIT.java` |
| `hbase/HBaseExecutorTest.java` | `hbase/HBaseExecutorIT.java` |
| `neo4j/Neo4jOGMTest.java` | `neo4j/Neo4jOGMIT.java` |
| `neo4j/Neo4jJdbcTest.java` | `neo4j/Neo4jJdbcIT.java` |

The former Cassandra and DynamoDB unsuffixed unit file names now hold the
mock-based suites previously named `01Test`. The legacy DynamoDB example class
has no remaining annotated tests. Shared CQL checks now belong to CqlMapperTest
and ParsedCqlTest; sync and reactive Mongo live grouping checks each have a
MongoGroupingPathIT owner.

## Validation

JDK 21, Maven 3.9.16, JUnit 6.1.3, Surefire/Failsafe 3.6.0, JaCoCo 0.8.15.

| Check | Result |
| --- | --- |
| Clean default unit run | 4,031 tests, zero failures/errors/skips across 104 classes |
| Selected local integration run | 150 tests: 131 passed, 19 skipped, zero failures/errors |
| Async pagination with common-pool parallelism set to 1 | Passed, including cancellation and timeout modes |
| Test migration inventory | All 4,607 original methods accounted for |
| Compilation | All 146 test-source files, including every integration suite |
| Working-tree whitespace check | `git diff --check` passed |

Commands used for the final full runs:

```text
mvn -o -pl abacus-da-all clean test jacoco:report
mvn -pl abacus-da-all -Pintegration-tests "-Dit.test=DynamoDBExecutorIT,CosmosContainerExecutorIT,MongoGroupingPathIT,MongoDBIT" test-compile failsafe:integration-test failsafe:verify
```

The selected integration run covers DynamoDB Local, Cosmos, MongoDB, and both
sync/reactive Mongo grouping suites. The 19 skipped cases are Cosmos SQL-query
operations explicitly unsupported by the configured localhost emulator. They are
not counted as passes. BigQuery, HBase, Neo4j, and the two Cassandra live suites
were compiled but not executed in the final validation. Cassandra requires its
dedicated simplex fixture; the other local services or optional drivers were not
available for this run.

## Coverage preservation

Coverage was measured against the same production sources. The baseline compiled
the original test sources in an isolated temporary Maven project, using an
explicit offline selection and excluding live/mixed-service classes and the
profiler method. The final measurement used a clean default unit run. Integration
execution followed that report and is not included in these numbers.

| Metric | Original offline selection | Clean default unit suite |
| --- | ---: | ---: |
| Lines | 10,289 / 11,025 (93.32%) | 10,342 / 11,025 (93.80%) |
| Branches | 5,578 / 6,738 (82.78%) | 5,617 / 6,738 (83.36%) |
| Instructions | 54,810 / 59,088 (92.76%) | 55,090 / 59,088 (93.23%) |
| Methods | 2,805 / 2,884 (97.26%) | 2,811 / 2,884 (97.47%) |

No production class lost covered lines, branches, instructions, or methods. A
source-line comparison also found no reduction in covered instructions or
branches on any previously covered line. The gain includes local behavior
extracted from mixed integration fixtures, so the before/after execution counts
are different cohorts. Coverage is supporting evidence; the consolidation ledger
records why each removed assertion is still represented.

Detailed logs, original snapshots, the complete migration map, and the coverage
comparison are retained locally under ignored `target/test-cleanup`. Current
JUnit XML and HTML coverage reports use Maven's standard module target paths.
See [TESTING.md](../TESTING.md) for normal commands, fixture requirements, and
conventions for future tests.
