# Testing

Use JDK 21 and Maven. Test sources are in `abacus-da-all/src/test/java`.

## Offline unit tests

From the repository root:

```text
mvn -pl abacus-da-all clean test jacoco:report
```

Surefire discovers `**/*Test.java` directly. Tests require no database, emulator,
cloud credentials, or network service. Failures fail the build. The optional
`AbacusDATestSuite` is an IDE entry point; Maven does not execute it again.

Use the fully qualified class name when driver versions have the same test name:

```text
mvn -pl abacus-da-all "-Dtest=com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBMappingTest" test
```

Reports are in `abacus-da-all/target/surefire-reports`. Coverage is in
`abacus-da-all/target/site/jacoco/index.html`. JaCoCo's agent argument is passed to
the forked JVM along with the existing Java module-access options. Start with
`clean` when comparing coverage so earlier unit or integration runs are excluded.

## Integration tests

Live-service tests end in `IT` and run through Failsafe only with the
`integration-tests` profile. Prepare the required local fixture, then select it:

```text
mvn -pl abacus-da-all -Pintegration-tests "-Dit.test=DynamoDBExecutorIT" verify -Dgpg.skip=true
```

To compile and run only the selected integration tests, without packaging or
rerunning unit tests:

```text
mvn -pl abacus-da-all -Pintegration-tests "-Dit.test=DynamoDBExecutorIT,CosmosContainerExecutorIT,MongoGroupingPathIT,MongoDBIT" test-compile failsafe:integration-test failsafe:verify
```

Omitting `-Dit.test` from the profile's `verify` command selects all `*IT`
classes. Include Failsafe's `verify` goal when invoking it directly: that goal
makes integration failures fail the build. Reports are in
`abacus-da-all/target/failsafe-reports`. A selector that matches no integration
tests fails instead of reporting a successful empty run.

| Suite | Required fixture and ownership |
| --- | --- |
| DynamoDBExecutorIT | DynamoDB Local at localhost:8000, dummy credentials, and unused TestTable, UserTable, RangeTable names. These names exercise entity annotations. The suite refuses existing tables and deletes only tables it created. |
| CosmosContainerExecutorIT | Cosmos emulator at https://localhost:8081 with the public emulator key. A unique database is created and deleted per run. The certificate-validation property is restored. The existing localhost SQL-query limitation produces explicit skipped cases; other setup failures fail. |
| MongoDBIT | MongoDB at localhost:27017. A unique database is created for the run, then dropped and the client closed. |
| MongoGroupingPathIT, sync and reactive | MongoDB with database-level $documents support. Captured pipelines execute against literal documents without creating or changing collections. |
| CassandraExecutorIT, driver v3 and v4 | Local Cassandra at port 9042 with the dedicated simplex schema described in the source. These legacy round trips modify that schema; use a disposable test instance. Sessions and clusters close in JUnit teardown. |
| BigQueryExecutorIT | The test-project BigQuery emulator at localhost:9050. A unique dataset is created and deleted per run. |
| HBaseExecutorIT | Local HBase and ZooKeeper at localhost:2181, with an unused account table name. The suite refuses an existing table and removes only its own table. |
| Neo4jJdbcIT, Neo4jOGMIT | Optional JDBC/OGM drivers and a disposable Neo4j fixture matching the source configuration. These legacy examples write graph data. Missing optional drivers or supported availability probes report skips. |

## Test organization

- Mirror the production package, including `aws.dynamodb.v2` and
  `cassandra.v3`. Keep SDK-specific assertions independent even when their
  source looks similar.
- Keep pure conversion, parameter validation, SQL/CQL construction, and mocked
  delegation in `*Test`. Put actual server round trips in `*IT`. A unit fixture
  must not initialize a live client or session as a side effect.
- Split large classes by responsibility. BigQuery has query, mutation,
  parameter, mapping, structured mapping, generic mapping, matrix, and builder
  lifecycle suites. DynamoDB has executor, mapper, mapping, and filter suites
  for each SDK. CQL builder tests are grouped by statement family. MongoDB executor
  suites separate mutation, query, scalar-query, streaming, and mapping contracts;
  the async facade has corresponding delegation suites. Cassandra base executor
  tests separate statement construction, synchronous operations, and async facades.
- Within a class, place fields and lifecycle methods first, related test
  families together, and fixture/helper types last. Keep success, empty/null,
  invalid input, and regression cases near the operation they exercise. Source
  order is for reading; do not introduce execution-order dependencies.
- Prefer descriptive behavior names for new or rewritten tests. Use named
  parameterized cases when setup, operation, and assertions are the same and
  only input/output data differs. Preserve overload-specific contracts and
  regression explanations when consolidating cases.
- Use exact value, structure, exception, and interaction assertions. Console
  output or a size check alone does not establish the expected contract.
  Keep performance loops out of the default correctness suite.
- Reuse `MockitoTestBase` where annotation-based mocks are initialized manually;
  it closes the returned Mockito resource after each test. Existing
  MockitoExtension fixtures may continue to use the extension. Keep shared
  helpers limited to the tests that actually use them. Keep reusable mapping data
  and assertions in fixture/support classes without test methods, so tests do not
  depend on another executable test class.
- Async delegation tests must execute submitted work. Use a real
  `AsyncExecutor(Runnable::run)` for deterministic delegation checks, then assert
  arguments and result identity/value at the synchronous boundary. Keep explicit
  scheduling and future-failure checks separate. A mocked task runner returning a
  prebuilt future does not verify the delegated operation.
- Use fixed timestamps and bounded completion signals. Pagination cancellation
  checks use `ObservedPageFuture` to await the page-processing stage after the
  public result completes; do not replace that signal with a sleep.

The [initial cleanup report](report/unit-test-cleanup.md) records the earlier suite
migration. The [standardization report](report/unit-test-standardization.md) and
[consolidation ledger](report/unit-test-standardization.tsv) record the current
cleanup, retained test owners, and coverage comparison.
