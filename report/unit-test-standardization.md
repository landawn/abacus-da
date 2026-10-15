# Unit-test standardization

Implemented the plan below against the clean baseline at `e3a4191` on 2026-10-15.
The final clean offline run passes **4,105 tests across 142 classes**, with zero
failures, errors, or skips. Coverage is unchanged at every measured production
class and source line.

## Plan executed

1. Measure a clean unit-test and coverage baseline and inventory the test methods.
2. Consolidate MongoDB async wrapper checks with tests that execute the submitted
   work and verify the delegated operation.
3. Extract shared mapping fixtures and group large executor suites by responsibility.
4. Parameterize repeated Cassandra naming-policy cases, remove confirmed duplicates,
   standardize source layout, and strengthen assertions.
5. Run focused tests and the clean full suite, reconcile removed/moved methods,
   and compare aggregate, class, and source-line coverage.

## Consolidation and improvements

- Removed 99 redundant async MongoDB tests that returned prebuilt futures without
  executing their callables. Their retained counterparts now use a real
  `AsyncExecutor(Runnable::run)`, check exact delegated arguments and result
  identity/value, and reject extra synchronous calls. Filter and sort inputs are
  distinct so swapped arguments cannot pass accidentally.
- Removed five further duplicates: HBase put size/family checks, ParsedCql null
  input, DynamoDB SDK v2 null response, and two reactive MongoDB collection
  accessor checks. SDK and Cassandra driver versions retain separate tests.
- Replaced 24 Cassandra naming-policy methods with eight parameterized methods,
  retaining all 24 executions. They assert exact CQL, case-preserving aliases,
  named markers, and ordered parameter values, using multiword property names.
- Expanded eight async get/gett contracts to cover present and absent results.
  Added one deterministic scheduling test proving that the synchronous operation
  waits for the submitted task to run. The existing background-thread failure
  assertion now uses a bounded wait.
- Strengthened 21 synchronous MongoDB driver-result assertions from non-null
  checks to identity checks. The async-view test now checks cached identity and
  the sync back-reference. The retained HBase fluent test checks wrapper identity
  and all three cell values in addition to size and family count.

The [consolidation ledger](unit-test-standardization.tsv) records 754 affected
original methods, their retained owners, and the reason for each change. All
4,484 original annotated methods are accounted for by an unchanged method or a
ledger entry; every retained owner exists. The source-method reduction is
104 redundant methods plus 16 methods consolidated by parameterization, offset
by one new scheduling method. Executions decrease by 104 and increase by nine
for the added read-result cases and scheduling check.

## Organization

- Sync and reactive MongoDB executor suites now separate construction/validation,
  mutation, query, scalar query, streaming, and mapping. Async suites separate
  construction/validation, mutation, query, streaming, and mapping.
- Cassandra base executor tests separate synchronous operations, statement
  construction, and async facade contracts.
- `MongoMappingFixtures` owns decoded BSON data and mapping assertions shared
  across executor, mapper, and live-test sources. Reusable mock setup and helper
  types live in support classes with no test methods.
- Related operations are grouped in source. Fields precede setup, tests precede
  helper methods, historical verification suffixes are removed in the affected
  suites, and rewritten suites use the existing Abacus Eclipse formatter profile.
  No execution-order annotations were introduced.
- [TESTING.md](../TESTING.md) documents the organization and async-test conventions.

| Inventory | Before | After |
| --- | ---: | ---: |
| Java test-source files, including fixtures and integration sources | 169 | 190 |
| Offline unit classes | 126 | 142 |
| Annotated offline unit methods | 4,104 | 3,985 |
| Annotated methods across all test sources | 4,484 | 4,365 |
| Offline unit executions | 4,200 | 4,105 |

The additional files are 16 focused unit suites and five fixture/support classes.

## Validation and coverage

JDK 21, Maven 3.9.16, JUnit 6.1.3, Surefire 3.6.0, JaCoCo 0.8.15.

The clean baseline and final run used:

```text
mvn -o -pl abacus-da-all clean test jacoco:report
```

| Metric | Before | After |
| --- | ---: | ---: |
| Lines | 10,853 / 11,129 (97.52%) | 10,853 / 11,129 (97.52%) |
| Branches | 5,998 / 6,828 (87.84%) | 5,998 / 6,828 (87.84%) |
| Instructions | 58,063 / 59,766 (97.15%) | 58,063 / 59,766 (97.15%) |
| Methods | 2,910 / 2,911 (99.97%) | 2,910 / 2,911 (99.97%) |
| Executable classes | 91 / 91 (100%) | 91 / 91 (100%) |

Aggregate counters come from the JaCoCo XML report totals. Every production
class has unchanged covered line, branch, instruction, and method counts.
Every previously covered source line retains its covered instruction and branch
counts. Production sources, Maven discovery, and coverage configuration were
not modified.

All 190 Java test sources compile, including the live suites whose shared
fixture references changed. Live services were not exercised for this cleanup.
The final declaration-order adjustment in two support classes was followed by
another test compilation. Working-tree whitespace checks pass.

Line and branch coverage remain below 100%; the previously documented gaps in
the [coverage improvement report](code-coverage-improvement.md) remain.

The baseline, final test/coverage logs, source snapshots, syntax inventories,
and detailed coverage comparison are retained locally under ignored
`target/test-standardization`. Standard reports remain under
`abacus-da-all/target/surefire-reports` and
`abacus-da-all/target/site/jacoco`.
