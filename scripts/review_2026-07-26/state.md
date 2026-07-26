# Review round 2026-07-26 — state / ledger

- **Task:** full line-by-line review of all classes under `abacus-da-all/src/main/java`
  (the repo-root `./src/main/java` in the user's prompt) for bug fixes, javadoc/comment
  improvements, and correctness of every Javadoc sample.
- **HEAD:** `7b33836`, working tree CLEAN at start (no uncommitted rewrite this round —
  fresh-eyes re-review of the mature tree).
- **Deps:** abacus-common 7.8.8, abacus-query 4.8.9 (unchanged).
- **Baseline before any edit:** `mvn -o -pl abacus-da-all compile` = exit 0;
  `mvn -o -pl abacus-da-all test` = **5524 tests, 0 Failures, 0 Errors, 128 Skipped**
  (live services up on this machine, so more tests ran than the 07-25 round's 3592).
- **Process:** 21 read-only Explore agents over the same 21-slice split as 07-25
  (see `scripts/review_2026-07-25/state.md` for the slice table); main agent = SOLE editor,
  every finding re-verified by probe/javap/code-read before editing.
- **Briefing:** `scripts/review_2026-07-26/BRIEFING.md` (mandates the 07-25 DO-NOT-RE-FLAG list).
- **Probe classpath:** `CP="abacus-da-all/target/classes;$(cat /tmp/review-0726/cp.txt)"`
  (freshly generated this round; Git-Bash `/tmp` paths need `cygpath -w` for `java -cp`).

## Findings (14 reported; 11 fixed, 1 rejected, 2 notes)

All fixed findings were independently re-verified by the main agent:

1. **[WS2][P2] v1 `AsyncDynamoDBExecutor` (3 sites: putItem/updateItem/deleteItem request samples)** —
   `ex.getCause() instanceof ConditionalCheckFailedException` inside `thenRunAsync` BiConsumers is
   ALWAYS FALSE: `ContinuableFuture` unwraps ExecutionException, so `ex` IS the SDK exception.
   Probe-verified (ProbeCF: `ex instanceof` = true, `ex.getCause() instanceof` = false).
   Fixed: `ex instanceof ConditionalCheckFailedException`.
   NOTE: v2 async (`aws/dynamodb/v2/AsyncDynamoDBExecutor`, 6 sites) is the OPPOSITE — SDK v2 futures
   complete with `CompletionException(cause)`, so `ex.getCause()` is correct there (probe-verified
   by slice D). Do NOT "align" the two.

2. **[WS2][P2] `mongodb/reactivestreams/MongoCollectionMapper.deleteOne(Bson, DeleteOptions)` sample** —
   `Collation.builder().strength(2)` does not compile: `strength(int)` removed in driver 4.0
   (javap on mongodb-driver-core 5.8.0: only `collationStrength(CollationStrength)`).
   Fixed: `collationStrength(CollationStrength.SECONDARY)`; compile-probed.

3. **[WS2][P2] `cassandra/v3/CassandraExecutor.execute(Statement)` sample** —
   fluent chain `.bind(...).setConsistencyLevel(...).setReadTimeoutMillis(...)` assigned to
   `BoundStatement` does not compile: driver 3.11.5 setters return `Statement`, no covariant
   overrides on BoundStatement (javap-verified). Fixed: split the chain; compile-probed.

4. **[WS2][P2] `cassandra/AsyncCassandraExecutorBase.execute(String, Map)` sample** —
   claimed an empty map for a parameterless query "returns the result set"; actually the map is a
   1-element vararg and `prepareStatement` throws IAE "Too many parameters for parameterless query"
   synchronously (code-read verified, `CassandraExecutor.java:1673-1674`; v3 identical).
   Fixed: sample now documents the IAE as an Edge case.

5. **[WS2][P2] `gcp/BigQueryExecutor` (4 sites: query/list/stream/execute custom-SQL methods)** —
   `@throws NullPointerException if query is null` contradicted behavior: null query →
   Guava `Preconditions.checkArgument` IAE "Provided query is null or empty" from
   `QueryJobConfiguration.newBuilder` (javap-verified, google-cloud-bigquery 2.67.0).
   Fixed: `@throws IllegalArgumentException if query is null or empty (rejected by the BigQuery client layer)`.

6. **[WS3][P3] `hbase/AnyPut.create(entity, selectPropNames, namingPolicy)`** —
   invalid property name threw a message-less IAE (single-arg `checkArgNotNull`).
   Fixed: descriptive IAE naming the class and property (house style, cf. `HBaseExecutor:413`).
   **This is the round's only behavior change** (exception message text only).

7. **[WS2][P3] v2 `DynamoDBExecutor.asItem`/`asUpdateItem`** — missing `@throws ClassCastException`
   that v1 siblings and v2's own `asKey` document (probe-verified CCE reachable). Fixed: tag added.

8. **[WS2][P3] `mongodb/reactivestreams/MongoCollectionExecutor.list(...)` internal comment** —
   quoted a nonexistent "Unsupported target type" message. Fixed: reworded to the real behavior
   (multi-field doc → IAE from `singleValuePropName`; single-field doc → silent extraction).

9. **[WS2][P3] `mongodb/AsyncMongoCollectionExecutor`** — (a) phantom `@throws ClassCastException`
   on `queryForDate(String, Bson, Class)`: `N.convert` failures surface as IAE, never CCE
   (probe-verified); tag deleted. (b) two `mapReduce` `@throws` tags missed "or empty"
   (`checkArgNotEmpty` guards); fixed to mirror the sync sibling wording.

10. **[WS2][P3] `cassandra/CassandraExecutor.StatementSettings` class doc** — credited Lombok with
    the two hand-written constructors. Fixed: builder/accessors from Lombok, ctors declared below.

11. **[WS2][P3] `cassandra/CassandraExecutorBase:84`** — "Preferred statement cache" typo →
    "Prepared statement cache".

12. **[WS2][P3] `cassandra/CqlBuilder`** — (a) `batchInsert` `@throws` misattributed row validation
    to `into(String)`; it fires eagerly in `batchInsert` via parent `toInsertPropsList`
    (code-read + slice probe). (b) `deleteFrom(String)` doc's "if an entity class is associated"
    qualifier was wrong — naming-policy conversion is unconditional (probe: `expiresAt`→`expires_at`
    with no entity class). Both fixed.

## Rejected finding (with reason)

- **`AsyncCassandraExecutorBase` samples use undeclared type `RS`** (slice O, P3): REJECTED.
  The base spans driver v3 and v4, whose `ResultSet` types differ; the type-variable shorthand is
  the only correct option in this generic base, and the samples are conceptual sketches anyway
  (`executor`, `log` are likewise undeclared).

## Per-slice verdicts

CLEAN (no findings): A (DDB v1 sync), D (DDB v2 async — exception samples probe-verified CORRECT,
opposite of v1), E (Mongo sync executor), H (Mongo sync mapper+base), J (HBase sync executor),
K (HBase async+Scan), M (HBase other mutations), P-minus-one (v3 async + package-info),
R (CqlMapper+ParsedCql, 95 probe assertions green), T (Cosmos), U (Neo4j+stubs).
Findings from: B, C, F, G, I, L, N, O, P, Q, S (all listed above).

## Verification after fixes

- `mvn -o -pl abacus-da-all compile` = exit 0.
- `git diff`: 12 files, 34+/22-; the ONLY non-comment change is the AnyPut exception message (#6);
  everything else is javadoc/comment.
- Fixed samples compile-probed: v3 BoundStatement chain (driver 3.11.5 jar) ✓;
  Collation `collationStrength` (driver-core 5.8.0 jar) ✓.
- Full test suite after all edits: **3592 tests, 0 Failures, 0 Errors — BUILD SUCCESS**
  (absolute counts shift with which live services are reachable; green before and after,
  including all 74 AnyPutTest tests covering the one behavior change).
