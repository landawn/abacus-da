# MongoDB and HBase executor review — 2026-10-31

## Findings and changes

- Fixed synchronous MongoDB scalar streams retaining the caller's mutable projection collection. Changing or clearing that collection after opening a stream could change the field decoded from subsequent rows, even though the driver projection was already constructed. The scalar mapper now captures the selected field at stream creation.
- Parsed dotted scalar projection paths once per mapper in synchronous list/stream and reactive list/find-first paths. Previously every row split the same path and allocated a new array and list wrapper. Top-level fields and null/missing-field behavior retain their existing semantics.
- Documented the scalar projection snapshot contract and added focused regression tests for collection reuse, collection clearing, synchronous null/missing values, and reactive null/missing values.
- Fixed quadratic allocation when HBase versioned properties use `CopyOnWriteArrayList`: accumulate each property's versions in an `ArrayList`, then append them to the original target once before deferred property assignment. The optimization is restricted to exact `CopyOnWriteArrayList` instances so subclasses retain overridden per-cell `add` behavior. Retaining the factory-created target also preserves its identity and any seeded contents. Updated the mapping Javadoc and commented the unusual buffering/finalization steps.
- Reviewed HBase metadata routing, versioned columns, nested/immutable beans, raw-byte conversion, scanner/table ownership, constructor/close failures, CRUD/mapper delegation, and asynchronous exception timing; no additional confirmed correctness fix was needed.

## File coverage

All implementation paths in these production files were reviewed, including nested classes. Javadoc checks covered class-level contracts, delegate consistency, and the mapping, null handling, resource ownership, and streaming contracts relevant to those paths.

| File under `abacus-da-all/src/main/java/com/landawn/abacus/da/` | Result |
| --- | --- |
| `mongodb/MongoDBBase.java` | Reviewed conversion, generic metadata, codecs, containers, identifiers, collectors and cursor cleanup; no change |
| `mongodb/MongoDB.java` | Reviewed database/collection factories and ownership; no change |
| `mongodb/MongoCollectionExecutor.java` | Fixed scalar stream projection capture and reused parsed paths; Javadocs improved |
| `mongodb/AsyncMongoCollectionExecutor.java` | Reviewed all delegates and cancellation/stream ownership; no change |
| `mongodb/MongoCollectionMapper.java` | Reviewed delegates and distinct aggregation construction; no change |
| `mongodb/package-info.java` | Reviewed; no change |
| `mongodb/reactivestreams/MongoDB.java` | Reviewed factories, codec ownership and conversion bridges; no change |
| `mongodb/reactivestreams/MongoCollectionExecutor.java` | Reused parsed scalar paths; reviewed publishers, mapping and aggregation construction |
| `mongodb/reactivestreams/MongoCollectionMapper.java` | Reviewed delegates and distinct aggregation construction; no change |
| `mongodb/reactivestreams/package-info.java` | Reviewed; no change |
| `hbase/HBaseExecutor.java` | Reviewed all methods and nested `HBaseMapper`; buffered copy-on-write version lists and clarified mapping Javadoc |
| `hbase/AsyncHBaseExecutor.java` | Reviewed all asynchronous delegates and scan ownership; no change |

The HBase `Any*` wrappers and annotation/package files were assigned to another reviewer.

## Regression tests and validation

- `com.landawn.abacus.da.mongodb.MongoScalarProjectionSnapshotTest`: three tests.
- `com.landawn.abacus.da.mongodb.reactivestreams.ReactiveMongoScalarProjectionPathTest`: one test.
- `com.landawn.abacus.da.hbase.HBaseVersionedCollectionAllocationTest`: four tests covering top-level and nested version ordering, duplicate and null payloads, immutable records, custom copy-on-write subclass behavior, and allocation growth.
- The HBase regression was compiled and run with an isolated JDK 21/JUnit classpath, without modifying Maven's shared output. Before the fix, its allocation assertion failed: 256 cells allocated 180,104 bytes and 1,024 cells allocated 2,290,568 bytes. After the fix, all four tests passed, allocating 50,544 and 196,192 bytes respectively. These are per-thread allocated-byte measurements after warmup, not elapsed-time benchmarks. Logs: `target/review-hbase-cow-proof/baseline.log` and `target/review-hbase-cow-proof/after.log`.
- `git diff --check` passed for this work.
- Maven compilation and test execution are coordinated by the primary agent; this reviewer did not start competing Maven builds. The final aggregate report records their results.
- No live MongoDB or HBase integration service was used. The review and new tests cover local conversion and mocked driver boundaries; they are not a proof of every server behavior or concurrent usage pattern.
- Performance improvement is established by moving path splitting out of the per-row loop, without a claimed wall-clock benchmark result.

## Independent cross-review

Reviewed the primary agent's pending `gcp/BigQueryExecutor.java`, `neo4j/Neo4jExecutor.java`, `BigQueryCollectionConstructionTest`, and `Neo4jFilterIterableRegressionTest` changes. Collection finalization preserves requested concrete types and null rules, and materializing a single-use filter iterable before inspection preserves the complete fallback input. Flagged that broadly skipping immutable-property conversion for already assignable scalar values could bypass annotation-driven conversion; the primary agent narrowed that optimization to compatible repeated fields and added a scalar annotation regression.
