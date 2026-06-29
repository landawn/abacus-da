# Cross-class consistency notes (running log)

Observations jotted as noticed; consolidated into SUMMARY.txt at the end.

## Intentional mirroring (NOT to be flagged as duplication)
- HBase Any* classes mirror the HBase client class hierarchy method-for-method.
- HBaseExecutor ↔ AsyncHBaseExecutor.
- CassandraExecutor (driver3) ↔ v3/CassandraExecutor (driver4); same for the async pair; both delegate to the abstract *Base.
- DynamoDBExecutor (SDK v1) ↔ v2/DynamoDBExecutor (SDK v2); same for async pair.
- MongoCollectionExecutor ↔ reactivestreams/MongoCollectionExecutor; AsyncMongoCollectionExecutor is the future-based async sync variant; MongoCollectionMapper ↔ reactivestreams/MongoCollectionMapper.
- CqlMapper↔SqlMapper, ParsedCql↔ParsedSql, CqlBuilder↔SQLBuilder (abacus-core equivalents). Per memory, these are deliberately locked to mirror the SQL siblings.

## Things to check for cross-class consistency (to be confirmed by agents)
- Optional vs null return conventions across executors (find/get single-row).
- Naming: `gett` vs `get`, `query` vs `find` vs `list` vs `stream` conventions across executors.
- Async executors: return type (ContinuableFuture vs CompletableFuture vs reactive Publisher) consistency.
- `array2Props`-style varargs/Object[] property-bag helpers — where duplicated.
- AutoCloseable: which executors implement it (HBaseExecutor, DynamoDBExecutor(both), CassandraExecutorBase do; check Cosmos/BigQuery/Neo4j/Mongo).

## CORRECTION — Cassandra driver-version mapping (verified from imports)
- `cassandra/CassandraExecutor` + `cassandra/AsyncCassandraExecutor` import `com.datastax.oss.driver.api.core.*` → DataStax driver **4.x** (modern, NOT deprecated).
- `cassandra/v3/CassandraExecutor` + `cassandra/v3/AsyncCassandraExecutor` import `com.datastax.driver.core.*` → DataStax driver **3.x** (`@Deprecated` at class level, points to the non-v3 class).
- So the "v3" package name refers to driver-major-version 3 (the legacy one). My initial state-file note had these swapped; corrected here.

## Pending findings to consolidate
(agents reported; orchestrator verifying cited lines before promoting to reports)
