# Review round 2026-08-01 — Javadoc-only deep review (state / ledger)

- **Task:** Multi-agent line-by-line Javadoc review of `abacus-da-all/src/main/java`
  (user path `./src/main/java`). Comments only — no executable-code changes.
- **Agents:** 10 Explore subagents (DynamoDB v1, DynamoDB v2, Mongo sync, Mongo reactive,
  Cassandra core, CqlBuilder+v3, HBase core, HBase Any*, BigQuery/Cosmos/Neo4j, stubs).
- **Briefing:** `scripts/review_2026-08-01/BRIEFING.md` + DO-NOT-RE-FLAG from 07-25.

## Findings fixed (main agent re-verified)

### DynamoDB v1
1. `DynamoDBExecutor.batchGetItem(..., returnConsumedCapacity)` / typed overload / Mapper —
   docs claimed capacity monitoring; implementation returns only `.getResponses()`.
2. `AsyncDynamoDBExecutor` matching overloads — same (prose contradicted correct examples).

### DynamoDB v2
3. `AsyncDynamoDBExecutor.Mapper` examples used non-API `createKey("user123")` / private
   `createKey(oldUser)` → replaced with public `asKey(...)`.

### MongoDB
4. Async `queryForSingleValue` / `insertOne` claimed codec-registry conversion → N.convert / toDocument.
5. Sync `MongoCollectionMapper.insertOne` same codec claim.
6. Reactive executor: get/findFirst/list/coll()/queryForSingleValue/aggregate/watch pipeline return type.
7. Reactive mapper: get/insertOne codec claims; mapReduce empty-function @throws.

### Cassandra
8. `CassandraExecutor` StatementSettings “all operations” / “override” wording; “Statement pooling”.
9. `prepareStatement(String)` @throws null query; Base parseCql metadata / “optimization”; ST return wording.
10. `CqlBuilder.delete(Class)` omit PK exclusion.
11. v3 “Statement and prepared statement pooling”; `execute(String)` unconditional cache claim.

### HBase
12. `HBaseMapper` coprocessor overloads missing `@throws IllegalArgumentException` after parent
    null-checks for `callable`/`callback`.

## Clean slices (no actionable findings)
HBase Any* (11), BigQuery, Cosmos, Neo4j, stubs/placeholders, CqlMapper, ParsedCql, ResultSets,
MongoDB/MongoDBBase, Mongo sync executor, Async Cassandra (v3+v4), AsyncHBase, ColumnFamily,
v2 DynamoDBExecutor sync.

## Verification
- `mvn -pl abacus-da-all compile` = exit 0 after edits.
