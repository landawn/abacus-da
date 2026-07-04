# Line-by-line review round 2026-07-03

Scope: ALL classes under abacus-da-all/src/main/java — WS1 bugs, WS2 javadoc/comments/logs, WS3 naming/consistency/design (BC waived by user).
Baseline: HEAD 3bd5d21, working tree clean. Full offline suite = 3441 tests, 0 failures, 1 error (known flaky live-Mongo test_update_async), 2 skipped.
Process: 14 read-only review agents (no gaps/overlap), main agent sole editor + verifier.

## Agent batches (status)

| # | Agent | Files (lines) | Status |
|---|-------|---------------|--------|
| 1 | ddb-v1 | aws/dynamodb/DynamoDBExecutor (4789), aws/dynamodb/AsyncDynamoDBExecutor (2154) | launched |
| 2 | ddb-v2 | aws/dynamodb/v2/DynamoDBExecutor (4783), v2/AsyncDynamoDBExecutor (3357) | launched |
| 3 | mongo-sync | mongodb/MongoCollectionExecutor (4301), MongoCollectionMapper (3250) | launched |
| 4 | mongo-async-base | mongodb/AsyncMongoCollectionExecutor (3642), MongoDBBase (1648), MongoDB (379) | launched |
| 5 | mongo-reactive | reactivestreams/MongoCollectionExecutor (3948), MongoCollectionMapper (3205), MongoDB (494) | launched |
| 6 | hbase-exec | hbase/HBaseExecutor (3506), AsyncHBaseExecutor (2019) | launched |
| 7 | hbase-query | AnyScan (2127), AnyGet (1091), AnyQuery (520), AnyOperation (167), AnyOperationWithAttributes (212), annotation/ColumnFamily (144) | launched |
| 8 | hbase-mutation | AnyPut (1458), AnyDelete (1054), AnyIncrement (760), AnyAppend (677), AnyMutation (636), AnyRowMutations (438) | launched |
| 9 | cassandra-base | cassandra/CassandraExecutorBase (3187), AsyncCassandraExecutorBase (2730) | launched |
| 10 | cassandra-v4 | cassandra/CassandraExecutor (2287), AsyncCassandraExecutor (445), ResultSets (155), ParsedCql (609), CqlMapper (847) | launched |
| 11 | cassandra-v3 | v3/CassandraExecutor (2569), v3/AsyncCassandraExecutor (440) | launched |
| 12 | cqlbuilder | cassandra/CqlBuilder (2200) | launched |
| 13 | bq-cosmos | gcp/BigQueryExecutor (2406), azure/CosmosContainerExecutor (1903) | launched |
| 14 | neo4j-misc | neo4j/Neo4jExecutor (2347), aws/AnyUtil (84), 11 stubs (search/*, aws/AWSRDSUtil+AWSS3Util, blink/*, hadoop/*, spark/*) | launched |

## Findings ledger

(appended as agents report; each finding: id, file:line, WS, severity, verdict, action)

## Fixes applied

(none yet)
