# API Design Review — State

MODEL = opus-4.8
DATE = 2026-06-23
EXECUTION = PARALLEL (one sub-agent per class or small batch; orchestrator verifies every finding against source before it enters a final report)
SCOPE = all classes in package com.landawn.abacus.da under abacus-da-all/src/main/java

## Out of scope (NO public API — package-private placeholder / internal util)
These declare no `public` type and therefore expose no public API to review.
- search/LuceneExecutor          — empty package-private placeholder
- search/SolrExecutor            — empty package-private placeholder
- search/ElasticsearchExecutor   — package-private placeholder (commented-out skeleton only)
- blink/DataSetUtil              — empty package-private placeholder
- blink/DataStreamUtil           — empty package-private placeholder
- spark/DStreamUtil              — package-private placeholder
- spark/DatasetUtil              — package-private placeholder
- cassandra/ResultSets           — package-private internal util (AsyncResultSet->ResultSet bridge)

## Relationship map (in-scope)

### hbase — "Any*" value-object wrappers (mirror the HBase client API)
Inheritance chain (bases are package-private abstract but contribute PUBLIC inherited methods to the public final subclasses, so their public methods ARE in scope via the subclasses):
- AnyOperation<AO>                         (pkg-private abstract, root)
  - AnyOperationWithAttributes<AOWA>       (pkg-private abstract)
    - AnyQuery<AQ>                          (pkg-private abstract)
      - AnyGet   (public final, implements Row)
      - AnyScan  (public final)
    - AnyMutation<AM>                       (pkg-private abstract, implements Row)
      - AnyAppend    (public final)
      - AnyDelete    (public final)
      - AnyIncrement (public final)
      - AnyPut       (public final)
- AnyRowMutations (public final, implements Row)  — standalone, wraps HBase RowMutations
Base-defined public methods reviewed ONCE at the base file to avoid duplicate findings across subclasses (GROUND RULE 2).

### hbase — executors
- HBaseExecutor (public final, AutoCloseable) — sync facade; uses Any* + AnyMapper
- AsyncHBaseExecutor (public final)            — async mirror of HBaseExecutor
- hbase/annotation/ColumnFamily (public @interface)

### mongodb
- MongoDBBase (public abstract)        — base
  - MongoDB (public final) extends MongoDBBase            — factory/facade
- MongoCollectionExecutor (public final)
- AsyncMongoCollectionExecutor (public final) — async mirror of MongoCollectionExecutor
- MongoCollectionMapper<T> (public final)
- AnyUtil (public final)               — bson helpers
- mongodb/reactivestreams/* mirror the blocking mongodb/* (MongoDB, MongoCollectionExecutor, MongoCollectionMapper) with reactive return types
  - reactivestreams/MongoDB extends mongodb.MongoDBBase (shared base)

### cassandra
- CassandraExecutorBase<RW,RS,ST,PS,BT> (public abstract, AutoCloseable)
  - CassandraExecutor (public final) extends ...Base<Row,ResultSet,Statement<?>,PreparedStatement,BatchType>      [driver v4]
  - v3/CassandraExecutor (public final) extends ...Base<Row,ResultSet,Statement,PreparedStatement,BatchStatement.Type> [driver v3]
- AsyncCassandraExecutorBase<...> (public abstract)
  - AsyncCassandraExecutor (public final) extends ...Base    [v4]
  - v3/AsyncCassandraExecutor (public final) extends ...Base [v3]
- CqlBuilder (public) extends AbstractQueryBuilder<CqlBuilder>
- CqlMapper (public final)             — mirrors SqlMapper
- ParsedCql (public final)             — mirrors ParsedSql

### aws / gcp / azure / neo4j
- aws/AWSRDSUtil (public final), aws/AWSS3Util (public final) — thin static utils
- aws/dynamodb/DynamoDBExecutor (public final, AutoCloseable)        [v1 SDK]
  - aws/dynamodb/AsyncDynamoDBExecutor (public final)                 — async mirror [v1]
- aws/dynamodb/v2/DynamoDBExecutor (public final, AutoCloseable)     [v2 SDK]
  - aws/dynamodb/v2/AsyncDynamoDBExecutor (public final, AutoCloseable) — async mirror [v2]
- gcp/BigQueryExecutor (public)
- azure/CosmosContainerExecutor (public)
- neo4j/Neo4jExecutor (public final)
- hadoop/HadoopUtil (public final), hadoop/HDFSUtil (public final) — thin static utils

## Per-class status (todo / in-progress / done)
Legend: [ ] todo  [~] agent dispatched  [V] findings being verified  [X] done (final .txt written)

### hbase Any* value objects   [WAVE 1 — ALL DONE, verified]
- [X] hbase/AnyOperation + AnyOperationWithAttributes + AnyQuery + AnyMutation -> AnyOperation_base_chain.txt
- [X] hbase/AnyGet -> AnyGet.txt
- [X] hbase/AnyScan -> AnyScan.txt
- [X] hbase/AnyPut -> AnyPut.txt
- [X] hbase/AnyDelete -> AnyDelete.txt
- [X] hbase/AnyIncrement -> AnyIncrement.txt
- [X] hbase/AnyAppend -> AnyAppend.txt
- [X] hbase/AnyRowMutations -> AnyRowMutations.txt
### hbase executors   [WAVE 1 — DONE, verified]
- [X] hbase/HBaseExecutor -> HBaseExecutor.txt
- [X] hbase/AsyncHBaseExecutor -> AsyncHBaseExecutor.txt
- [X] hbase/annotation/ColumnFamily -> ColumnFamily.txt

NOTE: base chain (4 pkg-private abstract classes) reported in one combined file AnyOperation_base_chain.txt
since they are not independently public and findings are cross-cutting.
VERIFICATION CORRECTIONS made vs agent drafts: (a) AnyAppend.add(Cell) "swallows exception" REJECTED —
no try/catch exists, faithful HBase mirror; (b) setAttribute(String,byte[]) reframed from "missing on
Put/Delete" to "redundant on Increment/Append" (toValueBytes identity for byte[]); (c) AnyScan getFamilies
null tempered Low (null = all-families HBase signal); (d) AnyDelete toRowBytes/toRowKeyBytes = behaviorally
identical (both delegate to toValueBytes).
### mongodb   [WAVE 2 — ALL DONE, verified]
- [X] mongodb/MongoDBBase -> MongoDBBase.txt
- [X] mongodb/MongoDB -> MongoDB.txt
- [X] mongodb/MongoCollectionExecutor -> MongoCollectionExecutor.txt
- [X] mongodb/AsyncMongoCollectionExecutor -> AsyncMongoCollectionExecutor.txt
- [X] mongodb/MongoCollectionMapper -> MongoCollectionMapper.txt
- [X] mongodb/AnyUtil -> AnyUtil.txt  (KEY: misplaced — used only by dynamodb v2)
- [X] mongodb/reactivestreams/MongoDB -> reactivestreams-MongoDB.txt
- [X] mongodb/reactivestreams/MongoCollectionExecutor -> reactivestreams-MongoCollectionExecutor.txt
- [X] mongodb/reactivestreams/MongoCollectionMapper -> reactivestreams-MongoCollectionMapper.txt
  (KEY: distinct() behavioral divergence vs blocking; coll()/collectionExecutor() naming drift)
### cassandra   [WAVE 3 — ALL DONE, verified]
- [X] cassandra/CassandraExecutorBase -> CassandraExecutorBase.txt
- [X] cassandra/CassandraExecutor -> CassandraExecutor.txt (v4)
- [X] cassandra/v3/CassandraExecutor -> v3-CassandraExecutor.txt  (KEY: missing @Deprecated annotation)
- [X] cassandra/AsyncCassandraExecutorBase -> AsyncCassandraExecutorBase.txt  (KEY: 0 @Beta vs 20 sync)
- [X] cassandra/AsyncCassandraExecutor -> AsyncCassandraExecutor.txt (v4)
- [X] cassandra/v3/AsyncCassandraExecutor -> v3-AsyncCassandraExecutor.txt
- [X] cassandra/CqlBuilder -> CqlBuilder.txt  (KEY: parse->renderCondition mislabel)
- [X] cassandra/CqlMapper -> CqlMapper.txt  (KEY: add() split contract)
- [X] cassandra/ParsedCql -> ParsedCql.txt  (KEY: namedParameters() leaks pooled mutable map)
### aws/gcp/azure/neo4j/hadoop   [WAVE 4 — ALL DONE, verified]
- [X] aws/dynamodb/DynamoDBExecutor -> DynamoDBExecutor.txt (v1)
- [X] aws/dynamodb/AsyncDynamoDBExecutor -> AsyncDynamoDBExecutor.txt (v1; phantom <T> on query)
- [X] aws/dynamodb/v2/DynamoDBExecutor -> v2-DynamoDBExecutor.txt
- [X] aws/dynamodb/v2/AsyncDynamoDBExecutor -> v2-AsyncDynamoDBExecutor.txt  (KEY: CompletableFuture not ContinuableFuture)
- [X] gcp/BigQueryExecutor -> BigQueryExecutor.txt  (KEY: update/delete NPE vs insert IAE)
- [X] azure/CosmosContainerExecutor -> CosmosContainerExecutor.txt  (KEY: missing get-Optional/exists/count)
- [X] neo4j/Neo4jExecutor -> Neo4jExecutor.txt  (KEY: query()->Stream naming)
- [X] aws/AWSRDSUtil -> AWSRDSUtil.txt ; aws/AWSS3Util -> AWSS3Util.txt ; hadoop/HadoopUtil -> HadoopUtil.txt ;
      hadoop/HDFSUtil -> HDFSUtil.txt  (all empty public placeholders)

## ===== COMPLETE: all 43 in-scope classes reviewed + SUMMARY.txt written. =====
- [X] SUMMARY.txt (cross-class consistency + P1/P2/P3 action plan)
- drafts/ holds the raw agent drafts; root holds the verified final per-class .txt reports.
