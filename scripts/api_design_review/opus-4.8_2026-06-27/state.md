# API Design Review — State File

- **MODEL**: opus-4.8
- **DATE**: 2026-06-27
- **EXECUTION**: PARALLEL (one sub-agent per class/small family batch; orchestrator VERIFIES every agent finding against source before it enters a report)
- **Scope root**: `abacus-da-all/src/main/java/com/landawn/abacus/da/**`
- **Report folder**: `scripts/api_design_review/opus-4.8_2026-06-27/`

## In-scope classes

### Public classes with reviewable public API (35)
| # | Class | File | LOC |
|---|-------|------|-----|
| 1 | aws/AnyUtil | aws/AnyUtil.java | 93 |
| 2 | aws/dynamodb/DynamoDBExecutor (v1 SDK) | aws/dynamodb/DynamoDBExecutor.java | 5100 |
| 3 | aws/dynamodb/AsyncDynamoDBExecutor (v1) | aws/dynamodb/AsyncDynamoDBExecutor.java | 2157 |
| 4 | aws/dynamodb/v2/DynamoDBExecutor (v2 SDK) | aws/dynamodb/v2/DynamoDBExecutor.java | 5202 |
| 5 | aws/dynamodb/v2/AsyncDynamoDBExecutor (v2) | aws/dynamodb/v2/AsyncDynamoDBExecutor.java | 3494 |
| 6 | azure/CosmosContainerExecutor | azure/CosmosContainerExecutor.java | 1964 |
| 7 | cassandra/CassandraExecutorBase (abstract) | cassandra/CassandraExecutorBase.java | 3315 |
| 8 | cassandra/CassandraExecutor (driver v3 Statement<?>) | cassandra/CassandraExecutor.java | 2510 |
| 9 | cassandra/v3/CassandraExecutor (driver v4 Statement) | cassandra/v3/CassandraExecutor.java | 2791 |
| 10 | cassandra/AsyncCassandraExecutorBase (abstract) | cassandra/AsyncCassandraExecutorBase.java | 2943 |
| 11 | cassandra/AsyncCassandraExecutor | cassandra/AsyncCassandraExecutor.java | 465 |
| 12 | cassandra/v3/AsyncCassandraExecutor | cassandra/v3/AsyncCassandraExecutor.java | 460 |
| 13 | cassandra/CqlMapper | cassandra/CqlMapper.java | 909 |
| 14 | cassandra/ParsedCql | cassandra/ParsedCql.java | 680 |
| 15 | cassandra/CqlBuilder | cassandra/CqlBuilder.java | 2554 |
| 16 | gcp/BigQueryExecutor | gcp/BigQueryExecutor.java | 2575 |
| 17 | hbase/HBaseExecutor | hbase/HBaseExecutor.java | 3837 |
| 18 | hbase/AsyncHBaseExecutor | hbase/AsyncHBaseExecutor.java | 2084 |
| 19 | hbase/AnyGet | hbase/AnyGet.java | 1136 |
| 20 | hbase/AnyScan | hbase/AnyScan.java | 2248 |
| 21 | hbase/AnyPut | hbase/AnyPut.java | 1556 |
| 22 | hbase/AnyDelete | hbase/AnyDelete.java | 1098 |
| 23 | hbase/AnyAppend | hbase/AnyAppend.java | 727 |
| 24 | hbase/AnyIncrement | hbase/AnyIncrement.java | 797 |
| 25 | hbase/AnyRowMutations | hbase/AnyRowMutations.java | 461 |
| 26 | hbase/annotation/ColumnFamily | hbase/annotation/ColumnFamily.java | 148 |
| 27 | mongodb/MongoDBBase (abstract) | mongodb/MongoDBBase.java | 1775 |
| 28 | mongodb/MongoDB | mongodb/MongoDB.java | 407 |
| 29 | mongodb/reactivestreams/MongoDB | mongodb/reactivestreams/MongoDB.java | 522 |
| 30 | mongodb/MongoCollectionExecutor | mongodb/MongoCollectionExecutor.java | 4556 |
| 31 | mongodb/AsyncMongoCollectionExecutor | mongodb/AsyncMongoCollectionExecutor.java | 3717 |
| 32 | mongodb/reactivestreams/MongoCollectionExecutor | mongodb/reactivestreams/MongoCollectionExecutor.java | 4159 |
| 33 | mongodb/MongoCollectionMapper | mongodb/MongoCollectionMapper.java | 3361 |
| 34 | mongodb/reactivestreams/MongoCollectionMapper | mongodb/reactivestreams/MongoCollectionMapper.java | 3307 |
| 35 | neo4j/Neo4jExecutor | neo4j/Neo4jExecutor.java | 2465 |

### Package-private abstract base classes (public methods inherited by public concrete classes — reviewed within the concrete family)
- hbase/AnyOperation (abstract) — AnyOperation.java:48
- hbase/AnyOperationWithAttributes (abstract) — AnyOperationWithAttributes.java:64
- hbase/AnyQuery (abstract) — AnyQuery.java:90
- hbase/AnyMutation (abstract) — AnyMutation.java:90

### Package-private placeholder/util classes — NO public API (stub report)
- aws/AWSRDSUtil, aws/AWSS3Util (placeholder, no members)
- blink/DataSetUtil, blink/DataStreamUtil (placeholder)
- spark/DStreamUtil, spark/DatasetUtil (placeholder)
- hadoop/HDFSUtil, hadoop/HadoopUtil (placeholder)
- search/ElasticsearchExecutor, search/LuceneExecutor, search/SolrExecutor (placeholder)
- cassandra/ResultSets (package-private helper — no public API surface)

## Relationship map

### HBase Any* builder hierarchy (mirrors the underlying HBase client class hierarchy)
```
AnyOperation<AO>                       (abstract, pkg-private) ~ org.apache.hadoop.hbase.client.Operation
  └ AnyOperationWithAttributes<AOWA>   (abstract, pkg-private) ~ OperationWithAttributes
      ├ AnyQuery<AQ>                    (abstract, pkg-private) ~ Query
      │   ├ AnyGet  implements Row      ~ Get
      │   └ AnyScan                     ~ Scan
      └ AnyMutation<AM> implements Row  (abstract, pkg-private) ~ Mutation
          ├ AnyDelete                  ~ Delete
          ├ AnyPut                     ~ Put
          ├ AnyAppend                  ~ Append
          └ AnyIncrement               ~ Increment
AnyRowMutations implements Row (standalone) ~ RowMutations
```
Methods visible across these via inheritance are NOT duplicates. The Any* wrappers intentionally mirror the HBase client API method-for-method (fluent setters returning `this`).

### HBase executors
- HBaseExecutor (sync) and AsyncHBaseExecutor (async) intentionally mirror each other. ColumnFamily is a mapping annotation.

### Cassandra
- CassandraExecutorBase<RW,RS,ST,PS,BT> (abstract) holds shared impl; CassandraExecutor (DataStax driver 3, `Statement<?>`) and v3/CassandraExecutor (DataStax driver 4, `Statement`) are two concrete versions that intentionally mirror each other.
- AsyncCassandraExecutorBase (abstract) + AsyncCassandraExecutor + v3/AsyncCassandraExecutor mirror the sync trio asynchronously.
- CqlMapper/ParsedCql/CqlBuilder = CQL build/parse/map helpers (CqlMapper↔SqlMapper, ParsedCql↔ParsedSql, CqlBuilder↔SQLBuilder mirror the abacus SQL equivalents). ResultSets = pkg-private helper.

### DynamoDB (mirror pairs across SDK versions)
- aws/dynamodb/DynamoDBExecutor (AWS SDK v1) ↔ aws/dynamodb/v2/DynamoDBExecutor (AWS SDK v2)
- aws/dynamodb/AsyncDynamoDBExecutor (v1) ↔ aws/dynamodb/v2/AsyncDynamoDBExecutor (v2)

### MongoDB
- MongoDBBase (abstract) ← MongoDB (sync) and reactivestreams/MongoDB (reactive) — factory/registry base.
- MongoCollectionExecutor (sync) ↔ reactivestreams/MongoCollectionExecutor (reactive); AsyncMongoCollectionExecutor = future-based async of the sync one.
- MongoCollectionMapper<T> (sync) ↔ reactivestreams/MongoCollectionMapper<T> (reactive).

### Others
- gcp/BigQueryExecutor, azure/CosmosContainerExecutor, neo4j/Neo4jExecutor — standalone executors.
- aws/AnyUtil — standalone util (one method array2Props).

## Agent task batches (PARALLEL)
- T1: HBase query side — AnyOperation, AnyOperationWithAttributes, AnyQuery, AnyGet, AnyScan
- T2: HBase mutation side — AnyMutation, AnyDelete, AnyPut, AnyAppend, AnyIncrement, AnyRowMutations
- T3: HBase executors — HBaseExecutor, AsyncHBaseExecutor, annotation/ColumnFamily
- T4: cassandra/CassandraExecutorBase
- T5: cassandra/CassandraExecutor + v3/CassandraExecutor
- T6: cassandra Async — AsyncCassandraExecutorBase + AsyncCassandraExecutor + v3/AsyncCassandraExecutor
- T7: cassandra CQL helpers — CqlMapper, ParsedCql, CqlBuilder (+ResultSets stub)
- T8: mongo base+factory — MongoDBBase, MongoDB, reactivestreams/MongoDB
- T9: mongo sync exec — MongoCollectionExecutor, AsyncMongoCollectionExecutor
- T10: mongo reactive exec — reactivestreams/MongoCollectionExecutor
- T11: mongo mappers — MongoCollectionMapper, reactivestreams/MongoCollectionMapper
- T12: dynamodb v1 — DynamoDBExecutor + AsyncDynamoDBExecutor
- T13: dynamodb v2 — v2/DynamoDBExecutor + v2/AsyncDynamoDBExecutor
- T14: azure/CosmosContainerExecutor
- T15: gcp/BigQueryExecutor
- T16: neo4j/Neo4jExecutor
- T17: aws/AnyUtil + all placeholder/util stubs

## Status
| Task | Status |
|------|--------|
| Step 0 scope+relationships | done |
| T1 HBase query | todo |
| T2 HBase mutation | todo |
| T3 HBase executors | todo |
| T4 CassandraExecutorBase | todo |
| T5 Cassandra concrete | todo |
| T6 Cassandra async | todo |
| T7 Cassandra CQL helpers | todo |
| T8 Mongo base+factory | todo |
| T9 Mongo sync exec | todo |
| T10 Mongo reactive exec | todo |
| T11 Mongo mappers | todo |
| T12 DynamoDB v1 | todo |
| T13 DynamoDB v2 | todo |
| T14 Cosmos | todo |
| T15 BigQuery | todo |
| T16 Neo4j | todo |
| T17 AnyUtil + stubs | todo |
| Verification pass | done (all 6 High + ~12 Med findings verified against source, 100% accurate; Cassandra driver-version mapping corrected; v2 DynamoDB CompletableFuture-vs-ContinuableFuture confirmed) |
| Per-class reports | done (all 35 public classes + 4 HBase bases + placeholder stub) |
| SUMMARY.txt | done |
| TASK COMPLETE | yes |

## Verified findings log (orchestrator read the cited lines)
- AnyScan.setStartStopRowForPrefixScan(byte[]) @1195 takes byte[]; replaces deprecated setRowPrefixFilter(Object)@1171; toValueBytes@2986 returns byte[] as-is (no-op). HIGH confirmed.
- HBaseExecutor typed batch get@1703 routes through toList helper@664-678 that skips result.isEmpty()@668-670; untyped get@1569 positionally aligned (Javadoc 1563-64). HIGH confirmed.
- CqlMapper.ids()@387 = ImmutableSet.copyOf (snapshot) but Javadoc@368/381 claims "live view". HIGH confirmed.
- BigQueryExecutor: grep confirms NO findFirst/Optional/get-by-id. HIGH confirmed (missing single-entity reads).
- AsyncMongoCollectionExecutor: no estimatedDocumentCount; groupBy/groupByAndCount@3553/3578/3616/3658 only Document-returning (no Class<T>); no no-arg stream(). HIGH/Med confirmed.
- v2.DynamoDBExecutor.toAttributeValue@822 stringifies List/Map/Set; toValue@1536 materializes L/M/SS/NS/BS. HIGH confirmed (asymmetric round-trip).
- AnyMutation.compareTo@674-677 @Deprecated; AnyRowMutations.compareTo@367-371 NOT @Deprecated. Med confirmed.
- add(Cell): AnyAppend@605 no throws IOException; AnyDelete@520/AnyPut@1370/AnyIncrement@356 throw IOException. Med confirmed.
- setAttribute(String,byte[]) only on AnyAppend@636/AnyIncrement@700 (not Put/Delete); Javadoc admits "observationally equivalent". Med confirmed.
- CassandraExecutorBase.update(String,Object...)@967-968 = pure pass-through to execute. Med confirmed.
- CqlBuilder.onlyIf@555/589 alias to iF; Javadoc "prefer onlyIf, iF kept for backward compat" but iF not @Deprecated. Med confirmed.
- MongoDBBase.toEntity@941 mutates input Document (remove _id@952 / re-put@966 → moves _id to last; concurrency-unsafe vs class "thread-safe" claim). Med confirmed.
- Mongo queryFor* @Beta split: primitives+queryForString+queryForDate(2-arg)@1215-1523 @Beta; queryForDate(3-arg)/queryForSingleValue/queryForSingleNonNull@1560/1602/1658 NOT @Beta. Med confirmed.
- BigQuery buildQueryParameterValue@2513 / getSchema@2560 package-private but referenced by public class Javadoc {@link}@116. Med confirmed.
- DynamoDB v1 toMap(Object[])@1222 overloads the toMap name (vs toMap(item)@1257). Med confirmed.
