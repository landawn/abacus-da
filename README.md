# abacus-da
abacus-da is a data access toolkit for working with NoSQL databases, search engines, cloud data services, and big-data platforms through consistent, high-level APIs.

[![Maven Central](https://img.shields.io/maven-central/v/com.landawn/abacus-da.svg)](https://maven-badges.herokuapp.com/maven-central/com.landawn/abacus-da/)
[![Javadocs](https://www.javadoc.io/badge/com.landawn/abacus-da.svg)](https://www.javadoc.io/doc/com.landawn/abacus-da)
 
It wraps vendor SDKs with executor-style APIs for CRUD, queries, mapping, bulk operations, async/reactive access, and dataset-oriented processing so application code stays smaller and more uniform across backends.

It includes integrations for:

* MongoDB: [MongoCollectionExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/MongoCollectionExecutor_view.html), async MongoDB, and reactive-streams MongoDB
* Cassandra: [CassandraExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/CassandraExecutor_view.html) and [CqlBuilder](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/CqlBuilder_view.html)
* DynamoDB: [DynamoDBExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/DynamoDBExecutor_view.html) and async executors for AWS SDK v1/v2
* HBase: [HBaseExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/HBaseExecutor_view.html)
* Neo4j: [Neo4jExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/Neo4jExecutor_view.html)
* Additional modules for Couchbase, Azure Cosmos DB, Google BigQuery, Elasticsearch, Solr, Lucene, Hadoop/HDFS, Spark, and Blink


## Download/Installation & [Changes](https://github.com/landawn/abacus-da/blob/master/CHANGES.md):

* [Maven](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.landawn%22)

* Gradle:
```gradle
// JDK 17 or above:
implementation 'com.landawn:abacus-da:2.0'
```


### Functional Programming:
(It's very important to learn Lambdas and Stream APIs in Java 8 to get the best user experiences with the APIs provided in abacus-common)

[What's New in Java 8](https://leanpub.com/whatsnewinjava8/read)

[An introduction to the java.util.stream library](https://www.ibm.com/developerworks/library/j-java-streams-1-brian-goetz/index.html)

[When to use parallel streams](http://gee.cs.oswego.edu/dl/html/StreamParallelGuidance.html)

[Top Java 8 stream questions on stackoverflow](./Top_java_8_stream_questions_so.md)

[Kotlin vs Java 8 on Collection](./Java_Kotlin.md)


## User Guide:
Please refer to [Wiki](https://github.com/landawn/abacus-da/wiki)


## Also See: [abacus-common](https://github.com/landawn/abacus-common), [abacus-query](https://github.com/landawn/abacus-query), [abacus-jdbc](https://github.com/landawn/abacus-jdbc).


## Recommended Java programming libraries/frameworks:
[lombok](https://github.com/rzwitserloot/lombok), 
[Jinq](https://github.com/landawn/Jinq), 
[jdbi](https://github.com/jdbi/jdbi), 
[mybatis](https://github.com/mybatis/mybatis-3), 
[Sharding-JDBC](https://github.com/apache/incubator-shardingsphere),
[mapstruct](https://github.com/mapstruct/mapstruct)...[awesome-java](https://github.com/akullpp/awesome-java#database)

## Recommended Java programming tools:
[Spotbugs](https://github.com/spotbugs/spotbugs), [JaCoCo](https://www.eclemma.org/jacoco/)...
