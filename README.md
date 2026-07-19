# abacus-da

[![Maven Central](https://img.shields.io/maven-central/v/com.landawn.abacus/abacus-da-all.svg)](https://central.sonatype.com/artifact/com.landawn.abacus/abacus-da-all/2.8.3)
[![Javadocs](https://img.shields.io/badge/javadoc-2.8.3-brightgreen.svg)](https://www.javadoc.io/doc/com.landawn.abacus/abacus-da-all/2.8.3/index.html)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE.txt)

**abacus-da** is a data-access toolkit for NoSQL databases, search engines, cloud data services, and big-data
platforms. It wraps each vendor SDK in a consistent, high-level *executor* API — CRUD, queries, entity mapping,
bulk operations, async/reactive access, and `Dataset`-oriented processing — so your application code stays smaller
and looks the same across very different backends.

```java
// Every backend follows the same shape: wrap the native driver, then work with entities.
CassandraExecutor db = new CassandraExecutor(session);

db.insert(new User("john", "john@example.com", "active"));
List<User> active = db.list(User.class, "SELECT * FROM users WHERE status = ?", "active");

// ...and every executor exposes a non-blocking view via async():
ContinuableFuture<List<User>> future = db.async().list(User.class, "SELECT * FROM users");
```

## Table of Contents

- [Supported backends](#supported-backends)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Design notes](#design-notes)
- [API docs & guides](#api-docs--guides)
- [Functional programming background](#functional-programming-background)
- [Related projects](#related-projects)
- [License](#license)

## Supported backends

| Backend | Primary API | Notes |
|---|---|---|
| **MongoDB** | [`MongoCollectionExecutor`](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/MongoCollectionExecutor_view.html), `MongoCollectionMapper` | Sync, async, and reactive-streams (`Publisher`-based) variants |
| **Cassandra** | [`CassandraExecutor`](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/CassandraExecutor_view.html), [`CqlBuilder`](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/CqlBuilder_view.html) | DataStax OSS driver 4.x; legacy 3.x driver under `cassandra.v3` |
| **DynamoDB** | [`DynamoDBExecutor`](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/DynamoDBExecutor_view.html) | AWS SDK v1 and v2 (under `dynamodb.v2`), each with an async executor; plus S3 & RDS utilities |
| **HBase** | [`HBaseExecutor`](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/HBaseExecutor_view.html) | Sync and async; fluent `AnyGet`/`AnyPut`/`AnyScan`… builders |
| **Azure Cosmos DB** | `CosmosContainerExecutor` | |
| **Google BigQuery** | `BigQueryExecutor` | |
| **Neo4j** | [`Neo4jExecutor`](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/Neo4jExecutor_view.html) | |
| **Search** | `ElasticsearchExecutor`, `SolrExecutor`, `LuceneExecutor` | |
| **Big data** | Hadoop/HDFS utilities; Spark `Dataset`/`DStream` utilities; Blink `DataSet`/`DataStream` utilities | |

## Installation

Requires **Java 17 or later**. Artifacts are published to Maven Central under the `com.landawn.abacus` group.

Pick `abacus-da-all` for everything, or a slim per-backend artifact that ships only one integration's classes:

| Artifact | Contents |
|---|---|
| `abacus-da-all` | Every backend above (the only artifact that includes Neo4j, search, Hadoop, Spark, and Blink) |
| `abacus-da-mongodb` | `com.landawn.abacus.da.mongodb` |
| `abacus-da-cassandra` | `com.landawn.abacus.da.cassandra` |
| `abacus-da-aws` | `com.landawn.abacus.da.aws` (DynamoDB, S3, RDS) |
| `abacus-da-gcp` | `com.landawn.abacus.da.gcp` (BigQuery) |
| `abacus-da-azure` | `com.landawn.abacus.da.azure` (Cosmos DB) |
| `abacus-da-hbase` | `com.landawn.abacus.da.hbase` |

> The Neo4j, search (Elasticsearch/Solr/Lucene), Hadoop, Spark, and Blink integrations are bundled **only** in
> `abacus-da-all`; there are no separately-published artifacts for them.

**Maven:**

```xml
<dependency>
    <groupId>com.landawn.abacus</groupId>
    <artifactId>abacus-da-mongodb</artifactId>
    <version>2.8.3</version>
</dependency>
```

**Gradle:**

```gradle
implementation 'com.landawn.abacus:abacus-da-all:2.8.3'
// or a single backend:
implementation 'com.landawn.abacus:abacus-da-mongodb:2.8.3'
implementation 'com.landawn.abacus:abacus-da-cassandra:2.8.3'
implementation 'com.landawn.abacus:abacus-da-aws:2.8.3'
implementation 'com.landawn.abacus:abacus-da-gcp:2.8.3'
implementation 'com.landawn.abacus:abacus-da-azure:2.8.3'
implementation 'com.landawn.abacus:abacus-da-hbase:2.8.3'
```

> **Bring your own drivers.** The vendor SDKs (MongoDB driver, DataStax driver, AWS SDK, HBase client, …) — along
> with `abacus-common` and `abacus-query` — are declared with `provided` scope so abacus-da never pins a driver
> version onto your project. Add the matching driver dependency (and `abacus-common` / `abacus-query`) to your own
> build. See [`pom.xml`](./pom.xml) for the exact versions abacus-da is built and tested against.

See the [full changelog](https://github.com/landawn/abacus-da/blob/master/CHANGES.md) for release history.

## Quick start

### MongoDB

```java
MongoClient client = MongoClients.create("mongodb://localhost:27017");
MongoDB mongoDB = new MongoDB(client.getDatabase("myapp"));

MongoCollectionExecutor users = mongoDB.collectionExecutor("users");

users.insertOne(new Document("name", "John").append("age", 30));

List<Document> adults = users.stream(Filters.gte("age", 18)).toList();
users.updateMany(Filters.eq("active", false), Updates.set("status", "inactive"));

// Aggregation pipeline
List<Document> byDept = users.aggregate(Arrays.asList(
        Aggregates.match(Filters.eq("status", "active")),
        Aggregates.group("$department", Accumulators.sum("count", 1)),
        Aggregates.sort(Sorts.descending("count")))).toList();
```

### Cassandra

```java
CqlSession session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
        .withLocalDatacenter("datacenter1")
        .build();

try (CassandraExecutor db = new CassandraExecutor(session)) {
    db.insert(new User("john", "john@example.com", "active"));

    // Positional or named parameters
    List<User> active = db.list(User.class, "SELECT * FROM users WHERE status = ?", "active");
    Optional<User> found = db.findFirst(User.class,
            "SELECT * FROM users WHERE email = :email", N.asMap("email", "john@example.com"));

    // Stream large result sets without loading them fully into memory
    try (Stream<User> stream = db.stream(User.class, "SELECT * FROM users")) {
        stream.filter(User::isActive).forEach(this::process);
    }
}
```

### DynamoDB

```java
DynamoDBExecutor db = new DynamoDBExecutor(dynamoDBClient);

// Automatic object mapping
db.mapper(User.class).putItem(new User("123", "John Doe"));
User user = db.mapper(User.class).getItem(new User("123", null));

QueryRequest query = new QueryRequest()
        .withTableName("Users")
        .withKeyConditionExpression("userId = :userId")
        .withExpressionAttributeValues(N.asMap(":userId", new AttributeValue("123")));
List<User> results = db.list(query, User.class);
```

### HBase

```java
Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create());
HBaseExecutor db = new HBaseExecutor(conn);

// Fluent AnyGet/AnyPut/AnyScan builders, with optional entity mapping
db.put("users", AnyPut.of("user123").addColumn("info", "name", "John"));
boolean exists = db.exists("users", AnyGet.of("user123"));
User user = db.get("users", AnyGet.of("user123"), User.class);

db.scan("users", AnyScan.create().setLimit(100), User.class).forEach(this::process);
```

Every executor also exposes an asynchronous view through `async()` (returning `ContinuableFuture`-typed results),
and MongoDB additionally offers a reactive-streams (`Publisher`-based) variant under
`com.landawn.abacus.da.mongodb.reactivestreams`.

## Design notes

- **One shape across backends.** Each integration is an *executor* that wraps the native driver and adds entity
  mapping, byte/type conversion, bulk helpers, streaming, and an `async()` bridge — so switching or combining
  backends means learning method names, not paradigms.
- **Hybrid "house vs. driver" naming.** Reads follow the abacus house style (`get`/`gett`, `findFirst`, `list`,
  `query` returning a `Dataset`, `stream`, `exists`, `count`, `queryForXxx`); writes mirror each vendor's own
  vocabulary (e.g. MongoDB's `insertOne`/`updateMany`/`findOneAndXxx`). This keeps read code uniform while write
  code stays recognizable to anyone who knows the underlying driver.
- **Provided-scope drivers.** abacus-da deliberately avoids pinning driver versions (see
  [Installation](#installation)), so you stay in control of your dependency tree.
- **Built on [abacus-common](https://github.com/landawn/abacus-common) and
  [abacus-query](https://github.com/landawn/abacus-query)** for `Dataset`, streams, `Filters`/query builders, and
  type handling.

## API docs & guides

- **Javadoc:** [javadoc.io/doc/com.landawn.abacus/abacus-da-all](https://www.javadoc.io/doc/com.landawn.abacus/abacus-da-all/2.8.3/index.html)
- **User guide / wiki:** [github.com/landawn/abacus-da/wiki](https://github.com/landawn/abacus-da/wiki)
- **HTML API previews:** [MongoCollectionExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/MongoCollectionExecutor_view.html) ·
  [CassandraExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/CassandraExecutor_view.html) ·
  [CqlBuilder](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/CqlBuilder_view.html) ·
  [DynamoDBExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/DynamoDBExecutor_view.html) ·
  [HBaseExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/HBaseExecutor_view.html) ·
  [Neo4jExecutor](https://htmlpreview.github.io/?https://github.com/landawn/abacus-da/master/docs/Neo4jExecutor_view.html)

## Functional programming background

abacus-da's query and streaming APIs lean heavily on Java lambdas and the Stream API. These references are worth a
read to get the most out of them:

- [What's New in Java 8](https://leanpub.com/whatsnewinjava8/read)
- [An introduction to the java.util.stream library](https://developer.ibm.com/articles/j-java-streams-1-brian-goetz/)
- [When to use parallel streams](http://gee.cs.oswego.edu/dl/html/StreamParallelGuidance.html)
- [Top Java 8 stream questions on Stack Overflow](./Top_java_8_stream_questions_so.md)
- [Kotlin vs Java 8 on Collections](./Java_Kotlin.md)

## Related projects

- [abacus-common](https://github.com/landawn/abacus-common) — core utilities, `Dataset`, and streams
- [abacus-query](https://github.com/landawn/abacus-query) — SQL/CQL query builders and conditions
- [abacus-jdbc](https://github.com/landawn/abacus-jdbc) — JDBC data access

**Other libraries & tools worth knowing:**
[Lombok](https://github.com/projectlombok/lombok),
[Jinq](https://github.com/landawn/Jinq),
[jdbi](https://github.com/jdbi/jdbi),
[MyBatis](https://github.com/mybatis/mybatis-3),
[ShardingSphere](https://github.com/apache/shardingsphere),
[MapStruct](https://github.com/mapstruct/mapstruct),
[SpotBugs](https://github.com/spotbugs/spotbugs),
[JaCoCo](https://www.eclemma.org/jacoco/) ·
[awesome-java](https://github.com/akullpp/awesome-java#database)

## License

Licensed under the [Apache License 2.0](./LICENSE.txt).
</content>
</invoke>
