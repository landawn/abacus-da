# abacus-da

[![Maven Central](https://img.shields.io/maven-central/v/com.landawn.abacus/abacus-da-all.svg)](https://central.sonatype.com/artifact/com.landawn.abacus/abacus-da-all)
[![Javadoc](https://img.shields.io/badge/API-Javadoc-brightgreen.svg)](https://www.javadoc.io/doc/com.landawn.abacus/abacus-da-all)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE.txt)

**abacus-da** is a Java data-access toolkit for MongoDB, Cassandra, DynamoDB, HBase,
Azure Cosmos DB, Google BigQuery, and Neo4j OGM. It wraps native clients with
conveniences for entity mapping, CRUD, parameterized queries, tabular results,
and streams, while retaining access to native request and response types.

This guide describes **version 2.9.1 and Java 21**, as configured in this checkout's
[pom.xml](./pom.xml). Dependency examples use that version. If it is not available
in your repository, install this checkout locally or select a published release
with its matching documentation and dependencies. See [CHANGES.md](./CHANGES.md)
for release history.

The integrations share conventions, but their query syntax, keys, transactions,
consistency, pagination, and async behavior remain backend-specific. The wrappers
do not automatically provision databases or migrate schemas.

## Contents

- [Integrations and artifacts](#integrations-and-artifacts)
- [Installation](#installation)
- [Core API conventions](#core-api-conventions)
- [MongoDB](#mongodb)
- [Cassandra](#cassandra)
- [CQL builder reference](#cql-builder-reference)
- [DynamoDB](#dynamodb)
- [HBase](#hbase)
- [Azure Cosmos DB](#azure-cosmos-db)
- [Google BigQuery](#google-bigquery)
- [Neo4j](#neo4j)
- [Async and reactive reference](#async-and-reactive-reference)
- [Resource management and operational guidance](#resource-management-and-operational-guidance)
- [Troubleshooting](#troubleshooting)
- [Building and testing](#building-and-testing)
- [Documentation and related projects](#documentation-and-related-projects)
- [License](#license)

## Integrations and artifacts

All artifacts use group ID `com.landawn.abacus`. Packages in this table are
relative to `com.landawn.abacus.da`.

| Integration | Artifact | Package and entry points |
| --- | --- | --- |
| MongoDB synchronous | `abacus-da-mongodb` | `mongodb.MongoDB`, `MongoCollectionExecutor`, `MongoCollectionMapper<T>` |
| MongoDB reactive | `abacus-da-mongodb` | `mongodb.reactivestreams.MongoDB`, `MongoCollectionExecutor`, `MongoCollectionMapper<T>` |
| Cassandra driver 4 | `abacus-da-cassandra` | `cassandra.CassandraExecutor`, `CqlBuilder`, `CqlMapper`, `ParsedCql`, `ResultSets` |
| Cassandra driver 3 | `abacus-da-cassandra` | `cassandra.v3.CassandraExecutor` |
| DynamoDB AWS SDK v2 | `abacus-da-aws` | `aws.dynamodb.v2.DynamoDBExecutor`, `AsyncDynamoDBExecutor` |
| DynamoDB AWS SDK v1 | `abacus-da-aws` | `aws.dynamodb.DynamoDBExecutor`, `AsyncDynamoDBExecutor` |
| HBase 2.x | `abacus-da-hbase` | `hbase.HBaseExecutor`, `AnyGet`, `AnyPut`, `AnyScan`, other `Any*` wrappers |
| Azure Cosmos DB | `abacus-da-azure` | `azure.CosmosContainerExecutor` |
| Google BigQuery | `abacus-da-gcp` | `gcp.BigQueryExecutor` |
| Neo4j OGM | `abacus-da-all` | `neo4j.Neo4jExecutor` |

Use `abacus-da-all` for all implementation classes, or choose individual backend
artifacts. Do not combine `abacus-da-all` with individual artifacts: they contain
duplicate classes. Multiple individual artifacts can be used together.

Elasticsearch, Solr, Lucene, Hadoop/HDFS, Spark, Blink, S3, and RDS classes in this
checkout are **package-private placeholders with no usable public API**. Their
presence in the source does not provide integrations for those services. There
is no separate Neo4j artifact in this reactor.

## Installation

### Requirements and dependency ownership

Use **JDK 21 or later**. Configure the backend, schema, credentials, region or
datacenter, and networking separately.

The integration modules declare `abacus-common`, `abacus-query`, and vendor SDKs
with **provided** scope. Applications must add the dependencies needed by the
APIs they use to their own runtime classpath. The all-in-one artifact bundles
abacus-da classes, not all vendor SDKs.

For synchronous MongoDB, add these dependencies inside your application's Maven
`<dependencies>` element and configure its compiler for Java 21:

```xml
<dependency>
    <groupId>com.landawn.abacus</groupId>
    <artifactId>abacus-da-mongodb</artifactId>
    <version>2.9.1</version>
</dependency>
<dependency>
    <groupId>com.landawn.abacus</groupId>
    <artifactId>abacus-common</artifactId>
    <version>8.3.0</version>
</dependency>
<dependency>
    <groupId>com.landawn.abacus</groupId>
    <artifactId>abacus-query</artifactId>
    <version>4.9.5</version>
</dependency>
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync</artifactId>
    <version>5.13.0</version>
</dependency>
```

Equivalent Gradle Kotlin DSL configuration:

```kotlin
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    implementation("com.landawn.abacus:abacus-da-mongodb:2.9.1")
    implementation("com.landawn.abacus:abacus-common:8.3.0")
    implementation("com.landawn.abacus:abacus-query:4.9.5")
    implementation("org.mongodb:mongodb-driver-sync:5.13.0")
}
```

### Driver dependency reference

These are the versions configured in this checkout's parent POM, not a claim
about the newest releases or compatibility with every driver version. Choose the
appropriate backend module and add its driver alongside the core dependencies.

| API | Additional Maven coordinates (`groupId:artifactId:version`) |
| --- | --- |
| MongoDB sync | `org.mongodb:mongodb-driver-sync:5.13.0` |
| MongoDB reactive | `org.mongodb:mongodb-driver-reactivestreams:5.13.0`, `io.projectreactor:reactor-core:3.8.7` |
| Cassandra driver 4 | `org.apache.cassandra:java-driver-core:4.19.3` |
| Cassandra driver 3 | `com.datastax.cassandra:cassandra-driver-core:3.11.5`, `com.datastax.cassandra:cassandra-driver-mapping:3.11.5`, `com.codahale.metrics:metrics-core:3.0.2` |
| DynamoDB SDK v2 | `software.amazon.awssdk:dynamodb:2.55.6` |
| DynamoDB SDK v1 | `com.amazonaws:aws-java-sdk-dynamodb:1.12.797` |
| HBase | `org.apache.hbase:hbase-client:2.6.6`, `org.apache.hbase:hbase-common:2.6.6` |
| Cosmos DB | `com.azure:azure-cosmos:4.83.0` |
| BigQuery | `com.google.cloud:google-cloud-bigquery:2.72.0` |
| Neo4j | `org.neo4j:neo4j-ogm-core:5.0.10`, plus the OGM transport implementation for your connection |

Cassandra 4 uses Apache Cassandra artifact coordinates but retains Java packages
under `com.datastax.oss.driver.api`. The driver-3 adapter requires the legacy API.
HBase 3 removes public APIs used by this adapter; use the HBase 2 baseline.

`CqlMapper` XML persistence also needs Jakarta XML Binding at runtime; the build
uses `jakarta.xml.bind:jakarta.xml.bind-api:4.0.5` in its tests. Consult the
[parent POM](./pom.xml) and module POMs for complete dependency/scope definitions.

## Core API conventions

### Result shapes

These are recurring conventions, not a promise that every executor exposes
every method or uses the same argument order.

| Result shape | Typical methods | Use |
| --- | --- | --- |
| Optional entity | `get`, `findFirst`, Neo4j `findOnly` | Represent an absent result explicitly |
| Nullable entity | `gett`, DynamoDB `getItem`, Neo4j `load` | Handle `null` when nothing matches |
| Materialized rows | `list`, Neo4j `loadAll` | A bounded result fits in memory |
| Tabular data | `query`, static `extractData` | Work with an abacus `Dataset` |
| Row traversal | `stream`, HBase/DynamoDB `scan` | Consume results without collecting every row |
| Scalar | `queryForSingleValue`, `queryForSingleNonNull`, backend-specific `queryForInt`, etc. | Read a selected value |
| Native result | `execute`, native request overloads | Retain backend metadata and controls |

Cassandra's unique-result reads can throw `DuplicateResultException`; use
`findFirst` when accepting the first row is intentional. HBase's raw `get`
returns an HBase `Result`, whose empty state is checked with `isEmpty()`.

### Imports and mapping

Many return types belong to abacus-common:

```java
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
```

These `Stream` and `Optional` types differ from their `java.util` counterparts.
`Nullable<T>` can distinguish an absent result from a present null value;
`queryForSingleNonNull` collapses null into an empty optional. Check the selected
overload's contract when nulls and multiple results matter.

MongoDB filters use `com.mongodb.client.model.Filters`; CQL/generated SQL
conditions use `com.landawn.abacus.query.Filters`. Avoid conflicting imports.

Start entity mapping with ordinary Java beans having a no-argument constructor
and getters/setters. Use explicit table, property, and key metadata. Abacus
annotations include `com.landawn.abacus.annotation.Table`, `Id`, and `Column`
where supported. Neo4j uses OGM metadata; Cosmos DB uses Azure SDK serialization.

Generated Cassandra and BigQuery statements default to snake-case naming.
DynamoDB v2's mapper defaults to camel case. MongoDB recommends a Java `id`
property for BSON `_id`. HBase also needs column-family mapping. A single bean
definition does not necessarily have identical storage semantics across backends.

Examples below are independent. Unless a complete class is shown, place imports
at file level and executable statements inside a method. Handle or declare checked
exceptions such as I/O and future-wait failures. Named prerequisites are supplied
by the application or preceding example.

## MongoDB

Source: [MongoDB](./abacus-da-all/src/main/java/com/landawn/abacus/da/mongodb/MongoDB.java),
[MongoCollectionExecutor](./abacus-da-all/src/main/java/com/landawn/abacus/da/mongodb/MongoCollectionExecutor.java),
[MongoCollectionMapper](./abacus-da-all/src/main/java/com/landawn/abacus/da/mongodb/MongoCollectionMapper.java).

### Connect and perform CRUD

Run MongoDB at `localhost:27017`. This example writes to `myapp.users`.
The application owns the client.

```java
import java.util.List;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.landawn.abacus.da.mongodb.MongoDB;
import com.landawn.abacus.da.mongodb.MongoCollectionExecutor;
import com.landawn.abacus.util.stream.Stream;

try (MongoClient client = MongoClients.create("mongodb://localhost:27017")) {
    MongoDB database = new MongoDB(client.getDatabase("myapp"));
    MongoCollectionExecutor users = database.collectionExecutor("users");

    var inserted = users.insertOne(new Document("name", "Alice").append("age", 30));
    var identity = Filters.eq("_id", inserted.getInsertedId());
    List<Document> adults = users.list(Filters.gte("age", 18));

    users.updateOne(identity, Updates.set("age", 31));
    try (Stream<Document> rows = users.stream(Filters.gte("age", 18))) {
        rows.forEach(System.out::println);
    }

    users.deleteOne(identity);
}
```

The `get(String)`/`gett(String)` convenience overloads expect a hexadecimal
ObjectId string. For string-valued IDs, use `Filters.eq("_id", yourId)` with
`findFirst` or another filter-based method.

### Typed reads and aggregation

Use `database.collectionMapper("users", User.class)` for a reusable typed mapper,
or `users.list(filter, User.class)` for an individual typed read. The bean must
match the stored document. Read overloads accept selected property names or BSON
projections, filters, sorting, offsets, and counts. Use stable sorting for paging.

Within the client lifetime above, aggregate using native MongoDB builders:

```java
import java.util.List;
import org.bson.Document;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import com.landawn.abacus.util.stream.Stream;

try (Stream<Document> summary = users.aggregate(List.of(
        Aggregates.group("$department", Accumulators.sum("count", 1)),
        Aggregates.sort(Sorts.descending("count"))))) {
    summary.forEach(System.out::println);
}
```

| Operation | API families |
| --- | --- |
| Reads | `exists`, `count`, `estimatedDocumentCount`, `get`/`gett`, `findFirst`, `list`, `query`, `stream` |
| Writes | `insertOne`, `insertMany`, `updateOne`, `updateMany`, `replaceOne`, `deleteOne`, `deleteMany` |
| Bulk | `bulkInsert`, `bulkWrite` with driver `WriteModel` and `BulkWriteOptions` |
| Read and modify | `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete` |
| Aggregation/observation | `aggregate`, `distinct`, `groupBy`, `watch` |
| Native access | `database.db()`, `database.collection(...)`, `users.coll()` |

The `MongoDB` wrapper installs its own codec registry, replacing the supplied
database's registry. Custom codecs configured on the original client/database
are not automatically preserved.

### Reactive MongoDB

Use the separate `mongodb.reactivestreams` package and reactive driver.
Methods return Reactor `Mono`/`Flux` implementing Reactive Streams `Publisher`;
in particular, `list` returns a `Flux<T>`, not a materialized Java list.

```java
import org.bson.Document;
import reactor.core.publisher.Mono;
import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.landawn.abacus.da.mongodb.reactivestreams.MongoDB;

MongoClient client = MongoClients.create("mongodb://localhost:27017");
MongoDB database = new MongoDB(client.getDatabase("myapp"));
Mono<Long> adultCount = database.collectionExecutor("users")
        .list(Filters.gte("age", 18), Document.class)
        .count();
// Compose/return adultCount, or subscribe with error handling.
// Close client at application shutdown, after subscriptions finish.
```

Creating a publisher does not consume results. Keep the client alive through
completion/cancellation. The synchronous executor's `async()` facade instead
schedules blocking driver calls on worker threads.

## Cassandra

Source: [CassandraExecutor](./abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CassandraExecutor.java),
[CassandraExecutorBase](./abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CassandraExecutorBase.java).

### Schema and entity

The example assumes a local instance with datacenter `datacenter1` and this schema:

```sql
CREATE KEYSPACE IF NOT EXISTS myapp
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS myapp.users (
    id text PRIMARY KEY,
    name text,
    email text
);
```

This replication configuration is for a single-node local example. Match your
actual deployment's topology when configuring a production keyspace.

```java
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;

@Table(name = "users")
public class User {
    @Id
    private String id;
    private String name;
    private String email;

    public User() { }

    public User(String id, String name, String email) {
        this.id = id;
        this.name = name;
        this.email = email;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}
```

### Connect, bind values, and read

```java
import java.net.InetSocketAddress;
import java.util.List;
import com.datastax.oss.driver.api.core.CqlSession;
import com.landawn.abacus.da.cassandra.CassandraExecutor;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Optional;

CqlSession session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
        .withLocalDatacenter("datacenter1")
        .withKeyspace("myapp")
        .build();

CassandraExecutor db = new CassandraExecutor(session);
try {
    db.insert(new User("u-1", "Alice", "alice@example.com"));
    Optional<User> found = db.get(User.class, "u-1");

    List<User> rows = db.list(User.class,
            "SELECT id, name, email FROM users WHERE id = ?", "u-1");

    db.execute("UPDATE users SET email = :email WHERE id = :id",
            java.util.Map.<String, Object>of("email", "new@example.com", "id", "u-1"));

    try (Stream<User> stream = db.stream(User.class,
            "SELECT id, name, email FROM users LIMIT 100")) {
        stream.forEach(user -> System.out.println(user.getName()));
    }

    db.delete(User.class, "u-1");
} finally {
    db.close(); // Closes the wrapped session.
}
```

Bind data values rather than concatenating them into CQL. The executor parses
named markers such as `:id`; do not mix named and positional markers. Parameter
names/counts must match. Identifiers and raw query fragments remain application
controlled because value parameters do not represent table or column names.

### CRUD and driver controls

| Need | API |
| --- | --- |
| Key lookup | `get(Class, ids...)`, `gett(Class, ids...)`, `exists(Class, ids...)` |
| Entity writes | `insert(entity)`, `update(entity)`, `delete(entity)` |
| Selected-property update | `update(entity, propNamesToUpdate)` |
| Conditions | Overloads accepting abacus-query `Condition` |
| Batches | `batchInsert`/`batchUpdate` with driver batch type; `batchDelete` for entity collections |
| Queries | `findFirst`, `list`, `query`, `stream`, scalar query methods |
| Native controls | `execute(Statement<?>)`, `session()`, constructor `StatementSettings` |
| Conversion | Static `toEntity`, `toMap`, `toList`, `extractData`; `ResultSets` helpers |
| Query/naming configuration | Constructor overloads with `CqlMapper` and `NamingPolicy` |

Annotate primary-key properties with `@Id`; generated updates/deletes need keys.
Composite keys must match the schema. `registerKeys` is a deprecated alternative.
`@Column` overrides a stored name; the default `NamingPolicy.SNAKE_CASE` maps
`firstName` to `first_name` in generated statements.

Cassandra still enforces partition/clustering key, index, ordering, and filtering
rules. Wrapping an arbitrary non-key query does not make it valid or efficient.
Use driver paging state for page navigation. Lists materialize results; streams
can fetch additional pages while being consumed.

For driver 3, import `com.landawn.abacus.da.cassandra.v3.CassandraExecutor` and
supply a `com.datastax.driver.core.Session`. Do not mix driver generations'
sessions, rows, or statements. Closing an executor closes its wrapped session;
assign one lifecycle owner if sessions are shared.

## CQL builder reference

Source: [CqlBuilder](./abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CqlBuilder.java),
[ParsedCql](./abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java),
[CqlMapper](./abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CqlMapper.java).

`CqlBuilder.Dsl` provides immutable factories. Each statement call creates a
mutable builder. Complete it once with `build()` to obtain text and parameters
and release internal resources. With `db` and `User` from the Cassandra example:

```java
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import java.util.List;
import com.landawn.abacus.query.Filters;

var statement = PSC.select("id", "name", "email")
        .from("users")
        .where(Filters.eq("id", "u-1"))
        .build();

System.out.println(statement.query());
// SELECT id, name, email FROM users WHERE id = ?

List<User> users = db.list(User.class,
        statement.query(), statement.parameters().toArray());
```

Pass both the generated query and parameters to execution.

| Parameter style | Preserve names | snake_case | SCREAMING_SNAKE_CASE | camelCase |
| --- | --- | --- | --- | --- |
| Positional `?` | `PSB` | `PSC` | `PAC` | `PLC` |
| Named `:name` | `NSB` | `NSC` | `NAC` | `NLC` |

Raw literal factories `SCCB`, `ACCB`, and `LCCB` are deprecated. Prefer
parameterized factories for application data.

| Capability | Methods |
| --- | --- |
| Statements | `select`, `selectFrom`, `insert`, `update`, `deleteFrom` |
| Conditions/projection | `where`, `set`, `orderBy`, `groupBy`, `limit` |
| Expiration/write time | `usingTTL(seconds)`, `usingTimestamp(timestamp)` |
| Conditional writes | `ifNotExists`, `ifExists`, `onlyIf` |
| Filtering | `allowFiltering`, rendered after other supported clauses |
| Completion | `build()`; `apply`/`accept` also finalize the builder |

Builders are not thread-safe and cannot be reused after completion. Share DSL
constants rather than builders. Identifiers follow the chosen naming policy;
explicit table strings are preserved except for reserved-keyword quoting.
Explicitly quoted CQL identifiers remain case-sensitive.

SQL-only features such as offsets, `HAVING`, unions, and `FOR UPDATE` are rejected.
Some inherited methods can still render syntax Cassandra rejects, such as joins.
Successful construction does not validate the server schema. Use `limit(count)`
and driver paging state rather than SQL offsets.

`ParsedCql` parses markers and exposes parameterized CQL and parameter names.
`CqlMapper` manages reusable named query definitions, including XML load/save.
These utilities prepare queries; they do not execute them or create schema.

## DynamoDB

Source: [SDK v2 executor](./abacus-da-all/src/main/java/com/landawn/abacus/da/aws/dynamodb/v2/DynamoDBExecutor.java),
[SDK v1 executor](./abacus-da-all/src/main/java/com/landawn/abacus/da/aws/dynamodb/DynamoDBExecutor.java).

### AWS SDK v2 example

Create `Users` with string partition key `id`. Configure credentials through
the SDK credential provider chain and select the table's region. For DynamoDB
Local, configure an endpoint override and local credentials separately.

```java
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor;

DynamoDbClient client = DynamoDbClient.builder().region(Region.US_WEST_2).build();
DynamoDBExecutor db = new DynamoDBExecutor(client);
try {
    db.putItem("Users", DynamoDBExecutor.asItem(
            "id", "u-1", "name", "Alice", "email", "alice@example.com"));

    Map<String, Object> user = db.getItem("Users", DynamoDBExecutor.asKey("id", "u-1"));

    QueryRequest request = QueryRequest.builder()
            .tableName("Users")
            .keyConditionExpression("#id = :id")
            .expressionAttributeNames(Map.of("#id", "id"))
            .expressionAttributeValues(DynamoDBExecutor.asItem(":id", "u-1"))
            .build();
    List<Map<String, Object>> matches = db.list(request);

    db.deleteItem("Users", DynamoDBExecutor.asKey("id", "u-1"));
} finally {
    db.close(); // Closes the synchronous DynamoDbClient.
}
```

`asKey`, `asItem`, `toAttributeValue`, `toItem`, `toMap`, and `toEntity` bridge
Java values and SDK attribute maps. Raw attribute names must match the schema.
A query needs a valid key condition; filtering does not replace a partition-key query.

### Typed mapper

The v2 mapper uses abacus/JPA table and ID metadata. Use the `User` bean above with
`@Table(name = "Users")` instead of `@Table(name = "users")`, then within the executor lifetime:

```java
var mapper = db.mapper(User.class);
mapper.putItem(new User("u-1", "Alice", "alice@example.com"));
User found = mapper.getItem(new User("u-1", null, null));
```

Alternatively, `db.mapper(User.class, "Users", NamingPolicy.CAMEL_CASE)` supplies
the table explicitly (`NamingPolicy` is in `com.landawn.abacus.util`). The mapper
requires one or two ID properties; use `@Id`. A property named `id` is a fallback
when no ID is annotated. Select a naming policy that matches stored attribute names
and validate both partition/sort key mappings for composite keys.

### Pagination and batches

For v2 `list(QueryRequest)` and `query(QueryRequest)`, an empty `exclusiveStartKey`
enables automatic traversal of all pages. A nonempty start key makes those methods
materialize one page. Request `limit` is a page limit, **not a total result limit**
during automatic pagination. For manual paging, including the first page, use
`db.dynamoDBClient().query(request)` and retain its `lastEvaluatedKey`.

`stream(QueryRequest)` and `scan(ScanRequest)` expose row streams. Close them after
consumption and bound work deliberately; a scan can read a whole table. Empty
pages may still have continuation keys, so follow the key rather than interpreting
zero returned items as completion.

`batchWriteItem` returns the native response: inspect unprocessed items and arrange
appropriate retries. Use native request/response APIs when unprocessed keys,
consumed capacity, conditional outcomes, or other metadata are required.
A batch is not automatically a transaction or an all-or-nothing operation.

### Driver generations

| Concern | AWS SDK v1 | AWS SDK v2 |
| --- | --- | --- |
| Package | `aws.dynamodb` | `aws.dynamodb.v2` |
| Client | `AmazonDynamoDBClient` | `DynamoDbClient` |
| Requests | Mutable requests with `with...` methods | Immutable requests built with `.builder()` |
| Async entry | `db.async()` | `db.async(DynamoDbAsyncClient)` or `AsyncDynamoDBExecutor` |
| Future | `ContinuableFuture<T>` | `CompletableFuture<T>` |
| Execution | Worker-thread wrapper | AWS asynchronous client |

The v1 path includes AWS `DynamoDBMapper` integration and configuration. Do not
copy v1 request classes or mapping assumptions into v2. A v2 async client has its
own lifetime; closing the synchronous executor does not close that separate client.

## HBase

Source: [HBaseExecutor](./abacus-da-all/src/main/java/com/landawn/abacus/da/hbase/HBaseExecutor.java),
[ColumnFamily](./abacus-da-all/src/main/java/com/landawn/abacus/da/hbase/annotation/ColumnFamily.java).

Provide configuration, normally through `hbase-site.xml` on the classpath.
The example assumes a `users` table with an `info` column family.

```java
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import com.landawn.abacus.da.hbase.AnyGet;
import com.landawn.abacus.da.hbase.AnyPut;
import com.landawn.abacus.da.hbase.AnyScan;
import com.landawn.abacus.da.hbase.HBaseExecutor;
import com.landawn.abacus.util.stream.Stream;

HBaseExecutor db = new HBaseExecutor(
        ConnectionFactory.createConnection(HBaseConfiguration.create()));
try {
    db.put("users", AnyPut.of("u-1").addColumn("info", "name", "Alice"));

    Result row = db.get("users", AnyGet.of("u-1"));
    if (!row.isEmpty()) {
        System.out.println(Bytes.toString(row.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
    }

    try (Stream<Result> rows = db.scan("users",
            AnyScan.create().addColumn("info", "name").setLimit(100))) {
        rows.forEach(System.out::println);
    }
} finally {
    db.close(); // Closes admin and connection.
}
```

| Wrapper | Role |
| --- | --- |
| `AnyGet` | Row key and selected families/qualifiers |
| `AnyPut` | Cells to write, with optional timestamps |
| `AnyScan` | Row range, columns, filters, scan limits |
| `AnyDelete` | Row, family, or qualifier deletion |
| `AnyAppend` | Append cell values |
| `AnyIncrement` | Increment numeric values |
| `AnyRowMutations` | Group mutations for one row |

Native HBase `Get`, `Put`, `Scan`, and other requests are also accepted.
For entity mapping, use `@Table`, `@Id` for the row key, `@Column` for a qualifier,
and `com.landawn.abacus.da.hbase.annotation.ColumnFamily` for family selection.
A class-level family can be overridden on a property. `mapper(Class)` and its
table/naming-policy overload create typed `HBaseMapper` instances. See the
[HBase integration test](./abacus-da-all/src/test/java/com/landawn/abacus/da/hbase/HBaseExecutorIT.java)
for a complete mapped entity.

Keep byte encoding consistent between writes and reads. The wrappers implement
abacus conversion conventions; they cannot infer arbitrary external encodings.
Scan streams own scanner resources, so use try-with-resources, especially for
early termination. `async()` schedules synchronous work; a future holding a stream
does not eagerly consume that scan.

## Azure Cosmos DB

Source: [CosmosContainerExecutor](./abacus-da-all/src/main/java/com/landawn/abacus/da/azure/CosmosContainerExecutor.java).

Create the database and container through the SDK or your infrastructure tools.
This example assumes `myapp.users` with partition key path `/tenantId`, and
`COSMOS_ENDPOINT`/`COSMOS_KEY` environment variables.

```java
import java.util.Map;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.landawn.abacus.da.azure.CosmosContainerExecutor;

try (CosmosClient client = new CosmosClientBuilder()
        .endpoint(System.getenv("COSMOS_ENDPOINT"))
        .key(System.getenv("COSMOS_KEY"))
        .buildClient()) {
    CosmosContainerExecutor users = new CosmosContainerExecutor(
            client.getDatabase("myapp").getContainer("users"));
    PartitionKey tenant = new PartitionKey("tenant-a");

    users.createItem(Map.of("id", "u-1", "tenantId", "tenant-a", "name", "Alice"),
            tenant, new CosmosItemRequestOptions());

    var found = users.get("u-1", tenant, Map.class);
    SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM c WHERE c.name = @name",
            java.util.List.of(new SqlParameter("@name", "Alice")));
    CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setPartitionKey(tenant);
    users.queryItems(query, options, Map.class).forEach(System.out::println);

    users.deleteItem("u-1", tenant, new CosmosItemRequestOptions());
}
```

The partition key is part of item identity. A matching `id` with the wrong
partition key does not identify the intended item.

| Need | API |
| --- | --- |
| Create/overwrite | `createItem`, `upsertItem`, `replaceItem` |
| Partial update | `patchItem` with `CosmosPatchOperations` |
| Point read | `get`/`gett`, `readItem` returning `CosmosItemResponse<T>` |
| Multiple items | `readMany`, `readAllItems`, `streamAllItems` |
| SQL | `queryItems` returning `CosmosPagedIterable<T>`, `streamItems` returning abacus streams |
| Delete | `deleteItem`, `deleteAllItemsByPartitionKey` |
| Native access | `cosmosContainer()` |

Retain native responses for request charge, ETag, diagnostics, and session
metadata. Use request options for query controls and `SqlQuerySpec` for parameters.
`get`/`gett` translate not-found responses to empty optional/null; `readItem`
exposes SDK response/error behavior. This wrapper is synchronous, without `async()`.

## Google BigQuery

Source: [BigQueryExecutor](./abacus-da-all/src/main/java/com/landawn/abacus/da/gcp/BigQueryExecutor.java).

Configure credentials and project selection through the Google SDK. The example
uses an existing `my-project.my_dataset.users` table and the earlier `User` bean;
replace the table identifier with your own.

```java
import java.util.List;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.landawn.abacus.da.gcp.BigQueryExecutor;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.stream.Stream;

BigQueryExecutor db = new BigQueryExecutor(BigQueryOptions.getDefaultInstance().getService());
String sql = "SELECT id, name, email FROM `my-project.my_dataset.users` WHERE id = ?";
List<User> users = db.list(User.class, sql, "u-1");
Dataset table = db.query(User.class, sql, "u-1");

QueryJobConfiguration config = QueryJobConfiguration.newBuilder(
        "SELECT id, name, email FROM `my-project.my_dataset.users` WHERE id = @id")
        .setUseLegacySql(false)
        .addNamedParameter("id", QueryParameterValue.string("u-1"))
        .build();
try (Stream<User> rows = db.stream(User.class, config)) {
    rows.forEach(user -> System.out.println(user.getName()));
}
```

`execute(sql, parameters...)` binds positional `?` values and waits for a query
job. Use `QueryJobConfiguration` with `stream`, or the native `bigQuery()` service,
for named parameters and additional job controls. Streaming avoids collecting
every row into a Java list; query submission/completion still blocks.

| Need | API |
| --- | --- |
| Parameterized SQL | `execute(String, Object...)` returning `TableResult` |
| Typed reads | `list`, `stream`, `query`, scalar query methods |
| Generated entity DML | `insert`, `update`, `delete` |
| Conditions | Overloads accepting abacus-query `Condition` |
| Existing result conversion | Static `toEntity`, `toMap`, `toList`, `extractData` |
| Native access | `bigQuery()` |

Generated entity operations use SQL DML, not streaming inserts. Configure table
metadata and key metadata for generated updates/deletes, and the intended naming
policy. Use explicit `@Table` metadata for fully qualified table names.

Untyped null parameters are rejected. Bind SQL NULL with a typed value such as
`QueryParameterValue.string(null)`. Use explicit SDK parameter types for precise
temporal/structured typing: a raw Java `LocalDate` is not automatically a BigQuery
DATE. Prefer `java.sql.Date` or an explicitly typed DATE parameter.

If interrupted while waiting, `execute` restores the interrupt flag and propagates
a runtime exception. There is no executor `async()`; use native job APIs when
job-level control is needed.

## Neo4j

Source: [Neo4jExecutor](./abacus-da-all/src/main/java/com/landawn/abacus/da/neo4j/Neo4jExecutor.java).

This integration wraps an OGM `SessionFactory`. Add `abacus-da-all`, OGM core,
and an appropriate OGM transport. Configure your connection and the package
containing OGM-annotated entities. With an application-owned
`SessionFactory sessionFactory`:

```java
import java.util.Map;
import com.landawn.abacus.da.neo4j.Neo4jExecutor;
import com.landawn.abacus.util.stream.Stream;

Neo4jExecutor db = new Neo4jExecutor(sessionFactory);
try (Stream<Map<String, Object>> rows = db.stream(
        "MATCH (p:Person) WHERE p.name = $name RETURN p.name AS name",
        Map.of("name", "Alice"))) {
    rows.forEach(System.out::println);
}
```

Use `save`, `load`, `loadAll`, and `delete` for OGM entities. Load overloads
accept filters, sort order, pagination, and relationship depth. `findOnly` returns
a unique-result optional; Cypher streams support map and typed rows.

Neo4j's query streams traverse already-fetched rows: the query runs and results
are materialized before the borrowed session is returned to the pool. A stream
return type here does not imply bounded-memory server-side paging.

For several operations in one transaction, use a session callback and explicitly
manage the OGM transaction:

```java
import org.neo4j.ogm.transaction.Transaction;

// personA and personB are application-defined OGM entities.
db.run(session -> {
    try (Transaction tx = session.beginTransaction()) {
        session.save(personA);
        session.save(personB);
        tx.commit();
    }
});
```

`run` and `call` are marked `@Beta`. Borrowed sessions are cleared and returned
to an internal pool after the callback. Do not retain the session or return lazy
work depending on it. Returned entities are detached from the cleared session.
Close the owning `SessionFactory` at shutdown. There is no executor `async()`.

## Async and reactive reference

| Integration | Entry point | Execution/result model |
| --- | --- | --- |
| MongoDB sync | `collectionExecutor.async()` | `ContinuableFuture`; synchronous driver calls on `AsyncExecutor` |
| MongoDB reactive | `mongodb.reactivestreams.MongoDB` | Reactor `Mono`/`Flux` over the reactive driver |
| Cassandra 4 | `executor.async()` | `ContinuableFuture` over driver async execution; preparation may block |
| Cassandra 3 | `executor.async()` | `ContinuableFuture` over the driver-3 async result API |
| DynamoDB v1 | `executor.async()` | `ContinuableFuture`; worker-thread facade |
| DynamoDB v2 | `executor.async(asyncClient)` | `CompletableFuture` with `DynamoDbAsyncClient` |
| HBase | `executor.async()` | `ContinuableFuture`; worker-thread facade |
| Cosmos DB, BigQuery, Neo4j | No executor `async()` | Synchronous wrappers; use native client facilities where needed |

While the synchronous MongoDB client remains active:

```java
import java.util.List;
import org.bson.Document;
import com.mongodb.client.model.Filters;
import com.landawn.abacus.util.ContinuableFuture;

ContinuableFuture<List<Document>> future = users.async().list(Filters.gte("age", 18));
List<Document> adults = future.get(); // Blocks this calling thread while waiting.
```

Use continuation/composition methods when the caller should continue working.
For DynamoDB v2, use standard `CompletableFuture` composition. Avoid blocking
waits on event-loop threads. Constructors accepting `AsyncExecutor` allow
application control over scheduling of blocking driver work.

A future of a stream does not mean later iteration is asynchronous. Cassandra
async streams can fetch later pages synchronously; mapping/page failures can
surface during consumption. Keep clients alive through future completion **and**
result consumption, and close returned streams.

Handle immediate validation/preparation failures as well as exceptional future
completion. An async name does not guarantee every stage runs off the caller thread.

## Resource management and operational guidance

### Ownership

| Object | Lifecycle rule |
| --- | --- |
| MongoDB wrappers | Close the application-owned `MongoClient` |
| Cassandra executor | `close()` closes the wrapped session |
| DynamoDB synchronous executor | `close()` shuts down/closes its wrapped v1/v2 client |
| DynamoDB v2 async client | Manage separately from the synchronous executor |
| HBase executor | `close()` releases admin and connection; close handles obtained with `getTable()` yourself |
| Cosmos wrapper | Close the owning `CosmosClient` |
| BigQuery wrapper | Keep the SDK service available while submitting/consuming queries; no executor close method |
| Neo4j wrapper | Close the owning `SessionFactory`; obey callback session boundaries |
| Returned streams | Consume and close within the client/session lifetime |

The Cassandra, DynamoDB, and HBase executors expose `close()` but do not implement
`AutoCloseable`. Use `try/finally` for those wrappers, as shown above. Their native
clients and abacus streams may support try-with-resources independently.

Reuse native clients according to their SDK contracts. The short lifetimes in
examples demonstrate ownership; services commonly keep clients for the whole
application lifetime rather than creating one per request.

### Query size, consistency, and failures

- Select only required columns/attributes. Lists and `Dataset` results consume
  memory proportional to returned data. Use bounded queries or streams for large results.
- Distinguish page size from total result size. Retain continuation tokens or
  paging state for resumable reads and use stable ordering where supported.
- Configure credentials, pools, timeouts, retries, and consistency through native
  clients/request options. There is no cross-service transaction or consistency model.
- Bind user data through parameters or conditions. Allowlist dynamic identifiers,
  projections, and raw query fragments separately.
- Inspect native write responses for conditional outcomes, unprocessed work,
  and diagnostics. Before retrying a write, account for idempotency and the
  possibility that the earlier attempt succeeded.
- Preserve exception causes. Authentication, invalid queries, mapping failures,
  and transient service errors require different remedies.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Class-file version/compiler release error | Use JDK 21+; check `java -version` and `mvn -version` |
| Missing abacus classes | Add `abacus-common` and `abacus-query`; provided dependencies are not supplied to consumers |
| Missing driver/Reactor/OGM classes | Add the dependencies for the selected API, including its transport where needed |
| `NoSuchMethodError` or incompatible requests | Check dependency convergence and driver generations against the POM baseline |
| `Optional`/`Stream` type mismatch | Import the abacus types returned by the method |
| Cassandra query rejected | Check schema, key restrictions, valid CQL, and bind values |
| Wrong generated table/column names | Check `@Table`, `@Column`, keys, and naming policy |
| MongoDB string ID parse failure | ObjectId convenience methods need hexadecimal IDs; use a filter for custom IDs |
| Custom MongoDB codec ignored | The wrapper replaces the supplied database codec registry |
| DynamoDB results exceed request `limit` | Auto-pagination treats it as a page limit; use native responses for manual paging |
| Cosmos item missing | Check both `id` and partition key; inspect `readItem` diagnostics |
| BigQuery null/temporal binding failure | Use a typed `QueryParameterValue` matching the SQL type |
| HBase mapping failure | Check family, qualifier, row key, and byte encoding |
| Reactive MongoDB does no work | Compose/subscribe and keep the client alive through completion |
| Async failure occurs during iteration | A completed future may hold a lazy stream; handle errors during consumption |
| Search/Spark/S3 helper cannot be imported | These are package-private placeholders in this checkout |

## Building and testing

This is a Maven reactor. Production source and tests live under `abacus-da-all`;
individual modules compile selected packages from the same source tree.

| Location | Contents |
| --- | --- |
| `pom.xml` | Version, Java baseline, dependency management, shared plugins |
| `abacus-da-all/src/main/java` | All implementation packages |
| `abacus-da-all/src/test/java` | Unit tests, mapping examples, integration tests |
| Backend module POMs | Package selection and dependencies |
| `docs/` | Supporting material and historical HTML previews |
| `TESTING.md` | Test commands, fixture requirements, organization |

Compile all modules from the repository root:

```shell
mvn -DskipTests compile
```

Install this checkout into your local Maven repository for applications to consume
(including source/Javadoc packaging; skip local artifact signing):

```shell
mvn -DskipTests -Dgpg.skip=true install
```

Run unit tests and generate coverage:

```shell
mvn -pl abacus-da-all clean test jacoco:report
```

Unit tests match `*Test.java` and require no live database or cloud credentials.
Maven can still need network access to resolve dependencies on a fresh machine.
Reports are in `abacus-da-all/target/surefire-reports`; coverage is at
`abacus-da-all/target/site/jacoco/index.html`.

Select a fully qualified test name when driver generations share class names:

```shell
mvn -pl abacus-da-all "-Dtest=com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBMappingTest" test
```

Integration tests match `*IT` and run only with the `integration-tests` profile.
Prepare the disposable fixture in [TESTING.md](./TESTING.md), then select a suite:

```shell
mvn -pl abacus-da-all -Pintegration-tests "-Dit.test=DynamoDBExecutorIT" verify -Dgpg.skip=true
```

Integration suites can create/modify backend data. Omitting the selector runs all
integration suites. The testing guide also documents direct Failsafe invocation
without packaging or rerunning unit tests; include its `verify` goal so failures
fail the build.

## Documentation and related projects

- [Published Javadoc](https://www.javadoc.io/doc/com.landawn.abacus/abacus-da-all): select the version matching your dependency.
- [Implementation source](./abacus-da-all/src/main/java/com/landawn/abacus/da): detailed contracts, overloads, conversions, and examples for this checkout.
- [Tests and fixtures](./abacus-da-all/src/test/java/com/landawn/abacus/da): focused usage examples and backend contracts.
- [Testing guide](./TESTING.md) and [release history](./CHANGES.md).
- [Project wiki](https://github.com/landawn/abacus-da/wiki): supplementary material; compare older examples with your API version.
- [abacus-common](https://github.com/landawn/abacus-common): `Dataset`, streams, futures, mapping, and utilities.
- [abacus-query](https://github.com/landawn/abacus-query): query builders and conditions.
- [abacus-jdbc](https://github.com/landawn/abacus-jdbc): relational JDBC access.

## License

Licensed under the [Apache License 2.0](./LICENSE.txt).
