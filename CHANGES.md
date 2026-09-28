## 2.8.9
* Improvements and bug fixes
  * CqlBuilder: `ALLOW FILTERING` is rendered last regardless of call order; `UUID` values are inlined as bare literals by the raw builders; primary-key properties are excluded from the implicit `SET` list of `update(Class)`/`set(entity)`.
  * CassandraExecutor (v3/v4): bean parameters bound to `@Column`-named variables; v3 `UDTCodec` reads UDT fields by position so case-sensitive field names work.
  * CassandraExecutorBase: `registerKeys` reports an unknown key property with a clear `IllegalArgumentException`.
  * MongoDBBase.toList: non-`Document` map rows map to entities; scalar rows of a different Java type are converted per row.
  * MongoCollectionExecutor (sync and reactive): typed `aggregate`/`mapReduce`/`findOneAndXxx` return the raw document for `Object`/`Bson` result types.
  * DynamoDBExecutor (v1): `scan` with an empty `attributesToGet` list retrieves all attributes instead of sending an invalid request.
  * Javadoc corrections across HBase, Cassandra, MongoDB, DynamoDB, BigQuery and Neo4j executors.
  * abacus-query 4.9.4: CqlBuilder renders column deletes (`delete(cols).from(...)`) itself, because the parent `from(...)` now rejects `DELETE`; `delete(cols)` without `from(...)` is rejected instead of rendering `DELETE FROM null`; `from(Class, alias)`/`from(String, Class)`/`onlyIf(String)` are atomic on failure.
  * ParsedCql: bind markers inside list literals and subscripts (`[?, ?]`, `l[?]`, `m[:k]`) are counted/rewritten, a `]` inside a string in a list no longer swallows the rest of the statement, and `?-` no longer hides a marker. `CqlMapper.saveTo` validates XML characters before truncating the target file.
  * CassandraExecutor (v3/v4): a named single map-column marker bound from a `Map` keyed by its name binds the named value; typed-array row targets (`String[]`, `Long[]`) convert column values; async `memoize` also caches `Error`s.
  * DynamoDBExecutor (v1/v2): `String[]`/`Object[]` properties round-trip through their JSON-array `S` attribute.
  * MongoDB: `groupBy`/`distinct` on a dotted field into a single-value type return the values (sync + reactive executors and mappers); `MongoDBBase.toList` converts every scalar row, not only when the first row differs.
  * HBaseExecutor: cells mapped to read-only (getter-only) properties are skipped on read instead of failing the whole entity.
  * BigQueryExecutor: `queryForSingleValue`/`queryForSingleNonNull` decode a STRUCT cell into a `List`/`Collection` target again (abacus-common 8.1.0 `N.convert` no longer applies the registered converter there).

## 2.8.8
* Naming convention improvements
* Improvements and bug fixes

## 2.8.6
* Naming convention improvements
* Improvements and bug fixes

## 2.8.5
* Naming convention improvements
* Improvements and bug fixes

## 2.8.4
* Naming convention improvements
* Improvements and bug fixes

## 2.8.3
* Naming convention improvements
* Improvements and bug fixes

## 2.8.2
* Naming convention improvements
* Improvements and bug fixes

## 2.8.1
* Naming convention improvements
* Improvements and bug fixes

## 2.8.0
* Naming convention improvements
* Improvements and bug fixes

## 2.7.7
* Naming convention improvements
* Improvements and bug fixes

## 2.7.6
* Naming convention improvements
* Improvements and bug fixes

## 2.7.3
* Naming convention improvements
* Improvements and bug fixes

## 2.7.2
* Change <groupId>com.landawn</groupId> to <groupId>com.landawn.abacus</groupId>
* Naming convention improvements
* Improvements and bug fixes

### 2.0
* Improvements and bug fix.

### 1.9.29

* Improvements and bug fix.


### 1.9.28

* Improvements and bug fix.


### 1.9.27

* Improvements and bug fix.


### 1.9.26

* Improvements and bug fix.


### 1.9.25

* Improvements and bug fix.


### 1.9.24

* Refactoring `CqlBuilder`.
* Improvements and bug fix.


### 1.9.23

* Refactoring `CqlBuilder`.
* Improvements and bug fix.


### 1.9.22

* Refactoring `CqlBuilder`.
* Improvements and bug fix.


### 1.9.21

* Improvements and bug fix.


### 1.9.20

* Improvements and bug fix.


### 1.9.19

* Improvements and bug fix.


### 1.9.18

* Improvements and bug fix.


### 1.9.17

* Improvements and bug fix.


### 1.9.16

* Support default `@ColumnFamily` on class.
* Improvements and bug fix.


### 1.9.15

* Improvements and bug fix.


### 1.9.14

* Add `HBaseMapper`.
* Add `DynamoDBExecutor.Mapper`.
* Improvements and bug fix.


### 1.9.13

* Hide the constructors of `AnyGet/AnyPut/AnyDelete/AnyScan/...` because the static creator method `XXX.of(...)` is preferred.
* Add `AnyAppend/AnyIncrement`.
* Rename `NamedCQL` to `ParsedCql`.
* Improvements and bug fix.


### 1.9.11

* Improvements and bug fix.


### 1.9.10

* Rename `NamedSQL` to `ParsedSql`.
* Improvements and bug fix.


### 1.9.9
 
* Move the utility classes to the new project: https://github.com/landawn/abacus-extra
* Improvements and bug fix.


### 1.9.8

* Move functional interfaces from `Try` to `Throwables`.
* Improvements and bug fix.


### 1.9.7

* Improvements and bug fix.


### 1.9.6

* Improvements and bug fix.


### 1.9.5

* Improvements and bug fix.


### 1.9.4

* Improvements and bug fix.


### 1.9.3

* Improvements and bug fix.


### 1.9.2

* Improvements and bug fix.


### 1.9.1

* Introduce new project: https://github.com/landawn/abacus-jdbc


### 1.9.0

* Introduce new project: https://github.com/landawn/abacus-jdbc


### 0.9.6

* Improve Java Docs.


### 0.9.5

* Improve Java Docs.


### 0.9.4

* Improve Java Docs.


### 0.9.3

* Improvements and bug fix.


### 0.9.2

* Improvements and bug fix.


### 0.9.1

* Improvements and bug fix.


### 0.9

* Add `f/Points`.
* re-organize the package.


### 0.8.3

* Update docs.


### 0.8.2

* Add `Matrix/Sheet`.


### 0.8.1

* Add `RemoteExecutor`.


### 0.8

* First release.
