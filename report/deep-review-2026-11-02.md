# Deep review — 2026-11-02

## Scope and method

Every class under `abacus-da-all/src/main/java` (the shared production source root; the other modules compile subsets
of it), 107K lines in 68 files, reviewed line by line from HEAD `dc6403e`. The dependency bump since the last full round
(abacus-common 8.3.2, abacus-query 4.9.7, Mongo driver 5.14.0, AWS SDK 2.57.8, BigQuery 2.74.0, Cassandra driver 4.19.4,
reactor 3.8.8) was diffed where the code relies on it.

- 21 review slices (one agent each), compared against their twins (v1/v2, v3/v4, sync/async/reactive).
- 3 follow-up fixers for cross-slice issues (dotted paths in BigQuery/Mongo, Cassandra consolidation, collection paths).
- 7 independent verifiers re-read the final cumulative diff per area, with HEAD-vs-tree differentials. They caught
  **3 regressions introduced by this round's own fixes** (all fixed before the final run; see "Verification").
- Live services used: MongoDB, Cassandra 5, DynamoDB Local. Not available: HBase, Neo4j, BigQuery emulator, and the
  Cosmos emulator (503 all session) — those areas were verified offline (Mockito, real SDK paged flux, OGM with a mocked driver).

## Bugs fixed

| Area | Trigger → old behavior | Fix |
|---|---|---|
| **Neo4j** pooled sessions | OGM `Session.clear()` keeps `MappingContext.primaryIdToNativeId` (OGM 5.0.7–5.0.11). Pooled sessions grew it without bound, and a primary-index-only entity reused a stale graph id: `delete(k1)` then `save(new k1)` silently wrote nothing (or overwrote a reused id's node); `delete` removed the stale id without reloading. | The cache is cleared before a session is returned to the pool (reflection, field type-checked); if it cannot be cleared the session is not pooled. |
| **Cosmos** streams | `streamItems`/`streamAllItems` pulled **256 pages** (SDK `flatMap` concurrency) when the stream was created, before any consumption; closing did not cancel. | Streams iterate by page: 0 requests before consumption, one page of read-ahead, close cancels (also for a closed, never-consumed stream). Query-creation errors and failure timing unchanged. |
| **Cosmos** conditions | Properties starting with `$` (`$type`) were left unqualified → invalid SQL; `NOT $type` read NOT as a property. | Qualified as `c["$type"]`; unary NOT recognized. |
| **Dotted paths, copying setters** (DynamoDB, Cassandra v3/v4, BigQuery, MongoDB) | A dotted column/key (`address.city`) into a bean whose setter stores a copy (`@AccessFieldByMethod`) lost the first value per intermediate (`Address(null, 75001)`), because abacus-common's path setter writes into its own detached instance. | Shared rule in each adapter: create/assign the missing intermediate, keep the owner's stored instance only when it reads back stably, write the leaf directly; paths through a collection are left entirely to the path setter (HEAD behavior). |
| **MongoDB** row types | `BsonDocument`/`RawBsonDocument` row types always failed (CCE / cannot instantiate); scalar `BsonString`/`BsonInt32`/`BsonValue` row types failed in `list`/`queryForSingleValue`/`groupBy`/mapper `distinct`. | Rows encoded through the codec registry; a Java value is encoded into the requested BSON value class (mismatch → IAE). |
| **MongoDB** `Number` properties | A property/component/element declared `Number` holding a BigInteger/AtomicLong (stored as text) made the whole document unreadable (UOE), including documents written before 10-27. | Text is parsed for exactly-`Number` targets. Also: embedded document requested as `Binary`/`Code` → IAE instead of NPE. |
| **HBase** versioned properties | An `ArrayBlockingQueue<HBaseColumn<..>>` property (top-level or nested) could be written but every read failed. | Versions buffered and the queue built once with capacity = versions read. |
| **Cassandra v3/v4** rows | `ArrayBlockingQueue` row class always failed; a NULL column/UDT field read into `ConcurrentHashMap`/`Hashtable` gave a bare NPE. | Bounded queue sized to the row; IAE naming the column/field. |
| **DynamoDB v1/v2** rows | A NULL attribute read into a null-rejecting map row (`ConcurrentHashMap`) gave a bare NPE. | IAE naming the attribute (shared `fillMapRow`). |
| **Cassandra** key guard | `update(entity, ["account_id"])`/`["ACCOUNTID"]` (other spellings of a key property) built `SET <key>` and failed only on the server (whole batch). | Names resolved like the builder resolves them; key properties rejected up front with the documented IAE. |
| **CqlBuilder** | A `build()` rejected for an incomplete JOIN left ` ALLOW FILTERING` in the buffer; the retry rendered it mid-statement and again at the end. | Clause removed again when the parent rejects the build. |
| **BigQuery** | A `java.util.Calendar` parameter/property bound as STRING (`…-08:00[America/Los_Angeles]`), so a TIMESTAMP read into a Calendar could not be written back. | Binds as TIMESTAMP like `Date`. |

## Performance and memory

- Cosmos streaming: see above (largest win: RU and memory).
- HBase `toEntity` per-cell loop: single-version dedupe keyed by family → qualifier (no concatenated String per cell),
  family String reused for byte-equal consecutive families. 30,000-case HEAD differential byte-identical; −11% to −30%
  bytes per row depending on the row shape.
- CqlBuilder: the reserved-keyword check for the full select list is cached per (naming policy, entity class)
  (`selectFrom` ≈ 3.4 µs → 2.1 µs on a 20-column entity); keyword detection allocates nothing for names with `_`/digits.
  19,000-case CQL differential identical to HEAD apart from the ALLOW FILTERING fix.
- MongoDB `toEntity`: values are normalized inside the private row copy instead of copying the row again.
- Neo4j: pooled sessions no longer leak the primary-index cache.

## Javadoc

About 150 corrections and additions, among them: DynamoDB `updateItem` with null/empty updates succeeds (verified live
on both SDKs); values written as `S` text (Atomic*, wrapped numbers, SdkBytes); retry *strategy* examples for SDK 2.57;
BigQuery DML row counts come from `getNumDmlAffectedRows()` (`getTotalRows()` can be 0), parameter binding rules;
Cassandra "Row Limits" and "Map Rows" sections, the `count … LIMIT` comment, v3 async I/O-thread paging; Cosmos
partition keys persisted into reused options (a reused `deleteItem(item, opts)` can hit the earlier partition) and
read-ahead; Mongo row-shape rules for `Object[]`/`Collection` rows (sync `list` vs `findFirst`/`stream`), async
`selectPropNames` timing; HBase async pool sizing (never grows past core size), scan prefetch/caching defaults;
Neo4j nested executor calls inside `run`/`call`, MERGE on save; CqlMapper XML rules. All CqlBuilder examples were executed.

## Tests

27 new test files (`*Review20261102Test`, plus `CosmosPagedTestSupport`), 160+ test methods. Every behavior fix has
tests that fail on HEAD (or, for verifier-caught regressions, on the pre-fix tree); doc corrections are pinned by
tests that pass on both. 31 Cosmos mocks were moved from `stream()` to `streamByPage()` without changing assertions.

## Verification

Verifiers found and fixed:
1. **Copying getters regressed** by the first dotted-path fix (a getter returning a defensive copy lost values HEAD
   kept) — fixed in all four adapters with guard tests; then a residual case (copying getter before a collection
   intermediate) closed by leaving such paths to the path setter.
2. **Mongo `BsonInt32` → `BsonInt64`** conversions started throwing after the BsonValue-encoding fix — restored.
3. **Cosmos never-consumed streams** still fetched a page when closed — fixed.
Plus a race in one new async test and several doc wordings.

| Check | Result |
|---|---|
| Baseline, clean HEAD `dc6403e` (JUnit launcher, every `*Test` class, live Mongo/Cassandra/DynamoDB up) | 5,646 found / 5,646 passed / 0 failed / 0 aborted |
| Final tree, same runner and environment | **5,808 found / 5,808 passed / 0 failed / 0 aborted** (+162 tests) |
| All reactor modules (`mvn -o compile`) | OK |
| Doclint (`all,-missing`) over every main source | 0 real findings (44 known Lombok `StatementSettings` false positives under `-proc:none`) |

Not run: `CosmosContainerExecutorIT` (emulator returned 503 all session), and anything needing HBase, Neo4j or BigQuery servers.

## Left for a decision (proposed, not applied)

1. **Neo4j: drop session pooling** (fresh session per call). OGM sessions are cheap; this removes the reflection and the
   stale-configuration caveat. Touches the open "Neo4j pooling" decision.
2. **Copying setter on a path through a collection** (`book.addresses.city`) still loses data, as on HEAD. Fix would
   need the helper to handle the collection step itself.
3. **MongoDB sync `list` with `Object[]`/`Collection` rows** reads the sole non-`_id` field; every other path (and all
   reactive paths) is positional. Unify? (2026-09-27 decision; now documented on every method.)
4. **DynamoDB v2 `SdkBytes`/`AttributeValue` values** are written as their `toString()` text (garbage S keys, filters
   that never match, unreadable bean properties). Writing `fromB`/pass-through changes the stored format.
5. **BigQuery binding**: collections/beans as `QueryParameterValue.array/struct` instead of JSON; unwrap present
   Optional/Nullable; native `ZonedDateTime`/`LocalDateTime`; `BigInteger`/`AtomicLong` as INT64/BIGNUMERIC.
6. **Cassandra/Cosmos**: honor a condition's own LIMIT in `get`/`findFirst`/`exists`/`queryFor*` instead of throwing
   ISE (but a user LIMIT 1 would hide `DuplicateResultException`).
7. **Cosmos**: clearing a stale partition key from reused options needs an internal SDK API.
8. **HBase**: wrap hbase-client 2.6.4's `setQueryMetricsEnabled`/`setEnableScanMetricsByRegion` (provided-scope risk on older runtimes).
9. **CqlMapper.add** attribute validation like abacus-query 4.9.7 `SqlMapper` (exception moves from save to add time).
10. **CqlBuilder**: raw DSLs render value wrappers as quoted text; `usingTTL` accepts values above Cassandra's maximum.
11. **Upstream abacus-common**: `BeanInfo.setPropValue(obj, path, value, true)` should re-read after assigning a new
    intermediate (root cause of the dotted-path workarounds); `N.convert(String, Number.class)` throws UOE.
12. Minor: MongoDB `Number` text uses `Numbers.createNumber` grammar ("010" → 8); Cassandra `Row.class` in
    `findFirst`/`gett` vs `list`; BigQuery code comments that still say DML counts come from `getTotalRows()`.
