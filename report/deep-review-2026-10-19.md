# Source review — 2026-10-19

## Scope and method

Line-by-line review of every file under `abacus-da-all/src/main/java` (69 files, ~98k lines) on HEAD `cf1e2bd`, for bug
fixes, Javadoc, performance and memory. 23 reviewers owned disjoint slices (DynamoDB v1/base/v2/async, Cosmos, Cassandra
base/async/v4/v3, CqlBuilder, ParsedCql/CqlMapper/cs, BigQuery, HBase ×4, MongoDB sync/async/mapper/base/reactive ×2,
Neo4j + placeholders). One extra agent surveyed the dependency bump that landed with HEAD (abacus-common 8.1.0 → 8.2.1,
abacus-query 4.9.4 → 4.9.5), which no earlier round had reviewed. Cross-slice findings were relayed so sibling files
(v1/v2, v3/v4, sync/async/reactive) got the same fix. Six independent verifiers (none of them authors) then re-reviewed
the whole diff with HEAD-vs-tree differentials and live probes (Cassandra, MongoDB, DynamoDB Local).

Every code fix has a regression test that fails on HEAD and passes on the tree; performance/memory changes are covered by
existing or new coverage tests.

## Verification

- Baseline (clean HEAD, all `*Test` classes, JDK 21): **4,541 tests, 0 failures**.
- Final tree (after the follow-up below): **4,634 tests, 0 failures, 0 aborted** (+93 tests; new classes `DynamoDBReadOnlyPropertyMappingTest`, test helper `MongoIterableStubs`).
- Doclint (`-Xdoclint:all,-missing`) clean on every changed main file; `git diff --check` clean.

## Bugs fixed

### Caused by the dependency bump
- **Read-only properties (abacus-common 8.2, P2)** — `PropInfo.isReadOnlyProperty` became public and now also covers a
  field-backed getter-only property of an `@AccessFieldByMethod` bean, for which `setPropValue` throws
  `UnsupportedOperationException`. The local heuristic `field == null && SERIALIZE_ONLY` missed it, so one such column lost
  the whole row/item on read. Fixed in `HBaseExecutor`, Cassandra v4/v3 (`toEntity`, `toList`, UDT codecs),
  `DynamoDBExecutorBase.toEntity` (v1+v2), `MongoDBBase.normalizeDecodedProperties`. HBase keeps filling a read-only nested
  bean held by a field in place (verifier caught that the first HBase fix silently dropped those cells).
- **CqlBuilder raw date literals (abacus-query 4.9.5, P2)** — `Date`/`Timestamp`/`Calendar` were inlined as zone-less JVM-local
  time; Cassandra stored a shifted instant (8 h off from a UTC−8 JVM, proven live). Now rendered as ISO-8601 UTC instants
  (ms precision); raw `ZonedDateTime` (previously unparsable by Cassandra) too. `java.sql.Date`/`Time` unchanged.
- **Cosmos SCREAMING_SNAKE_CASE bracket accessor (4.9.5, P2)** — `c['x']` rendered as `c.C['x']` (matches nothing).

### Other bugs
- **MongoDB grouping (P2)** — multi-field / dotted `groupBy`/`groupByAndCount` put documents missing a field and documents
  with that field `null` in different groups (and returned misaligned `Object[]`/`List` rows). Group leaves are now
  `{$ifNull: ["$f", null]}`; verified live in sync, async, reactive executors and both mappers. Document group keys now
  contain `a: null` where they used to omit `a`.
- **MongoDB `extractData` bean target (P2, regression from 10-18)** — positional/scalar rows threw "Not a bean class".
- **Async MongoDB stream cancel (P3)** — after `cancel(true)` the interrupted worker's cursor close skipped `killCursors`,
  leaving the server cursor open until its idle timeout (proven live). Interrupt is cleared around the close and restored.
- **ParsedCql (P2)** — a native `:name` inside `(...)` within braces (`{loc: (:lat, :lon)}`) was not rewritten/bound;
  parsing failed during JVM shutdown (pool closed by its own hook, or class init during shutdown).
- **Cassandra v3/v4 prepared-statement cache (P3)** — closed-at-shutdown pool made CQL from shutdown hooks fail; now a miss.
- **Cassandra v3/v4 sync `findFirst`/`fetchOnlyOne` (P3)** — NULL column into a primitive target threw NPE while every
  other path returned the primitive default.
- **Cassandra `UDTCodec.format` (P2 v3 / P3 v4)** — `UDTValue.toString()` threw `ClassCastException` once a bean UDT codec
  was registered (pre-existing; found by a verifier, proven live).
- **DynamoDB v1/v2 (P3)** — NaN/Infinity in `MutableDouble`/`MutableFloat`/`DoubleAdder` were sent as `N` values instead of
  being rejected with the documented `IllegalArgumentException`.
- **BigQuery `extractData` (P3)** — a REPEATED column mapped to a non-container property kept the raw `ArrayList`; it is now
  converted like `toEntity` (so `query(Class, …)` now throws where `toEntity` already threw).
- **Messages (P3)** — Cassandra `update`/`batchUpdate` (sync+async) blamed `propNamesToUpdate` for an entity with no
  updatable property; BigQuery scalar field-count message.

## Performance / memory
- `RowDataset.fromOwnedColumns` instead of the column-copying constructor in DynamoDB v1/v2, Cassandra v3/v4, BigQuery
  `extractData` (result no longer held twice). Note: the method is `@Internal` in abacus-common.
- Mongo sync+reactive dotted-column queries fill the Dataset in place instead of `Dataset.columns(...)` (another full copy).
- DynamoDB: one row mapper + direct page conversion in paginated `list`, per-policy attribute-name cache (~110 → 5 ns),
  pre-sized maps/lists, copy-free binary key check. Cassandra: UDT codec field/PropInfo resolution once per codec (v3/v4),
  `PagedResultSet.all()` pre-sized. BigQuery: primitive columns no longer always go through `N.convert`.

## Javadoc
~120 corrections, all verified against code/driver behavior: generated `_id` write-back rules (Mongo sync/async/reactive
executors and mappers), `$`-field replacement IAE, reactive `distinct` dropping nulls, HBase null-family/default-timestamp/
copy-vs-reference/live-map/`@throws` details, async stream failure timing, DynamoDB/BigQuery read-only property rules,
4.9.5 backslash-escape naming limitation (Cosmos, BigQuery), Neo4j `loadAll` returning new instances, broken links.

## Follow-up applied after maintainer decisions (2026-10-20)

- **Strict Boolean conversion — documented, not overridden.** Class-level "Boolean Conversion" notes in `CassandraExecutorBase`,
  `AsyncCassandraExecutorBase`, Cassandra v4/v3, `DynamoDBExecutorBase`, `BigQueryExecutor`, `HBaseExecutor`, `MongoDBBase`, and
  `@throws`/`@return` text on every `queryForBoolean` (sync throws; async fails the future; reactive signals the error). Cassandra
  bind values rejected by the rule are bound unchanged and fail with the driver's `CodecNotFoundException`. Cassandra NULL→default
  wording also corrected (~20 sites: `Optional`-like targets get an empty value, not `null`).
- **`RowDataset.fromOwnedColumns`** use accepted as is.
- **CqlBuilder rejects SQL-only clauses**: `limit(count, offset)`, `offset`, `offsetRows`, `fetchFirstRows`, `fetchNextRows`
  and both `having` overloads throw `UnsupportedOperationException`; a condition passed to `append`/`appendIf`/
  `appendIfOrElse`/`renderCondition` that carries HAVING, a row-skipping LIMIT or a verbatim `FETCH … ROWS ONLY` throws
  `IllegalArgumentException`, matching what `where(...)` throws for the same condition (plain `limit(n)` unchanged).
- **CqlBuilder raw literals**: `byte[]`/`ByteBuffer` → `0x…` blob literals (buffer position unchanged), `InetAddress` →
  `'host-address'` with any IPv6 zone dropped (verified against live Cassandra).
- **DynamoDB v1** returns fresh mutable copies of `SS`/`NS`/`BS` lists and empty `L`/`M` (matches v2).
- **Neo4j**: after a failed rollback the discarded session is cleared and its transaction detached from OGM's ThreadLocal
  (public `TransactionManager.close(tx, no-op)`; never commits/rolls back); cleanup failures are suppressed onto the logged error.
- **MongoDB `toList` / single-field `list` stream conversion** instead of materializing all documents first: peak heap at 50k
  documents bean 33.6→16.4 MB, map 48.4→28.0 MB, scalar 15.7→3.1 MB; results/exceptions/cursor closing identical (~660-case
  stub + live differential).
- Two further independent verification passes found and fixed one gap (scoped IPv6 zone) and no regressions.

## External review follow-up (2026-10-20)

- **BigQuery `extractData`**: a REPEATED column mapped to a read-only property is left unconverted again (as `toEntity` skips
  it); this round's conversion had made `query(Class, …)` throw for it.
- **MongoDB `extractData`**, Map target with a Map first row: positional and scalar rows pass through when `selectPropNames`
  is given; when column names are inferred from map keys such a row gets a clear `IllegalArgumentException` (was "Not a
  bean class"). Javadoc corrected on all five overloads.
- **Cassandra v3 `UDTCodec`**: field names now come from the UDT's positional field iteration for the bean cache and Map
  decode, so they always match the positions used to read and write values (v4 already index-ordered).
- **HBase**: test for a read-only nested bean whose field holds `null` (family skipped, row still read).
- **Neo4j**: `releaseDiscardedSession` Javadoc narrowed to "never throws a `RuntimeException`" (errors deliberately not caught).

## Decisions left for the maintainer
1. ~~Strict Boolean conversion~~ — decided: document (done).
2. ~~`RowDataset.fromOwnedColumns`~~ — decided: accepted.
3. ~~Read-only rule edge~~ — decided: unified. Cassandra v3/v4 (`toEntity`, `toList`, UDT codecs) and BigQuery
   (`toEntity`, `extractData`) now skip a read-only property whatever its direction, as HBase, DynamoDB and MongoDB do; an
   explicit `DESERIALIZE_ONLY` on an `@AccessFieldByMethod` field without a setter used to fail the whole row with
   `UnsupportedOperationException`. (The external review's "setter-backed `SERIALIZE_ONLY` property now written on read"
   shape cannot be declared: `@JsonXmlField` targets fields only, so it was no behavior change.)
4. Proposed, not applied: Condition with its own LIMIT + executor LIMIT → ISE, `count` capped;
   DynamoDB `AtomicInteger`/`AtomicLong` stored as `S` (document); v1 DynamoDB binary `B`/`BS` ByteBuffers still shared with the item; quoted
   `:"Q"` CQL marker names keep quotes; HBase null column family → IAE instead of NPE at request build; BigQuery dialect
   switch for backslash escapes; dates beyond year 9999 in raw CQL.

## Re-review fix round (2026-10-20)

A line-by-line re-review of the whole uncommitted diff (plus five independent reviewers) found no functional regression, but
several incomplete siblings of fixes, overstated Javadoc and coverage gaps. All of them were then fixed; full tree afterwards:
**4,710 tests, 0 failures, 0 aborted** (+76), doclint clean on every changed main file, `git diff --check` clean.

- **CqlBuilder raw literals**: `Instant`, `OffsetDateTime` (as their instant, UTC, millisecond precision) and `LocalDateTime`
  (the instant it denotes in the JVM default zone, like a bound value) join `Date`/`Calendar`/`ZonedDateTime`; the parent
  rendered `LocalDateTime` as zone-less text that Cassandra reads in the coordinator's zone (a shifted instant), and
  `Instant`/`OffsetDateTime` with a nanosecond fraction (Cassandra 5.0 accepts and truncates it, verified live: consistency
  only, not a failure). Class doc no longer claims every SQL-only
  clause fails fast (JOIN still renders; Cassandra rejects it at execution).
- **Cassandra v4 java.time binding/reading (new, found while fixing the above)**: binding a `LocalDateTime`/`ZonedDateTime`
  to a TIMESTAMP column, and reading a TIMESTAMP into a `LocalDateTime` target or property (also UDT fields), failed with
  `DateTimeParseException`/`CodecNotFoundException` because abacus-common 8.2.1 converts those pairs through `toString()`.
  Converted explicitly now (JVM default zone for `LocalDateTime`, matching driver 3's `Date` conversion).
- **Cassandra `UDTCodec`**: a raw driver `UdtValue`/`UDTValue` is passed through by `encode`/`serialize(T, ProtocolVersion)`
  as `format` already did (was `ClassCastException`). v4 prepared-statement cache rethrows an `IllegalStateException`
  unless the pool is closed (as v3).
- **BigQuery `extractData`**: a scalar column of a property `toEntity` ignores (read-only + SERIALIZE_ONLY) keeps its raw
  cell like an unmatched column, decided once per column (was `NumberFormatException` where `toEntity` succeeded).
- **DynamoDB v2 async `list(QueryRequest, Class)`**: one row mapper and direct page appends, shared `addRows` with the sync
  executor.
- **MongoDB `extractData`**: `Binary`, `Code`, `Symbol` and `BsonValue` rows (bean classes to `Beans.isBeanClass`) are read
  as scalars into the single selected column instead of as empty beans / `{type, data}` maps.
- **Neo4j**: the rollback-failure warning is logged even when the session release throws an `Error`, which then carries
  the rollback failure as suppressed.
- **Javadoc**: strict-Boolean paragraphs now state the real 8.2.1 rule per path (padding stripped; blank text is `false`
  on the `N.convert`/`valueOf(Object)` path, the property default on the `valueOf(String)` path used by HBase cells and
  BigQuery bean properties); HBase `toEntity` read-only carve-out narrowed to flattened nested beans; Mongo group-key
  shape (`a: null` for a missing field) on every public `groupBy`/`groupByAndCount` overload; v4 `fetchOnlyOne` NULL
  note; `AnyPut` batch "empty family" qualified to the default `KeyValueCodec`; v2 DynamoDB `ClassCastException` (non-String
  map key) in the method bodies; DynamoDB DESERIALIZE_ONLY read-only rule.
- **Tests added** (all offline except the two live-Mongo group-key classes, which skip without a server): Cassandra Boolean
  bind contract, NULL-default on every entry point, Dataset ownership, blob literal array offset, java.time literals in
  INSERT/UPDATE/IN, UDT raw value encode, closed-vs-open cache, timestamp bind/read; DynamoDB DESERIALIZE_ONLY and
  SERIALIZE_ONLY-with-setter (v1+v2), v2 Dataset mutability, null/empty items, async paging equivalence; BigQuery scalar
  read-only column; HBase SERIALIZE_ONLY/DESERIALIZE_ONLY, `@Column` read-only nested bean, strict Boolean cells; Mongo BSON
  value rows, server-side `$ifNull` grouping (sync, async, reactive, mappers); Neo4j suppressed/Error/non-current-tx paths.
- Maintainer decisions on the two leftovers: DynamoDB v1 `toList`/`extractData` keep their behaviour for a `null` item in a
  hand-built `QueryResult`/`ScanResult` (the service never returns one); the Javadoc now states items must be non-null maps.
  Cassandra v4 now reads a TIMESTAMP into `ZonedDateTime`/`OffsetDateTime` targets and properties in the JVM default zone
  (one rule for every java.time target, as v3 and the `LocalDateTime` read) instead of the accidental UTC offset that came
  from `N.convert` parsing `Instant.toString()`; `withZoneSameInstant(UTC)` gives the old shape.

## Second re-review fix round (2026-10-21)

A second line-by-line review of the whole diff (five independent reviewers plus library probes) found no bug, two
incomplete sibling fixes and coverage gaps; all applied. Full tree afterwards: **4,829 tests, 0 failures, 0 aborted**
(+117), doclint clean on every changed main file, `git diff --check` clean.

- **Cassandra UDT TIMESTAMP fields**: v3 `setUdtField` gained the TIMESTAMP fallback (java.time values convert to `Date` as a
  bound value does; was `CodecNotFoundException`); v4's fallback now converts any value `convertValue` can turn into an
  `Instant` (`Date`, `Timestamp`, `Calendar`, `Long`, parseable text), rethrowing the driver exception with the conversion
  failure suppressed otherwise.
- **CqlBuilder fail-fast policy completed**: `forUpdate()`, all 15 set-operation methods (`union*`, `intersect*`,
  `except*`, `minus*`) and the `groupBy(...)` overloads taking a `SortDirection` throw `UnsupportedOperationException`;
  the matching conditions (set-operation clauses, `Criteria` carrying them, `GROUP BY ... DESC`, FOR UPDATE) passed to
  `append`/`appendIf`/`appendIfOrElse`/`renderCondition` throw `IllegalArgumentException`. Class doc lists exactly what is
  rejected and what still renders (plain GROUP BY, LIMIT n, ORDER BY direction, JOIN).
- **DynamoDB `list(QueryRequest, Class)`** (v1, v2, v2 async) tolerates a `null` client response again (empty list, no
  further page), as the former `toList` path did.
- **Javadoc**: v4/v3 async `queryForSingleValue`/`queryForSingleNonNull` NULL-to-default wording (Optional-like targets get
  an empty value); UDT codec raw-value pass-through described as reachable only through caller `codecFor(DataType)` lookups
  (the driver never hands one over), subclass bypass noted, `getFieldNames()` order wording, `withOffsetSameInstant` for
  `OffsetDateTime`; DynamoDB Boolean rule also covers JSON number tokens in S text, DESERIALIZE_ONLY wording, v1 Map-key
  `ClassCastException` sentences, v1 null-item `@param` text matching the exact behaviour; Cosmos SCREAMING_SNAKE_CASE
  bracket accessor inside an `Filters.expr` subquery documented as a limitation (use `Filters.subQuery`); BigQuery
  read-only column keeps STRUCT cells as `Object[]`; Mongo Boolean paragraph (BSON has no int16/int8); `AnyPut` dash.
- **Tests added** (every documented behaviour above now pinned): strict-Boolean READ path for Cassandra (sync + async,
  v3 + v4), DynamoDB (every mapping context, v1 + v2), BigQuery (bean and single-value paths) and Mongo (sync, async,
  reactive, mappers, `toEntity`); v3 open-pool ISE propagation, v3 UDT `@Column` + codec reuse, DESERIALIZE_ONLY skip,
  v4 UDT TIMESTAMP into `OffsetDateTime`; KEBAB_CASE Dsl, `appendIfOrElse` true branch, ParsedCql `{a: f(:x), b: :y}`;
  v1 `toItem`/`toUpdateItem` non-String key, v1 null item, null response; Cosmos function-argument accessor and the
  subquery limitation; `AnyPut`/`AnyGet`/`AnyScan` null-family and empty-family at request build, `AnyPut` null attribute
  name, prefix-scan bounds, `setScanMetricsEnabled(false)`, `AnyAppend`/`AnyIncrement` row copy; Neo4j Error from the
  detach; Mongo generated `_id` write-back (live), `$`-replacement rejection (live), reactive vs sync `distinct` null
  (live), reactive dotted-column fill, `extractData` `Object[]` target with a BSON value row.

## Follow-up on the maintainer's residual findings (2026-10-21)

Full tree afterwards: **4,846 tests, 0 failures, 0 aborted**; doclint clean on the changed files; `git diff --check` clean.

- **Neo4j**: every session-borrowing operation now runs through `withSession`/`runWithSession` with try-with-resources
  semantics: once the operation has thrown, nothing from the session return (an `Error` from the discard path included)
  can replace it; the cleanup failure is attached as suppressed. An `Error` from the return still propagates when the
  operation completed normally. 12 tests (run/call/load, Error from clear or detach, real OGM transaction left open).
- **DynamoDB** `list(QueryRequest, Class)` (v1, v2, v2 async): a `null` LATER page ends the pagination with the rows
  gathered so far, as a null first page already did; Javadoc says so. The `query(...)` Map branches and `stream`/`scan`
  iterators never tolerated a null first page, so they were left as they are.
- **CqlBuilder raw timestamp literals**: the quoted ISO form is used only for UTC years 0001–9999 (live Cassandra 5.0
  rejects year 0000 as well as signed years); any other instant renders the bare epoch-millisecond integer literal, which
  Cassandra accepts for every representable value (verified live, including `Long.MIN_VALUE`/`MAX_VALUE`). `Instant.MIN`/
  `MAX`, whose millis overflow a `long`, throw `IllegalArgumentException`. `hasSortDirection` treats any Unicode whitespace
  or space character (no-break spaces included) as a word separator, so `GROUP BY a<newline>DESC` is rejected too.
