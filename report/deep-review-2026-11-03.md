# Deep review — 2026-11-03

## Scope and method

Every class under `abacus-da-all/src/main/java` (the shared production source root; the other modules compile subsets
of it), 108K lines in 68 files, reviewed line by line from HEAD `509af62`. Dependencies are unchanged since the
2026-11-02 round (abacus-common 8.3.2, abacus-query 4.9.7, Mongo driver 5.14.0, AWS SDK 2.57.8, Cassandra driver
4.19.4/3.11.5, azure-cosmos 4.83.0, BigQuery 2.74.0, hbase-client 2.6.7, neo4j-ogm 5.0.11), so the fresh focus was the
previous round's own fixes (`509af62`), which had only had one read-only re-review.

- 23 review slices (one agent per slice, at most 8 at a time). Each agent compared its files with their twins
  (v1/v2, v3/v4, sync/async/reactive, executor/mapper).
- Cross-slice items were routed to the owning slice, or done by the main agent: removing a duplicated helper and a
  redundant workaround, mirroring twin docs.
- 7 independent verifiers re-read the final cumulative diff per area. Most used HEAD-vs-tree differentials (hundreds of
  thousands of generated cases in total). They caught **3 regressions introduced by this round's own fixes**, all fixed
  before the final run (see "Verification").
- Live services: MongoDB was up at the start, then stopped mid-round. Cassandra 5, DynamoDB Local and the Cosmos
  emulator were down for about two hours (Docker stopped) and then back. Everything else was verified offline. The
  Cassandra parser oracle was built from cassandra-all 5.0. The Cosmos read-ahead was measured with the real SDK against
  a fake local gateway.

## Bugs fixed

| Area | Trigger → old behavior | Fix |
|---|---|---|
| **HBase** nested beans | A nested-bean property whose getter returns a new instance per call (a computed view, or a copying getter under `@AccessFieldByMethod`) lost **every** cell. Each cell was written into a throwaway copy, also in an `AnyPut.create` → `toEntity` round trip. | The held instance is reused for all cells of the row and assigned back through the setter once, only when the property no longer returns it. Plain getters are unchanged (no setter call, identity kept). |
| **HBase** generic row key | A row key declared by a generic superclass (`Base<K>{@Id K id}`, `Entity extends Base<Long>`) was decoded as the erased setter type, so a String sat in a Long-bound property (CCE on use). With a bridge setter, every row failed. | Decoded as `PropInfo.type` when it narrows the setter's type. |
| **HBase** generic nested beans | `Pair<String,Integer>`, `Holder<List<T>>`, record `Box<Integer>`: values stored as Strings. `NumberBox<Long>` with `N extends Number`: the whole row failed (UOE). On the write side, `AnyPut.create` wrote `Box<HBaseColumn<..>>` as ONE text cell instead of one cell per version. | Both sides use the parameterized BeanInfo for a fully concrete type (shared `HBaseExecutor.getNestedBeanInfo`). Wildcards and type variables are unchanged. |
| **Cassandra v3/v4** `extractData` | Properties backed only by a public field, and getters returning `T`/`T[]` from a generic superclass, kept raw driver values: a ByteBuffer for `byte[]`, an InetAddress for String, an Integer in a Long property, an Object[] for String[]. `toEntity` converted them. | Typed through `PropInfo`, with the same guard in both versions. A 33K-case differential showed changes only for those shapes. |
| **Cassandra v3/v4** bean parameters | A bean with public-field properties passed as named/positional parameters failed: "Missing required parameter". | Fallback after the getter lookup: `BeanInfo.getPropInfo`, then the `@Column` name. Unknown names are still IAE. |
| **Cassandra** `registerKeys` (deprecated) | A key property backed only by a public field was rejected under every spelling. | Same fallback, only for properties without a getter. |
| **Cassandra v3/v4** row readers | A nested-Row column bypassed `addRowValue`: a sorted row class gave a bare CCE naming no column. `addRowValue` itself hid the NPE of a null collection. | Routed through `addRowValue`; the original NPE is rethrown. |
| **CqlBuilder** | `selectFrom(..., includeSubEntities=true, excluded)` matched exclusions with `String.startsWith`/a HashSet copy, ignoring the set's own `contains`. A case-insensitive TreeSet did not exclude; a raw `TreeSet<Integer>` threw ArrayStoreException. Every other exclusion path asks the set. | The set is asked for each name and its dotted prefixes. A 25K-case differential: identical for HashSet-like sets. |
| **ParsedCql** | `{#{k}:$$v$$}` (MyBatis map key glued to a dollar-quoted value) was never rewritten. `{{street: :s}:1}` named the marker `s}:1` and dropped `}:1`. `{{f: :s}::v}` left `:s` uncounted. All fail at prepare. | Dollar-quote and brace-end rules fixed. Checked against an oracle built from Cassandra 5.0's parser: about 80K statements over two independent generators, 446 fixes, 0 regressions. |
| **MongoDB** BsonValue properties | An entity property typed `BsonInt64`/`BsonString`/`BsonDocument`/`BsonValue`/`RawBsonDocument`… could be written but never read back ("Can't parse: 5"). `List<BsonValue>`/`Map<String,BsonNumber>` elements stayed Integer/String. A record with a `BsonValue` component failed. | Such properties, elements and components are encoded through the codec registry. |
| **MongoDB** embedded documents as beans | `queryForSingleValue(field, Bean.class)` (sync, async, reactive, both mappers), `toList(…, Inner[].class)`, `distinct` with a bean-array class, and GeneralCodec bean-array components read an embedded document with plain `N.convert`. Binary fields gave NumberFormatException, `List<Long>` held Integers, `LocalDate` came back in the JVM zone, and records failed. | `convertBsonValue` reads them as the embedded document of a bean property is read, and arrays element by element. Done centrally; a per-executor workaround with different id semantics was removed. |
| **MongoDB** row types | `ArrayBlockingQueue` rows always failed. A null field into `ConcurrentLinkedQueue`/`ArrayDeque`/`ConcurrentHashMap`/`Hashtable` rows, or values a `TreeSet` row cannot compare, gave a bare NPE/CCE. `BsonArray` rows held Java values (CCE later); `RawBsonArray` rows could not be created. A document requested as `RawBsonDocument` threw. | Bounded queue sized to the row. IAE naming the field, cause kept. BSON array/document rows encoded through the codec registry. |
| **MongoDB** generic ids | `registerIdProperty` rejected a String/ObjectId id declared by a generic superclass. | Resolved through `PropInfo.clazz`. Reads convert like the non-generic twin. |
| **Cosmos** | A property name with a character outside the BMP (U+20000, legal in Java/JSON) was split at the surrogate pair, giving invalid SQL (`c.a𠀀c.b = @p0`). | Code-point scanning. 480K-case differential: no other output changed. |
| **BigQuery** | A NULL or uncomparable cell read into a row map/collection that rejects it (ConcurrentHashMap, ArrayBlockingQueue, ArrayDeque, TreeSet, …), including the public `toMap(..., mapSupplier)`, gave the JDK's bare NPE/CCE. | IAE naming the field, cause kept, as Cassandra/DynamoDB/Mongo already do. |
| **CqlMapper** | `<cql id="">` reported "Missing required 'id' attribute" although the attribute was there. | "Missing or empty …". |

## Performance and memory

- CqlBuilder batch `into(String)`: the column list is rendered once and reused for later rows. 2–4x faster for
  parameterized batches, 1.7x for named ones. 20K-case differential identical, including heterogeneous Map rows and
  mid-batch failures.
- No other hot-path change met the bar. Ideas judged too small, or not behavior-preserving, are in the slice notes
  (e.g. `N.newCollection(rowClass, n)` prefers a user class's `(int)` constructor; the Cassandra negative codec cache
  could go stale, see below).
- The Cosmos read-ahead (largest memory exposure) was measured and documented precisely. It cannot be lowered through
  any public SDK option.

## Javadoc

About 120 corrections and additions. Among them:
- **Cosmos:** read-ahead with real SDK numbers. Nothing is read before consumption. An open stream reads ~270 pages
  ahead for a query and ~85–105 for `readAllItems` (sized by CPU count), item iteration another ~200–250 pages, and
  ORDER BY reads all partitions at once. Last round's "one page ahead" was false.
- **Neo4j:** cross-call `save` is a MERGE on the primary index and all labels (a dynamic label → duplicate node). A
  label removed since the load is never removed. Read-only properties are removed by the MERGE. `delete` reload cases.
  Same-session delete-then-save hits the stale graph id.
- **Cassandra:** per-method `@throws IllegalStateException` for a condition carrying LIMIT, on 20 sync and 20 async
  methods (thrown at the call site). Aggregate examples: one row only without GROUP BY, verified live. The raw-row
  `all()` list is immutable when empty. The v4 driver-thread paging ISE.
- **DynamoDB:** dotted paths through a collection write into its first element (a builder target fails on the path's
  first property). A `java.util.Date` is stored in whole seconds; Timestamp/Calendar/java.time keep millis. v2 async:
  conversion and page failures arrive through the future; after `close()`, stream/scan fail when consumed.
- **MongoDB:** the `queryForSingleValue` embedded-bean rule, identical in all 5 executor/mapper files. A String/ObjectId
  id takes the document's own `id` over `_id`. `list` Object[]/Collection row rules on every delegating overload. A
  non-null record id component is not mapped to `_id`.
- **HBase:** increment amounts are 8-byte longs, not the decimal text put/entity mapping writes; `has(...)` with a boxed
  number never matches an increment. AnyScan `addFamily`/`addColumn` override rules. Non-positive caching/maxResultSize
  use the client default. `@ColumnFamily` default mode with `@Column`. Async executor lifecycle after `close()`.
- **BigQuery:** DML counts (`getTotalRows()` only on the `jobs.query` fast path or with Arrow results).

## Tests

31 new test files (`*Review20261103Test`), 133 test methods; 2 existing tests extended or adjusted. Every behavior
fix has tests that fail on HEAD (or, for verifier-caught regressions, on the pre-fix tree). Doc corrections are
pinned by tests that pass on both. All new tests are offline.

## Verification

Verifiers found and fixed:
1. **MongoDB generic ObjectId id**: after the `registerIdProperty` change, a stored String/Integer `_id` was written raw
   into an ObjectId-bound generic id (CCE on use). It now converts like the non-generic twin.
2. **MongoDB map rows**: the per-field put (added to name a rejected null) dropped the map type's own `putAll`. A
   `BasicDBObject` row kept an Integer key, and HashMap-subclass rows changed order. Fixed: `putAll` as before, then a
   field-by-field refill only to name the field on NPE.
3. **MongoDB `Optional[]`/`Nullable[]`**: the new element-wise array conversion left null elements null where HEAD
   gave `Optional.empty`. They take the component type's default again.

Plus a test guard that could never fail (proven with a mutant build), and about 15 doc sentences that overstated what
the code does (all corrected).

| Check | Result |
|---|---|
| Baseline, clean HEAD `509af62`, round start (JUnit launcher, every `*Test`; Mongo, Cassandra, DynamoDB Local, Cosmos up) | 5,832 found / 5,831 passed / 0 failed / 1 aborted (live-Mongo timeout) |
| Baseline, clean HEAD, re-measured in the final environment (MongoDB stopped) | 5,832 found / 5,814 passed / 0 failed / 18 aborted (live-Mongo skips) |
| Final tree, same runner and environment | **5,966 found / 5,948 passed / 0 failed / 18 aborted** (the same 18 live-Mongo skips; +134 tests) |
| All reactor modules (`mvn -o compile`) | BUILD SUCCESS |
| Doclint (`all,-missing`) over every main source | 0 real findings (44 known Lombok `StatementSettings` false positives under `-proc:none`) |

## Left for a decision (proposed, not applied)

1. **MongoDB generic id auto-detection**: a String/ObjectId id declared by a generic superclass (`Base<K>{K id}`) is
   not recognized without `registerIdProperty`. It is stored as `id` next to a generated `_id`. Fixing it changes the
   storage format: existing documents would read back with the generated id.
2. **MongoDB**: an embedded document read into a `Code`/`Binary`/`Symbol` (or bean-like BsonValue) property is still
   bean-mapped (`Code{code=null}`, NPE for Binary), as on HEAD. Routing it through `convertBsonValue` would make these
   reads IAE.
3. **All adapters**: `ImmutableCollection`/`ImmutableBiMap` row classes fail with "Not able to create instance"
   (ImmutableList/Set/Map work). Fix every adapter at once and update the pinned assertions.
4. **DynamoDB**: a NULL element of an `L` attribute read into an ArrayDeque/ArrayBlockingQueue/ConcurrentLinkedQueue
   property gives a bare NPE (maps give IAE). `LongAdder`/`DoubleAdder` properties are written as N but cannot be read
   back (abacus-common has no String factory for them, so upstream).
5. **Cassandra v4 perf**: `acceptsParameter` throws and catches `CodecNotFoundException` (~7 µs) per value whose class
   is not the column's Java type. A negative cache would go stale when a codec is registered later.
6. **ParsedCql**: `WHERE k = : k` (space after `:`) is rejected although Cassandra accepts it. A native marker ending a
   nested map-key literal glued to `$$v$$` (`{{f: :x}:$$v$$}`) is uncounted: the same root as the decided-open 10-24-r2
   item 9, so fixing it settles that item too.
7. **Cassandra**: a public field `hasChildren` next to a getter `getChildren` resolves to the getter in
   `extractData`/`prepareStatement` (abacus-common's has-prefix rule) but to the field in `toEntity`.
8. **HBase legacy rows**: a generic nested bean bound to a non-String versioned column
   (`Box<Map<Long, HBaseColumn<Integer>>>`) written by HEAD's AnyPut as one JSON text cell now fails to read
   (NumberFormatException). HEAD returned a String in a Map property, which failed on any typed access. Migration note?
9. **Upstream abacus-common**: a non-generic subclass of a generic container (`IntMap extends HashMap<String,Integer>`)
   gets `Type<Object,Object>`, so decoded Strings are stored in it (heap pollution in every adapter).
10. Minor: `AnyGet.setTimestamp` invalid values make HBase log an ERROR stack trace before rethrowing. A Neo4j fix for
    the duplicate node needs a load before every save. Still open from 11-02: the whole list in
    `report/deep-review-2026-11-02.md`.
