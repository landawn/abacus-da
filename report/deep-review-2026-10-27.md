# Source review — 2026-10-27

## Scope and method

A fresh line-by-line review of every file under `abacus-da-all/src/main/java` (69 files, ~104k lines), run on HEAD
`4c7857f`. The main sources were byte-identical to the tree the 2026-10-24 second round left; the only change since was
the dependency bump abacus-common 8.3.0 → 8.3.1 and abacus-query 4.9.5 → 4.9.6. The bump was diffed first:
abacus-common 8.3.1 only touches the JDBC `java.sql` type handlers (unreachable from this code); abacus-query 4.9.6 has
three code changes (SortedSet comparators kept for excluded property names in `Dsl.insert`/`selectFrom`, `ParsedSql.parse`
null check, `Criteria.selectModifier` block-comment rejection) and otherwise Javadoc. One of them exposed a real gap in
`CqlBuilder` (below).

The review used the same 23 slices as the previous rounds. Because the code had been reviewed three days earlier, each
slice was asked to take angles the previous pass did not: a method-by-method sibling differential (sync/async/reactive,
v1/v2, v3/v4), driver-source contract checks, executing (not reading) every Javadoc claim and compiling every example,
exception and cleanup paths, boundary arithmetic, concurrency, and tracing the abacus-common conversions actually used.
Findings that applied to a twin file were relayed to the owning slice; three of them became cross-cutting fixes.

Eight independent verifiers then re-checked the whole diff with HEAD-vs-tree differentials (see "Verification pass").

Every code fix has a regression test that fails before the fix and passes after it. Doc-only changes that describe
existing behavior are pinned by tests that pass on both. Live services: MongoDB was up (used for probes and the
self-skipping live tests). Cassandra, DynamoDB Local, Cosmos, BigQuery, HBase and Neo4j were down.

## Verification

- Baseline (HEAD `4c7857f`, all `*Test` classes, JDK 21 launcher, 8.3.1/4.9.6 jars): **5,099 tests, 0 failures,
  0 aborted**. Doclint baseline: 0 findings (the known Lombok `-proc:none` false positives aside).
- Final tree after the second review pass and the outside-review fixes, same launcher: **5,304 tests, 0 failures,
  0 aborted** (+205 over the baseline; 4 new test classes and a shared Cassandra blob fixture).
- `mvn -o -pl abacus-da-all clean test` on JDK 21: **5,304 tests, 0 failures, 0 errors, 0 skipped, BUILD SUCCESS**.
- Doclint (`-Xdoclint:all,-missing`) over every main file: no findings besides the known Lombok false positives on the two
  `CassandraExecutor` `StatementSettings` sites; Lombok-enabled doclint on the Cassandra executors is clean.
  `git diff --check` is clean, and every changed file kept its line endings.

## Bugs fixed

### Cross-cutting: immutable row classes (BigQuery, Cassandra v3 and v4, DynamoDB v1 and v2, MongoDB)
Asking for an abacus immutable row class (`ImmutableMap`, `ImmutableSortedMap`, `ImmutableList`, `ImmutableSet`, …)
returned rows of the factory's mutable stand-in (`HashMap`, `TreeMap`, `ArrayList`, `HashSet`): a `List<ImmutableMap>`
that actually held `HashMap`s, failing with `ClassCastException` only when a row was read. Every row-building site
(`readRow`, row mappers, the Cassandra `UDTCodec` decode branches, the Mongo `RowListCollector`) now converts a filled
container that is not an instance of the requested class with `N.convert`, which wraps it and keeps the values. Mutable
row classes still get the factory's instance; `ImmutableBiMap` still fails fast as before.

### Cross-cutting: Mongo BSON value row types (sync, async, reactive executors and mappers)
`Binary`, `Code`, `Symbol` and scalar `BsonValue` classes report themselves as beans, so a row of that type was mapped
as a bean: `Code{code=null}` silently, `Symbol` null, `Binary` NPE. `MongoDBBase.readRow`/`toList` now read such a row
type as a single value from the document's sole field, and the four `isSingleValueType`/`isSingleValueRowType` helpers
(single-select projection guard, `distinct` pipeline choice, `groupBy` routing, empty-document rule) were aligned with
the same predicate. Before the alignment the executors re-projected a dotted path into a nested document or converted
`_id` for a missing field, giving `ParsingException` where the docs promise `null`.

### DynamoDB (v1, v2)
- **`async(null)` / `sync(null)` messages (P3).** Named the constructor's parameter instead of the method's.
- **`asKey`/`asItem`/`asUpdateItem(Object... a)`.** The parameter is now `nameValuePairs`, so the existing
  `cs.nameValuePairs` message names the real parameter (v1 and v2; no public-API change).

### Cassandra (v3, v4)
- **Blob writes (P2/P3).** A non-`String`, non-`byte[]` value bound to a `blob` or custom column went through
  `N.convert(x, ByteBuffer)`, which base64-decodes `toString()` into a buffer left at its limit. Values whose text is valid
  base64 (`true`, `1234`, `12345678L`, `BigDecimal`, `StringBuilder("AQID")`, `MutableInt`, `AtomicLong`) were written as
  an **empty blob**. A `StringBuilder` or `StringBuffer` is now decoded as base64 text, like a `String`. Any other value
  of a JDK or abacus class is bound unchanged and the driver rejects it with
  `CodecNotFoundException`; a value of an application class still goes through its registered converter, and a conversion that yields a
  buffer at its limit binds the original value instead. An empty `Optional`/`Nullable`/`Holder` binds NULL; a present
  one binds its content (HEAD wrote an empty blob for a present `Optional<ByteBuffer>`).
- **Blob into a collection property (P3).** A blob read into `List<byte[]>`, `List<String>` or `Set<String>` was silently
  null; it is now that collection holding the one converted blob (`List<Long>` is rejected).
- **Wrapper element types (P3).** A `list<int>` column read into `List<Optional<Long>>` held `Optional<String>`
  elements; any parameterized element type now converts with its full type.
- **Blob reads and binds into non-blob types (P2).** `N.convert(ByteBuffer, X)` converts the base64 of the bytes before
  the buffer's position; a driver blob sits at position 0, so that text is `""`. A blob read into `Long`, `Integer`,
  `UUID`, `Instant`, `LocalDate`, `Date`, `Map`, `String[]` or a wrapper silently became null, 0 or empty (entity, nested
  dotted column, UDT field, `extractData`, `queryForSingleValue`, `readRow`); a `ByteBuffer` bound to an `int`,
  `bigint`, `uuid`, `timestamp`, `date`, `time`, `decimal`, `varint`, `double` or `map` column was bound as NULL; a UDT
  `timestamp` field got `setInstant(null)`. Such a target now throws `IllegalArgumentException` ("Cannot convert a blob …");
  on the bind path the buffer stays unchanged and the driver rejects it. A `CharSequence` target gets the base64 text, as
  `String` already did. Collections and arrays whose element type holds the buffer are unchanged, and a class with its own
  `Type` implementation registered by the application still converts the blob as before. A `Type` registered from string
  functions is rejected like a built-in one (HEAD handed it an empty string).
- **Map column binds (P3).** Text that is not a JSON object threw abacus `ParsingException` instead of being bound for
  the driver to reject, as documented. The catch now includes it.
- **`toEntity` with a `Map` type (P3).** An `@Entity` `Map` subclass threw `UnsupportedOperationException` ("No all
  arguments constructor found"); the documented `IllegalArgumentException` (pointing to `toMap`) is now thrown for every
  `Map` type (a missed site of the previous round's cross-cut).
- **v3 `close()` (P3).** A cluster or pool close failure replaced a session close failure. The first failure is thrown
  and later ones are added as suppressed; every resource is still closed. (v4's `pool.close()` cannot throw.)
- **Empty raw-row `list` results (P3, base and async base).** For a `Row`/`Object` target the executors pass the
  driver's `ResultSet.all()` through, which is the immutable `Collections.emptyList()` when there is no row (both
  drivers), although the docs promise a mutable list. An empty result is now a new `ArrayList`.

### CqlBuilder, CqlMapper
- **`set(Object|Class, Set)` excluded names (P3).** The excluded set was copied into a `HashSet` whenever the class has
  key properties, dropping a `SortedSet` comparator: a case-insensitive `TreeSet` holding `FIRSTNAME` excluded
  `firstName` for a key-less class but not for a keyed one. A `SortedSet` is now copied into a `TreeSet` that keeps its
  comparator, as abacus-query 4.9.6's `Dsl` does.
- **`Dsl.insert(Map, Set)` and `set(Map, Set)` (P3).** The parent removed excluded keys by looking each one up in the
  map, ignoring the set's comparator. Excluded keys are now matched with the set's own `contains`; the parent's checks,
  messages and column order are unchanged.
- **`onlyIf(Filters.where(...))` (P3).** Rendered `… IF  WHERE b = ?`, which Cassandra rejects. It is now an
  `IllegalArgumentException` raised while the IF condition is rendered, leaving the builder unchanged.
- **`CqlMapper.saveTo(OutputStream)` (P3).** The documented `UncheckedIOException` never happened: the serializer reports
  a stream failure as `UncheckedException(TransformerException ← SAXException ← IOException)`. The first `IOException` in
  the chain is now unwrapped into `UncheckedIOException`, matching `saveTo(File)` and abacus-query's `SqlMapper`.

### MongoDB
- **Empty value wrappers (P2).** An empty `Optional`/`java.util.Optional`/`Nullable`/`OptionalInt`…/`Holder(null)`
  property made `GeneralCodec` call `writeString(null)`, so the entity could not be inserted at all (also inside Maps and
  Lists). It is now written as BSON null and reads back as the empty wrapper. A null value itself still fails.
- **Nested records (P2).** A record held by a property, collection or array is encoded by the driver's `RecordCodec`,
  which looks each component up by its *declared* type; `Object`/`Number`/interface components resolved to
  `GeneralCodec`, which wrote `N.stringOf` text. A `Number` component holding 11 was stored as `"11"` and every later read
  of the entity failed; `Map<String,Object>`/`List<Object>` component values were stored as text. A value whose class
  differs from the codec's class is now encoded with the codec of its own class.
- **`Pattern` values (P3).** The driver stores a `Pattern` as a BSON regex, which decodes to `BsonRegularExpression`;
  `N.convert` then compiled its JSON text and threw `PatternSyntaxException`. The regex is now decoded with the driver's
  `PatternCodec`.
- **`extractData` with a BSON-value row type (P3).** Rows are read as the single selected field, not as beans of the
  value's getters. A mixed-row rejection message now names the row that is not of the requested type.

### HBase
- **Immutable nested beans (P2).** `AnyPut.create` flattens a record, all-args-constructor or builder bean held by a
  property into cells, but `toEntity` created each nested bean with `N.newInstance`, so every row of such an entity failed
  ("No default constructor"), and three related cases threw `ClassCastException` (a record held by a non-null field
  initializer, a read-only immutable nested bean, a builder-based target with any nested bean). Immutable nested beans
  are now filled through `BeanInfo.createBeanResult`/`finishBeanResult` after the last cell; a read-only immutable one is
  skipped. Every nested bean created for a row is handed to its owner once, after the last cell, so a setter or builder
  method that copies its argument receives it complete (HEAD lost those cells for a mutable entity with a copying setter);
  a mutable nested bean the entity already holds is still filled in place. A 100k-row randomized read-back differential over
  mutable layouts shows no change.
- **Operations as row keys (P2).** An HBase `Operation`/`Row` or `Any*` wrapper passed as a row key (`AnyDelete.of(anyDelete)`,
  `AnyGet.of((Object) anyGet)`, a scan bound) was stringified to the wrapper's bean JSON, so the call silently read, wrote
  or deleted a garbage row. `toRowKeyBytes` now rejects those types with `IllegalArgumentException` (the rule the
  previous round applied to `AnyPut.create`).
- **Versioned immutable containers (P2, pre-existing).** A versioned property declared as `ImmutableList`,
  `ImmutableSet` or `ImmutableMap` of `HBaseColumn` read back empty: a converted copy was stored while still empty and the
  later cells went to the original. The container is now assigned after the last cell.
- **Failed `addColumn` residue (P3).** HBase creates the family's cell list before building the `KeyValue`, so a failed
  add (family over 127 bytes) left an empty family behind in `AnyPut`/`AnyIncrement`; the mutation then passed HBase's own
  empty-mutation checks. The entry is removed when this call added it and it is still empty (the `AnyDelete` pattern).

### BigQuery
- **RANGE cells (P3).** A `RANGE` cell read into `String`/`CharSequence` gave AutoValue's debug text
  (`Range{startInner=…}`); it is now the RANGE literal `[2020-01-01, UNBOUNDED)`.
- **DATE parameters under non-ASCII-digit locales (P3).** `com.google.cloud.Date.toString()` uses the default locale, so
  under `ar-EG`, `fa-IR`, `th-TH-u-nu-thai` or `hi-IN-u-nu-deva` every DATE parameter was rejected by the SDK. The date is
  now formatted with `Locale.ROOT`.

### Cosmos
- **Unary `NOT` after an operator (P3).** `active = NOT deleted` rendered `c.active = c["NOT"] c.deleted`, a syntax
  error. After an operator, `NOT` followed by the start of another operand is now the keyword; it stays a property before
  `AND`/`OR`/`IN`/`LIKE`/`BETWEEN`/`IS`, a sign, `.`, `[`, `(` or `?`, keeping the previous round's `score = not - 1`
  decision. A parser differential over ~30k generated statements: 943 outputs went from invalid to valid, no valid output
  changed. A property named `escape` after `NOT` was missed at first (see the outside-review section); `ESCAPE` keeps
  `not` a property only when a string literal follows it.

### Neo4j
- **Caller filters resolved in place (P2).** OGM resolves the filters it receives in place: the single-`Filter`
  `loadAll` overloads re-indexed the caller's filter to 0, so a `Filters` object holding it then sent `age = 2 OR age = 2`
  for `age = 1 OR age = 2`; a filter reused for a second class kept the first class's owner type and `@Property` name and
  matched nothing. The 16 `Filter`/`Filters` `loadAll` overloads and `count(Class, Filters)` now send copies through the
  existing `FilterCopier`; the caller's objects are unchanged. Shapes the copier cannot reproduce faithfully (a `Filter`
  subclass, a `NestedPathSegment` subclass, a null element, an unknown OGM field layout) are passed as before.

## Behavior changes to note

1. **Cassandra (v3, v4).** A blob read into a type without a blob form now throws instead of yielding null/0/"". A blob
   read into an `Optional`/`Nullable`/`Holder` property holds the blob converted to the declared element type
   (`Optional<ByteBuffer>`, `Nullable<byte[]>`, `Optional<String>` as base64; HEAD gave an empty value), and
   `StringBuilder`/`StringBuffer` targets receive the base64 text; bound to a blob column or UDT field, such text is
   decoded as a `String` is. A JDK/abacus value without a blob form, or a
   third-party value without a converter, bound to a blob column is rejected by the driver instead of being written as an
   empty blob. A present wrapper bound to any column binds as its content would, so such properties round-trip: a codec
   registered for the content's class applies, and a wrapped number the column would change is rejected
   (`Optional.of(1.5)` for an `int` column was bound as 1).
2. **MongoDB storage format.** An empty value wrapper is stored as BSON null (it could not be stored before). Record
   components of abstract declared types are stored typed instead of as text; a UUID there now needs a UUID representation,
   and a `BigDecimal` outside `Decimal128`, a Map with non-String keys or an unencodable record now fails as it would as a
   Document field. Documents written earlier read back as before.
3. **MongoDB row types.** `Binary`/`Code`/`Symbol`/`BsonValue` row types are read as single values. A dotted `groupBy`
   key of a `Code` row type now signals `ParsingException` (the server returns the key as text) where it silently gave
   `Code{code=null}`.
4. **HBase.** An HBase operation or `Any*` wrapper passed as a row key, bare or inside a present value wrapper, is an
   `IllegalArgumentException`. Immutable nested beans are readable; a read-only immutable nested bean is skipped instead of
   throwing.
5. **Neo4j.** `loadAll`/`count` filters are copied; callers that relied on OGM filling in their filter objects no
   longer see those changes.
6. **CqlBuilder.** `onlyIf` with a `Where` condition throws; an exclusion set with its own membership rule (a
   case-insensitive `TreeSet`, or a view of one) is asked about each name.
7. **CqlMapper.** A stream write/flush failure is `UncheckedIOException` (a subclass of the old `UncheckedException`).
8. **Row classes (all adapters).** An `EnumMap` or `AbstractQueue` row class now fails with `IllegalArgumentException`;
   HEAD returned `HashMap`/`LinkedList` rows typed as those classes.

## Javadoc

About 230 Javadoc sites were corrected or completed, each checked against code, driver sources or probes. Highlights:
- **Entity construction (all adapters).** An entity needs a no-args constructor (private allowed), a builder, or a
  constructor taking all properties in declaration order (records); only a partial constructor fails. A getter-only
  property is a property only on `@Entity` classes, records, immutable or builder beans. A dotted path through a nested
  record or builder target fails (`IllegalArgumentException`/`ClassCastException`).
- **Sub-entity rule (Cassandra, Cosmos, BigQuery).** A bean-typed property without `@Column` is a sub-entity: entity
  insert/update and select-all silently omit it, and a dotted path through it is rendered with the sub-entity *class's*
  table name (`homeAddress.city` → `address.city`). Annotate UDT/STRUCT/nested-document properties with `@Column`.
- **Async facades (DynamoDB, Cassandra, Mongo, HBase).** Arguments are held by reference and converted on the worker;
  which checks run on the calling thread; `ContinuableFuture.map` re-runs per `get()`; Cassandra `get(timeout)` bounds only
  the first page; statement preparation blocks on the calling thread (and throws on a driver I/O thread).
- **MongoDB.** The typed `findOneAnd*` overloads convert the result after the server applied the write (a retry writes
  again); `aggregate`/`watch` null-stage and codec-lookup timing per variant; `distinct` decodes its single reply before
  emitting; a null selected name is rejected when the query is sent; filters/sorts/`propName` are BSON names so the mapped
  id is `_id`; cold re-subscription of an insert re-sends the fixed `_id`; `MongoDB` has no `close()`.
- **DynamoDB.** `CAMEL_CASE` keeps Map keys; `transient`/`@NonColumn` properties are written; which properties are IDs;
  `batchGetItem` drops unprocessed keys; `exclusiveStartKey` page rules; an empty query result has no columns; a Java `Set`
  becomes S text, never a set type, so ADD/DELETE need a hand-built set value.
- **HBase.** `setLimit(0)` returns the first server response; `setBatch`/`setFilter` conflicts; `familyMap` must be
  ordered by `Bytes.BYTES_COMPARATOR`; a scan's first-fetch failure is `java.io.UncheckedIOException`, not the abacus type;
  the `has(…, int)` overload is a timestamp match.
- **BigQuery.** The class doc pointed at a default-dataset setting that does not exist on `BigQueryOptions`; generated
  statements need a dataset-qualified `@Table`. `JobException` is not wrapped. STRUCT raw-property rule limited to
  `Object`/`Serializable`.
- **Neo4j.** `findOnly` counts results (distinct entities), not rows; `stream` of an entity class ignores scalar columns;
  `loadAll(ids)` with ids of another Java type is silently empty; pooled sessions keep the factory configuration they
  were opened with.
- **Cassandra.** NaN/Infinity into a `decimal` column also throws `ArithmeticException`; blob rules; a colliding
  `registerTypeCodec` is ignored with a warning; `ids()` of `CqlMapper` is insertion-ordered; `saveTo` output shape and
  escaping; DOCTYPE rejected.
- **Cosmos.** Partition key + feed range in options is an SDK `IllegalArgumentException`; `readAllItems` mutates its
  options; `SqlParameter` serializes eagerly; the ternary `?` counts as a placeholder (use `IIF`).

## Verification pass

Eight verifiers re-checked every hunk; none of them wrote the code they checked. Their differentials: a 12,985-line
Cassandra v4 read matrix and a 45×29 bind matrix; a 41,516-case v3 read and 6,545-case bind differential plus v3/v4
parity; 3,900 CqlBuilder cases through the Cassandra 5.0 grammar; a 771-case Mongo round trip (offline and live) and
18,000 `extractData` lines; a 5,500-case three-way (HEAD / tree / tree-without-helpers) live Mongo differential run three
times; 100k HBase read-back lines over new seeds plus extended immutable layouts; a 16,200-case DynamoDB row-class
differential; 1.59 million systematically generated Cosmos expressions through an ANTLR parser built from `sql.g4`; and
33 Neo4j filter shapes × 10 calls against a real OGM session factory. Every new test was run against HEAD to prove it
fails where claimed.

What they found:
- **Cassandra blob rules (regressions, both drivers).** The first version of the new rules bypassed abacus converters
  registered with `N.registerConverter` (a user wrapper class bound to a blob column stopped being converted) and user
  `Type`s registered with `TypeFactory.registerType` (a blob read into such a class threw). Fixed: a value is bound
  unchanged only when its class is built-in (the same classes abacus refuses converters for), and a blob read is rejected
  only when the target's `Type` is an abacus-own one. That correction in turn reopened the empty-blob write for
  third-party classes without a converter (`CqlDuration`, Guava `UnsignedLong`, BSON `ObjectId`); fixed with a guard that
  binds the original value when the converted buffer sits at its limit. Finally, an empty `Optional`/`Nullable`/`Holder`
  bound to a blob column had regressed from NULL to a driver rejection; an empty wrapper now binds NULL again and a present
  one binds its content (HEAD wrote an empty blob for a present `Optional<ByteBuffer>`).
- **Cassandra, pre-existing.** A blob read into a collection property of another element type (`List<byte[]>`,
  `List<String>`) was silently null; it is now that collection holding the one converted blob. A scalar element whose
  element type is a wrapper (`List<Optional<Long>>` from `list<int>`) held `Optional<String>`; fixed with `N.convert` on
  parameterized element types.
- **HBase.** A versioned property declared as an immutable container (`ImmutableList<HBaseColumn<..>>`) read back
  empty; fixed by assigning the converted container after the last cell (confirmed clean on 32k new differential lines).
- **MongoDB.** `extractData` with a BSON-value row type built bean columns instead of the scalar column the 2026-10-20
  rule prescribes; fixed. A mixed-row rejection message named the wrong row; fixed.
- **CqlBuilder.** `set(Map, Set)` still ignored a `SortedSet` comparator after `insert(Map, Set)` was fixed; fixed.
- **Javadoc.** Five doc corrections in the round's own new text (a `@param <BT>` twin, the DynamoDB getter-only claim,
  a Mapper id-convention twin, the Mongo empty-wrapper bullet, the HBase `has(…, int)` overload list), and one missing
  reactive `MongoDB` class-doc paragraph.

The remaining verifier observations are in "Left for a decision".

## Second review pass (all local changes, line by line)

Several fixes went through two to four iterations, and the verifiers above each checked one delta at a time. A second
pass therefore had eleven new reviewers read the final cumulative diff of every changed main file top to bottom (about 520
hunks), with its tests, for correctness, completeness across siblings, efficiency, comments, tests and Javadoc. A twelfth
agent then re-checked every code edit the pass itself made. Each reviewer wrote a one-line-per-hunk ledger.

What it changed:
- **Two incomplete fixes completed.** `CqlBuilder.set(Class|bean, Set)` still lost the comparator of a case-insensitive
  set that is a view rather than a `SortedSet` (an unmodifiable or synchronized wrapper, `ImmutableSet.wrap`); the
  exclusions are now decided by the set's own `contains`. `HBaseExecutor.toRowKeyBytes` rejected an operation passed as a
  row key but not one inside a present `Optional`/`Nullable`/`Holder`; it now rejects both.
- **One fix made cheaper and closer to HEAD.** The `AnyPut`/`AnyIncrement` failed-add cleanup did a family lookup before
  every cell (5–10% of an `addColumn`) and ran before HBase's own argument checks, so two misuse cases threw a different
  exception than HEAD. It now compares the family count before and after, which costs nothing and leaves every exception
  as on HEAD (444-case matrix).
- **Duplication removed.** The sync Mongo executor and mapper now share `MongoDBBase.isBsonValueClass` instead of two
  private copies (the reactive classes keep a copy because they are in another package).
- **About 110 doc and comment corrections**, mostly sentences written for an intermediate rule: the Cassandra blob-rule
  docs in v3, v4 and the base class; the Neo4j "a copy is sent" promises (not for filters that cannot be copied); the
  DynamoDB getter-only sentence (a field-backed getter-only property of an `@Entity` class is writable); the Mongo
  `selectPropNames`, `watch` and `distinct` timing notes across sync, async and reactive; the BigQuery `update`/`delete`
  `@throws`; and the HBase row-key `@throws` on 44 wrapper constructors and factories. The DynamoDB null-client messages
  now use `cs` constants.
- **About 45 tests added** for branches the round's own tests did not reach, most of them failing on HEAD.
- **No regressions and no hot-path cost.** The HBase row path allocates the same bytes per row for a plain entity and
  runs in the same time; the new Cassandra checks cost 8–19 ns and run only for blob columns.

One more effect worth noting: copying Neo4j filters also fixes invalid Cypher that HEAD sent for a nested path that repeats
one segment object (OGM compares path segments by identity).

## Findings from an outside review (fixed on request)

An outside review of the same diff reported seven items; all were confirmed against the code and fixed, and two independent
verifiers re-checked the code changes (three new HBase read-back seeds, the CqlBuilder exclusion matrices, and read/bind
differentials on both Cassandra drivers):
- **HBase, copying setters (data loss).** A nested bean was handed to a builder entity before its cells were read, so a
  builder method that copies its argument kept an empty copy and the cells were lost (the same flaw existed on HEAD for a
  mutable entity with a copying setter). Created nested beans are now assigned after the last cell.
- **HBase, docs.** Seeding an immutable nested property from the instance the property holds applies only to a mutable
  entity; a builder's default nested value is not consulted.
- **CqlBuilder, wrongly typed exclusion sets.** A set whose `contains(String)` throws (a `TreeSet<Integer>` passed as
  `Set<String>`) is again ignored, as on HEAD and in abacus-query's own checks, on every exclusion path including
  `delete(Class, Set)`, which also threw on HEAD.
- **Cassandra, blob reads.** `Optional`/`Nullable`/`Holder` properties now read back what the bind side writes, and
  `StringBuilder`/`StringBuffer` targets receive the base64 text; the docs state the exact rule.
- **Tests.** The Cosmos `NOT (` shapes and the v3 `close()` statement-pool failure branch are now pinned.

The Cassandra verifier caught one regression in the new read rule: in `extractData` a wrapper property inherited from a
generic superclass (`Optional<T>` with `T = String`) or declared through a raw getter took its element type from the
getter alone and held the raw buffer. The element type now comes from the property's own declared type, as in `toEntity`.

A follow-up from the same review reported two low-severity items in the new Cassandra blob rule; both were confirmed and
fixed in v3 and v4:
- **Stale rejection message and comments.** A blob read into a type without a blob form was rejected with a message that
  listed only `byte[]`, `ByteBuffer` or `String`. A longer list still missed collections, wrappers of containers,
  `Object`/`Comparable` and application types, so the message no longer claims to be complete. It names the rejected type,
  says it has no blob form, and gives `byte[]`, `ByteBuffer` and base64 text, alone or in a collection or wrapper, as
  examples. The class documentation has the full rule. The inline comments and Javadoc were corrected too.
- **One-way `StringBuilder`/`StringBuffer` support.** A blob read into these types as base64 text, but binding one back
  to a blob or custom-type column, or writing one to a blob UDT field, left it unchanged and the driver rejected it. Such
  text is now decoded like a `String`, bare or inside a value wrapper, and invalid base64 is rejected the same way. A
  `CharBuffer`, which abacus does not build from text, still has no blob form. Six new tests cover both drivers; all six
  fail with the decode limited to `String`, and the `CharBuffer` pins pass either way.

A third report from the same review found that a present value wrapper never reached the codec registered for its
content. It was confirmed and fixed in v3 and v4, for every column type rather than only the reported one:
- **Reported case (blob and custom-type columns).** Codec acceptance was checked only on the wrapper, so an
  `Optional<Payload>` was bound as the wrapper and the driver rejected it, although a registered `TypeCodec<Payload>`
  binds a bare `Payload`. This was not a regression; the original code converted the whole wrapper with `N.convert`.
- **Same cause, silent (every other column type).** A present wrapper was converted by `N.convert` alone. For a text
  column with a `Payload` codec, `Optional<Payload>` was stored as abacus's JSON of the value instead of the codec's
  text. A wrapped number also skipped the lossy-number check (`Optional.of(1.5)` for an `int` column was bound as 1), and
  in v3 a wrapped date/time value for a `time` column skipped the time-of-day rule.
- **Fix.** One method in each executor now handles a value that no codec accepts as is. A present wrapper, nested ones
  included, binds as its content would: content of the column's type or accepted by a codec is bound as is, and other
  content goes through the bare-value rules. Content without a form for the column leaves the wrapper unchanged for the
  driver to reject. A codec registered for the wrapper class itself is still asked first. Empty wrappers keep their
  behavior.
- **Tests.** Three new tests per driver cover a codec-backed payload on blob, custom and text columns in every wrapper
  kind and an entity insert; codec precedence for the wrapper itself; and wrapped lossy numbers and time values. With the
  old logic restored in a copy of the tree, the payload and conversion tests fail and the precedence and pin checks pass.

A fourth report from the same review raised one bug and one Javadoc correction; both were confirmed and fixed:
- **Cosmos, a property named `escape` after `= NOT`.** The unary-NOT rule treated every `ESCAPE` as the keyword that
  continues a predicate, but `ESCAPE` is a keyword only before a string literal. So `active = NOT escape` rendered
  `c.active = c["NOT"] c["escape"]` and `active = NOT escape.city` rendered `c.active = c["NOT"] c["escape"].city`, both
  rejected by Cosmos DB. They now render `c.active = NOT c["escape"]` and `c.active = NOT c["escape"].city`; an `ESCAPE`
  followed by a quote still keeps a property named `not` before it, and `LIKE 'a!%' ESCAPE '!'` is unchanged. The new
  test fails on the previous tree in its five `escape`-property cases, and its four pins pass on both.
- **DynamoDB `toEntity` Javadoc (SDK v1 and v2).** The v1 text put builder-built targets under the record rule
  (`IllegalArgumentException` for a missing intermediate bean), but a resolvable path into a builder-built target always
  fails with `ClassCastException`, because the path setter reads the intermediate bean from the builder. Both SDKs now
  say so, matching `DynamoDBExecutorBase`, and the existing test also asserts it for SDK v2.

## Left for a decision (proposed, not applied)

1. **Sub-entity dotted paths (Cassandra, Cosmos, BigQuery).** A bean-typed property without `@Column` is rendered with
   its class's table name and omitted from select-all/insert/update. A real fix is an abacus-query change (render with
   the property's column name; honor the first segment's `@Column` name) or a per-adapter builder hook. Documented with
   the `@Column` workaround in all three.
2. **Cassandra value wrappers.** Wrapper properties miss the executors' java.time and inet rules
   (`Optional<LocalTime>` from a `time` column reads the nanos as epoch millis, `Optional<String>` from `inet` gives
   `/10.0.0.2`). An application `Type` that implements only `valueOf(String)` gets the text of an empty prefix from a blob.
   A `CqlVector` bound to a non-vector column surfaces the driver's `ClassCastException`. UDT field writes never unwrap
   a value wrapper, while column binds now do for every column type (same as HEAD for UDT fields). `extractData` types a column by
   the property's raw class, so a `List<byte[]>` property from a blob holds `[ByteBuffer]` there while `toEntity` gives
   `[byte[]]`, and a non-blob value for a wrapper property is converted by the raw class (`Optional<Long>` from a `bigint`
   holds `Optional<String>("7")`) (pre-existing).
3. **Cassandra v3 `date` column binds** only the driver's `LocalDate`; `String`, `java.util.Date` and `java.time.LocalDate`
   end in `CodecNotFoundException` (v4 binds all of them). A number bound to a `timestamp` column converts silently
   (`NaN` → epoch 0, `1.5` → 1 ms); `""` binds NULL for most column types (v3 and v4). `UDTCodec` with a null `cqlType`:
   v3 rejects at construction, v4 throws NPE on first use.
4. **Cassandra sync paging and empty non-final pages.** Driver 4's synchronous iterator stops at an empty page that has
   more pages after it (the async path keeps paging). Cassandra 5.0's pagers never produce such a page; only other servers
   (ScyllaDB, DSE Search) might. A defensive loop or routing sync reads through `ResultSets.wrap` is optional.
5. **Cassandra UDT/codec bean properties as columns.** Cassandra has no joins, so a sub-entity can only be a UDT; treating
   bean-typed properties as columns by default is a behavior change spanning `CqlBuilder` and the base executor.
6. **CqlBuilder.** All-null or all-excluded factory inputs fail late at `into()`/`from()` with a misleading
   `IllegalStateException` (abacus-query's `Dsl` throws `IllegalArgumentException` at the factory); `Dsl.insert`/`select`/
   `delete(String...)` keep a live view of the caller's array; `limit(0)` renders `LIMIT 0`, which Cassandra rejects.
   `CqlMapper` load-time messages do not name the source file.
7. **MongoDB nested records.** The driver's `RecordCodec` fails for a generic record held by a property, for `List<?>`
   components, and honors `@BsonProperty` on write but ignores it on read; routing nested records through `toDocument`
   would need a provider ahead of the default registry. A `String` id from a non-`ObjectId` `_id` uses `toString()`
   (`Document{{…}}`, `Binary@hash`). Upstream: `N.stringOf` writes `Period`/`Year`/`YearMonth`/`MonthDay`/`ZoneId`/`Locale`/
   `Class` but `N.convert(String → those)` throws; `TimeZone` is typed as a bean.
8. **MongoDB.** `groupBy(Collection, Code.class or Symbol.class)` with one field throws `ParsingException`, because the
   server returns a JavaScript or Symbol key as text inside a compound `$group` key (HEAD returned a correct result only
   because `Code`'s property is named `code`, and silently wrong ones otherwise). Using the scalar `{_id: "$f"}` pipeline
   would fix it but would merge null and missing groups, for `String` too. `groupBy("code", Map.class)` rows hold such a key
   as text.
9. **DynamoDB.** The v1 `Mapper` writes the table name into the caller's request (v2 copies; four existing tests assert
   the mutation). v2 `Mapper.getItem(Map)`/`deleteItem(Map)` send a null key to the service (v1 throws first).
   `batchGetItem`/`batchPutItem`/`batchDeleteItem` with an empty collection send an empty request that DynamoDB rejects.
   v1 `ConditionBuilder` uses a `HashMap` where v2 uses `LinkedHashMap`. Dotted attribute names through an immutable
   intermediate fail the item (abacus-common path setter). Upstream: the JSON reader cannot create `EnumSet` or
   `ArrayBlockingQueue` properties, so their S text cannot be read back.
10. **HBase.** `AnyPut.create` cannot write a record or other immutable entity (the row-key lookup requires a setter on the
    write path); reading them now works.
11. **BigQuery.** An executor-level default dataset option (API addition). `java.time` parameters bind as `N.stringOf`
    text that drops zero seconds (`2024-01-15T12:00`), which GoogleSQL's DATETIME/TIME/TIMESTAMP formats may reject (not
    verified live); binding them natively would keep seconds. A `Range` parameter binds as its debug `toString()`.
    With `TimestampFormatOptions.ISO8601_STRING`, TIMESTAMP decoding throws.
12. **Cosmos.** The ternary `?` with parameters (mirror `ParsedSql` 4.9.6's "`?` after a complete operand is an
    operator" rule in `rewritePositionalParameters`); the `nextChar == ':'` object-key rule leaves a ternary's true branch
    bare (Cosmos object keys are always string literals).
13. **Neo4j.** `loadAll(Class, ids…)` with ids of another Java type: reject or convert instead of returning nothing;
    session pooling versus reconfiguring the `SessionFactory` after first use.
14. **From the second pass.** A present wrapper whose content a registered Cassandra codec accepts (`Optional<Proto>`
    with a blob codec) is bound as the wrapper and rejected (HEAD wrote an empty blob); returning the content instead would
    let the codec bind it. `AnyPut`,
    `AnyIncrement` and `AnyDelete` could share one cleanup helper (switching `AnyDelete` would change one misuse-case
    exception). A mismatched BSON value class read as a BSON-value row type (`Binary` into `Code`) still gives a garbage
    value, as on HEAD.
