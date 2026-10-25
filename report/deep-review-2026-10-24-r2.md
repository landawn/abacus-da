# Source review — 2026-10-24, second round

## Scope and method

A second line-by-line review of every file under `abacus-da-all/src/main/java` (69 files, ~100k lines), run on HEAD
`eab8146`, which holds the first round of the same day and its 12 applied proposals. Dependencies are unchanged
(abacus-common 8.3.0, abacus-query 4.9.5). The review used 23 slices, with the same file split as the first round. Each
slice re-checked every hunk of the previous round first, then read the whole file. Most of the earlier round's fixes had
been verified only once, and the HBase items not at all, so slices tested those hunks against HEAD. One finding cut across
the codebase: `Beans.isBeanClass` accepts an `@Entity`-annotated `Map` subclass. That finding went to every slice, and each
checked its bean-vs-Map dispatch order.

Six independent verifiers then re-checked the whole diff. They used HEAD-vs-tree differentials, a real ANTLR parser built
from the Cosmos `sql.g4` grammar, generated CQL/Cosmos corpora, randomized HBase layouts and live MongoDB probes. Together
they found five regressions in this round's own fixes, all fixed (see "Verification pass").

Every code fix has a regression test that fails before the fix and passes after it. For a regression the verifiers found,
"before" means the tree before their fix. Doc-only changes that describe existing behavior are pinned by tests that pass
on both.

Live services: MongoDB was up. The Cosmos emulator answered but was still starting ("pgcosmos extension is still
starting"). Cassandra, DynamoDB Local, BigQuery, HBase and Neo4j were down.

## Verification

- Baseline (HEAD `eab8146`, all `*Test` classes, JDK 21 launcher): **5,008 tests, 0 failures, 0 aborted**.
- Final tree, same launcher: **5,099 tests, 0 failures** (+91 over the baseline; 2 new test classes). One live-MongoDB
  test was skipped in the full run when server selection timed out under load; run alone, it passes.
- `mvn -o -pl abacus-da-all clean test` on JDK 21: **5,099 tests, 0 failures, 0 errors, 0 skipped, BUILD SUCCESS**.
- Doclint (`-Xdoclint:all,-missing`, Lombok `-proc:full` for the Cassandra executors) clean on every changed main file;
  `git diff --check` clean; CRLF preserved.

## Bugs fixed

### Cross-cutting: `@Entity`-annotated `Map` subclasses
`Beans.isBeanClass` accepts an `@Entity`-annotated `Map` subclass, while `N.typeOf(cls).isBean()` is false for it. Any
dispatch that tested "is bean" before "is Map" sent such an entity down the bean path. Its entries were lost and the Map
getters (`keySet`, `values`, `entrySet`, `size`, `empty`) were written as columns. Every such dispatch now checks Map
first, or rejects Map types where only a bean makes sense:
- **DynamoDB (v1, v2, sync and async).** `toItem`/`toUpdateItem` write the entries. The three `Mapper` constructors and the
  public `toEntity(item, Class)` reject a `Map` type with the same `IllegalArgumentException` as a plain `Map`
  (`toEntity` used to return `{}`).
- **Cassandra (v3, v4, sync and async).** `update(entity)`, `update(entity, names)`, `batchUpdate` and `prepareUpdate` set
  the entries. A missing key entry or a key-only map is an `IllegalArgumentException`. `extractData` with such a row type
  now fills entries instead of throwing or producing an `empty` column.
- **BigQuery.** Public `toEntity(FieldList, …)` fills the entries, as `toList` does. `prepareUpdate`/`keyValueOf` read the
  key from the entries; they used to read the bean field, so update/delete could hit the wrong row.
- **MongoDB.** `extractData` with such a row type fills entries (it threw `UnsupportedOperationException` or produced
  accessor columns). `toEntity(doc, Map type)` is now an `IllegalArgumentException` pointing to `toMap`.
- **HBase.** `AnyPut.create` and the `HBaseMapper` constructor reject such a class (it was mapped through its getters).

### DynamoDB
- **`Fraction` read-back (P2).** The previous round stores a `Fraction` as S text (`"3/4"`), but reading it into a
  `Number`-typed slot threw `NumberFormatException`. `toNumber` and the JSON-scalar and nested-property reads now parse the
  exact `Fraction` text. A `Fraction` inside a JSON-text value (a `List<Number>`, nested bean) is still not readable; see
  "Left for a decision".

### Cassandra (v3, v4)
- **Lossy-bind rule gaps (P3, regression from applied proposal 4).** `isLossyNumberConversion` let a fractional `Fraction`,
  `MutableDouble` or `DoubleAdder` truncate into an integer column. It also rejected an infinite `Mutable*`/`DoubleAdder`
  bound to a float/double column. Any number with a fractional value is now lossy for an integer column. Finiteness uses
  `Double.isFinite`, except for `BigDecimal`/`BigInteger`.
- **Typed container properties (P2).** A `List<Long>`, `Map<String, Instant>` or similar property read from a collection
  column kept the driver's element types (an `Integer` inside a `List<Long>`, heap pollution). Elements are now converted to
  the declared element type, through nested containers too. The driver's instance is kept when nothing changes.
- **v3 dates outside years 1000–9999 (P3).** Driver 3's `LocalDate.toString()` is not zero-padded, so reading such a date
  threw. These dates are now read through their epoch-day count.
- **CqlBuilder raw `LocalTime` literal (P3).** `09:00` (zero seconds dropped) is rejected by Cassandra; it now always
  carries seconds.
- **ParsedCql map-key marker (P3, regression from applied proposal 7).** In `{-:k: 1}` the marker name swallowed the glued
  `:` and the value. That gave a wrong name, invalid CQL, or a set literal. The name now ends at the glued `:`.

### MongoDB (sync, async, reactive)
- **Pipeline updates were not literal (P2).** The docs promise a literal `$set` stage. However, entity field values were
  evaluated as expressions, so a value `"$other"` copied another field and `"$missingField"` removed it. Field values are
  now wrapped as `{$literal: v}`. `Bson` values and operator maps still pass through as expressions.
- **Server cursor leak (P3).** Ten stream sites, plus the public `stream(MongoCursor/iterable)` helpers, closed the cursor
  with `onClose(Fn.close(cursor))`. A close interrupted mid-way left the server cursor open (live: HEAD +1 cursor in 7 of 9
  modes, tree 0).
- **`groupBy`/`groupByAndCount` (P2).** Grouping by `_id`-embedded fields with a single-value row type returned the counts
  as keys or dropped the key silently. The docs promise an `IllegalArgumentException`, which is now thrown.
- **Empty results (P3).** `extractData` of an empty result with `selectPropNames` returned a Dataset without columns. The
  doc examples call `getColumn` on it. The selected columns are now kept, and duplicate names fail without rows too.
- **Messages.** A null element in a collection argument is reported with the argument name and index.

### HBase
- **`HBaseMapper.delete(entity)` (P3).** A subclass declaring its own `@Id` that had not been put or read yet was deleted
  by the mapped class's ID property, which is another row. The row key now comes from the same lookup `put` uses.
- **Row key in the sole-writer map (P3).** The row key counted as a writer it never is. A cell shared only with the row-key
  property was unreadable.
- **Empty value-wrapper row keys (P3).** An empty `Optional`/`Nullable` row key in `AnyPut`, `AnyDelete`, `AnyIncrement` and
  `AnyAppend` constructors failed with a raw NPE. It is now an `IllegalArgumentException`.
- **Messages.** `HBaseMapper.delete(null)`, `put(null)` and `put((Collection) null)` named the internal helper's parameter.

### BigQuery
- **STRUCT into an `Object`/`Serializable` property (P3, regression from June, 9d21e01).** A two-field STRUCT failed the
  whole row with "Field count must be 1", and a one-field STRUCT silently held only its first field's text. The property
  now receives the raw `FieldValueList`, as `queryForSingleValue(Object.class, …)` does.

### Cosmos (checked with a real parser built from `sql.g4`)
- **`AND`/`OR`/`BETWEEN` after a complete operand (P3).** Before a sign, `.5` or `[`, these keywords were qualified as
  properties (`price > 1 AND -price < 0` → `c["AND"] -c.price`), a syntax error. They are now the keyword.
- **`NOT` before a sign (P3, regression from the previous round's bracket pass).** `NOT -price > 0` became
  `c["NOT"] - c.price`. That is valid but unrelated arithmetic, so the query silently matched nothing. It is the keyword
  again.
- Differentials: 0 statements went from valid to invalid; 2,646 went from invalid to valid.

## Behavior changes to note

1. **MongoDB pipeline updates** are literal. A nested bean or `Map` property now *replaces* the embedded document; HEAD
   merged it. Operator-shaped nested values and `$$NOW` are stored as given. Use a `Bson` update or an operator `Map` for
   expressions.
2. **DynamoDB `toEntity(item, Map type)`** and the `Mapper` constructors reject `Map` types, `@Entity` subclasses included.
   Use `toMap`.
3. **Cosmos.** A raw `not - value` at the start of an operand is now the keyword `NOT`. A property named `not` must be
   bracketed there. Right after an operator (`score = not - 1`) it is still the property.
4. **HBase `delete(entity)`** of a class with multiple or unsupported row-key properties now throws the same
   `IllegalArgumentException` as `put`.
5. **Cassandra.** Reading a typed container property can now fail on an element that cannot be converted. It used to
   succeed and store the wrong type. An `@Entity` `Map` subclass passed to update now writes its entries.

## Javadoc

Corrections and additions, each checked against code, driver sources or probes. Highlights:
- **DynamoDB.** Legacy `isNull`/`notNull` wording; a binary key must be a `ByteBuffer`; `limit` is a page size; the
  empty-Dataset shape; the `Mapper` rejected-ID-type list now includes arrays.
- **Cassandra.** A "Bind Value Conversion" class section, linked from about 140 `@throws` clauses across sync and async.
  Also: the `update(String)` overload-resolution trap, null clearing a column, and number rounding/NaN rules.
- **CqlBuilder.** `groupByAsc`/`groupByDesc` throw `UnsupportedOperationException`; named markers for reserved columns; the
  sub-entity expansion is not quoted.
- **MongoDB.** Bson-projection single-value rules; `distinct` unwinds arrays; pipeline-update literal semantics (operator
  maps excepted); registry arrays and dotted paths through arrays throw `ClassCastException`; empty-result Dataset shapes.
- **HBase.** Prefix-scan bounds for reversed scans; empty-wrapper open bounds; `AsyncHBaseExecutor` does not copy its
  arguments; `AnyDelete.addFamily` `@throws`.
- **BigQuery.** `BigDecimal` binds as `BIGNUMERIC`; fields of a STRUCT read into a typed container are not decoded natively;
  the `Map`-subclass contract.
- **Cosmos.** The entity-class sub-query limitation; use the SQL-text form instead.

## Verification pass

Six verifiers re-checked every hunk; none of them wrote the code they checked. Besides small doc fixes, they found:
- **Cosmos (regression).** The new "`NOT` before a sign is the keyword" rule also fired right after an operator, so
  `score = not - 1` changed meaning silently. 15,000 generated expressions with known intended output were compared;
  118 regressed. Fixed: after an operator character, `not` stays a property.
- **BigQuery (regression).** The raw-STRUCT rule also covered `Iterable`. An `Iterable<Long>` property held `FieldValue`
  elements and failed only when iterated. The rule is now limited to `Object`/`Serializable`, so an `Iterable` property
  fails at read time as before.
- **Cassandra (regression, v3 and v4).** A scalar element whose declared type is a parameterized container (a `set<int>`
  into `List<List<Long>>`) lost its element type. After the fix, a read differential (84 property types × 44/33 driver
  values) shows nothing that worked on HEAD now pollutes or throws.
- **DynamoDB.** The public `toEntity(item, @Entity Map subclass)` silently returned `{}` (now rejected, as above).
- **MongoDB.** Twelve doc places claimed operator maps were literal (corrected); the async/reactive empty-result `@return`.
- **HBase.** The `put(null)` message twin. A 539k-check randomized read-back over 21 seeds showed 0 differences.

## Left for a decision (proposed, not applied)

1. **Cassandra time column bound from a Number.** v4 reads it as epoch milliseconds, v3 as nanoseconds of the day.
2. **BigQuery `BigDecimal` → `NUMERIC`** when the value fits, as the Google SDK does. As it stands, a `BIGNUMERIC`
   parameter does not coerce to a `NUMERIC` column on insert. Not verified live, because the emulator was down.
3. **HBase phantom name-variant entries.** Name variants registered where the holder never writes can block or override a
   real writer. This is pre-existing: 452 lost and 182 wrong reads across 7 new random seeds. Fixing it is a routing change.
4. **DynamoDB `SdkBytes`/`AttributeValue` values** are written as their `toString()` S text.
5. **DST-gap dates (v3, v4).** A date read into `Date`/`Calendar` on a day whose midnight falls in a DST gap (for example
   `America/Sao_Paulo`) throws.
6. **`@Entity` `Map` class in class-typed CQL factories** (and the base `prepareQuery` select list) renders a phantom
   `empty` column.
7. **`Fraction` inside DynamoDB JSON-text values.** abacus-common's writer emits invalid JSON (`{"n": 1/4}`).
8. **HBase `AnyDelete`.** A failed `addFamily` does not restore the cells it cleared (invalid states only). `of(rowKey, ts,
   familyMap)` could fail fast on a row key over 32,767 bytes.
9. **ParsedCql.** A glued `$$…$$` value after an operator-position map key is still not counted.
10. Smaller: CqlBuilder quotes `Duration`/`Period` literals and renders `BigDecimal` outside double range. Cassandra
    `update("UPDATE …")` resolves to `execute`. A top-level UDT read into a bean property without a codec gives an empty
    bean. BigQuery decodes neither STRUCT typed-container fields nor `REPEATED STRING` into `List<com.google.cloud.Date>`.
    Cosmos: a property named like a keyword before `AND` plus a sign, and `-true` → `-c["true"]`, are still misread
    (pre-existing).

**Upstream (abacus-common 8.3.0).** `ParserUtil` resolves a `java.util.Date[]` property type as `java.sql.Date[]`, in every
adapter.
