# Source review — 2026-10-24

## Scope and method

Line-by-line review of every file under `abacus-da-all/src/main/java` (69 files, ~98k lines) on HEAD `df814d8`, for bug
fixes and Javadoc. 23 reviewers owned disjoint file slices; one more surveyed the dependency bump that landed with HEAD
(abacus-common 8.2.1 → 8.3.0), which no earlier round had reviewed. Sibling findings were relayed so twin files
(v1/v2, v3/v4, sync/async/reactive) got the same fix. Eight independent verifiers, none of them authors, then re-reviewed
the whole diff with HEAD-vs-tree differentials, offline grammar/parser checks and live MongoDB probes. They found and
fixed three regressions introduced by this round's own fixes (see "Verification pass").

Every code fix has a regression test that fails on HEAD and passes on the tree. Doc-only changes that describe existing
behavior are pinned by tests that pass on both.

Live services: only MongoDB was reachable. Cassandra, DynamoDB Local, the Cosmos and BigQuery emulators, HBase and Neo4j
were down. CQL claims were proven with the Cassandra 5.0 parser and literal type-checker run offline; Cosmos claims rest on
the NoSQL grammar (`sql.g4` from the .NET SDK) and Microsoft Learn; HBase claims on the hbase-client 2.6.6 sources.

## Dependency bump (abacus-common 8.2.1 → 8.3.0)

No behavior that abacus-da can reach changed. Method-level bytecode comparison plus a 469,643-check differential probe gave
identical output on both jars. Every known trap is unchanged (ContinuableFuture.map re-runs per get, immutable `N.asList`,
ByteBuffer conversion, the `isInstance` short-circuit, strict Boolean conversion, `LocalDateTime`/`ZonedDateTime` →
`Instant` parse failure, `PropInfo.isReadOnlyProperty`), so no workaround or Javadoc went stale.

## Verification

- Baseline (clean HEAD, all `*Test` classes, JDK 21, 8.3.0 jars): **4,846 tests, 0 failures, 0 aborted**.
- After the review and verification pass, same launcher: **4,948 tests, 0 failures, 0 aborted**; `mvn -o -pl abacus-da-all
  clean test` on JDK 21 (Surefire, no Mockito agent): 4,948 tests, 0 failures, BUILD SUCCESS.
- After the applied proposals (final tree): **5,008 tests, 0 failures, 0 aborted** with the launcher, and `mvn -o -pl
  abacus-da-all clean test` on JDK 21: **5,008 tests, 0 failures, 0 errors, 0 skipped, BUILD SUCCESS** (+162 tests over the
  HEAD baseline; 8 new test classes).
- Doclint (`-Xdoclint:all,-missing`) clean on every changed main file; `git diff --check` clean; CRLF preserved.

## Bugs fixed

### Cassandra
- **Zero-valued keys could not be inserted (P2).** abacus-query's `insert(entity)` leaves id properties out when every key
  value is null or 0 (the SQL "generated id" convention). Cassandra never generates keys and rejects an INSERT without them,
  so an entity whose key is 0 could not be inserted by any executor path (sync, async, batch, v3 and v4). The executors now
  build the insert map themselves (`CassandraExecutorBase.prepareInsert`); `CqlBuilder.Dsl.insert`/`batchInsert` keep the key
  too. Map and non-bean entities still take HEAD's exact path. In a batch that mixes a 0 key and a null key, the key column is
  now kept and null is bound for that row.
- **java.time and inet conversions (P3, v4 and v3).** Pairs that still went through `N.convert`'s `toString()` parsing threw:
  a timestamp read into `LocalDate`/`LocalTime`/`OffsetTime`, a date read into `LocalDateTime`/`Instant`/`ZonedDateTime`/
  `OffsetDateTime`, a `LocalDate` bound to a timestamp column. They now convert in the JVM default zone, as driver 3 already
  did. An inet value read into a `String` (or an `InetAddress` bound to a text column) gave `"/127.0.0.1"`; it is now the
  address text. Nested (dotted) columns now get the same conversions as top-level ones.
- **Later-page `Error` wrapped (P3).** `ResultSets` turned an `Error` failing a later page into a plain `RuntimeException`;
  it is rethrown unchanged, as the driver's own result set does.
- **CqlBuilder (P3).** Driver durations rendered quoted (`'1h30m'`, rejected); now bare. `distinctOn`/`distinctRow` rendered
  invalid CQL; now `UnsupportedOperationException`. LIMIT and ORDER BY on UPDATE/DELETE now throw `IllegalStateException`
  instead of rendering invalid CQL. The sub-query rejection message names the rejected construct.
- **ParsedCql bare `#` (P2).** A bare `#` (not `#{`) was treated as a MySQL comment, so `DELETE FROM t WHERE k = ? # AND c = ?`
  silently ran as the wider `DELETE FROM t WHERE k = ?`. CQL has no `#` comments; the text is now passed through and
  Cassandra rejects it. An error message for an empty MyBatis name no longer prints internal mask characters.

### DynamoDB
- **SDK v1 binary values (P3).** A `B` attribute (or a binary element of `BS`/`L`/`M`) read into a type that cannot hold a
  buffer became `""`, null or a default. It now converts from the bytes exactly as SDK v2 does: a `String` target gets
  `"[1, 2, 3]"`, number/date/UUID/enum targets fail with v2's exception.
- **Dotted attribute names (P3, v1 and v2).** A dotted attribute such as `nested.x` skipped the executor's conversion rules
  (v2 read a `ByteBuffer` as an empty buffer; typed lists kept raw Strings). It now reads exactly like the same property at
  top level.
- **v2 async page `Error` wrapped (P3).** `stream`/`scan` turned an `Error` failing any page future into a `RuntimeException`.

### MongoDB
- **`String[]`/`Object[]` round trip (P2).** The codec writes such arrays as JSON text; reading them back wrapped the text as
  one element. The text is now parsed back (leniently), also for dotted keys and nested beans.
- **Read-only binary properties (P3).** A getter-only `byte[]`/`ByteBuffer` property whose stored value was text failed the
  whole row; it is now skipped like other read-only properties.
- **Cursor leak on `cancel(true)` (P3).** Cancelling an async `list`/`query` future interrupted the worker, and the cursor
  close then skipped `killCursors`; the server cursor stayed open for 10 minutes (proven live). Cursors are now closed with
  the interrupt status cleared and restored.
- **Async streams during JVM shutdown (P3).** The stream methods ran the query and then threw, leaving the cursor open; they
  now fail before submitting.
- **Reactive `query(Collection, …)` (P3).** The Dataset columns were re-read from the caller's collection on each
  subscription and could disagree with the projected fields; the names are now copied at call time.

### HBase
- **Null scan bounds (P2).** `AnyScan` stored a null start/stop row as null, but HBase treats only the empty array as
  unbounded: a forward scan with a null stop row stopped after the first region, a reversed scan read only the first region,
  and an exclusive null start threw NPE.
- **Empty optional values (P2).** Writing an entity with an empty `Optional`/`Nullable`/`OptionalInt` or a `Holder` of null
  threw NPE. Such values are now skipped like null (an empty cell would read back as a present `""`).
- **Naming-policy variants (P3).** Whether a variant was registered depended on a class-wide check, so a SNAKE_CASE mapper
  could not read back cells it wrote itself.
- **`HBaseMapper.delete(entity)` (P3)** took the row key from the id property while `put`/`get` use the registered row-key
  property, so it deleted a different row.

### BigQuery
- **DATE into `com.google.cloud.Date` (P2).** The executor writes this type as a native DATE but could not read it back
  (every read path threw; a `List` property kept the raw text).
- **Zero ids (P2).** An entity with `id = 0` was inserted with the id column left out, storing NULL.

### Cosmos (P2, from the NoSQL grammar)
- Properties named like reserved words (`value`, `order`, `desc`, …), names starting with `$`, and non-ASCII names were
  written as `c.value`, a syntax error; they now use the bracket accessor `c["value"]`.
- A reserved, dotted or non-identifier table name produced an invalid `FROM`, or (dotted) iterated a nested property instead
  of the items; it now reads `FROM c`.
- An `IS NULL` operand glued to `AND`/`OR`/`NOT` (`(a = 1)AND name IS NULL`) was rewritten into a condition matching
  nothing.

## Behavior changes to note

These are deliberate and tested, but visible to callers:
1. DynamoDB v1 binary reads (and all dotted attribute reads, v1 and v2) now follow the same rules as SDK v2 / top-level
   reads. Some reads that silently returned empty or default values now throw.
2. ParsedCql never strips `#` text. A statement whose first line is a `#` note, which used to run with that line removed,
   is now rejected by Cassandra at prepare.
3. A plain bracketed string field read into a MongoDB `String[]`/`Object[]` property is now parsed (`"[draft]"` → `{"draft"}`).
4. `AnyScan.getStartRow()`/`getStopRow()` return an empty array instead of null for a null bound.
5. Inet values read as text are the bare address (`127.0.0.1`), no leading `/`.
6. The proposals applied at the end of the round add more: MongoDB array storage format, Cassandra lossy binds and base64
   blob text, DynamoDB `Fraction` as S and fail-fast row types, `N.convert` on Cassandra rows. See "Proposals applied on
   maintainer request", which lists each one.

## Javadoc

About 150 corrections and additions, each verified against code, driver sources or probes. Highlights: DynamoDB
`query(QueryRequest, Class)` supported row types, empty-result shapes, `exclusiveStartKey` continuation in `stream`/`scan`,
missing items yielding empty `Optional`-like values; MongoDB bulk writes throwing `MongoBulkWriteException` in unordered mode,
no `ClientSession` overloads, single-value projection rules, `groupBy` row shapes; Cassandra primitive vs object array row
types, NULL-to-default rules, name-variant matching, UDT field rules; HBase null-family caveats on `AnyDelete`, row copies,
`setFilter` order; Neo4j filter-index collisions in `count`, non-atomic multi-entity `delete`, negative depth on relationship
entities; BigQuery and CqlBuilder reserved-keyword workarounds.

## Verification pass

Eight verifiers re-reviewed every hunk. Beyond small doc corrections they found and fixed:
- a **regression** from the zero-id fixes: an `@Entity`-annotated `Map` subclass lost its entries on insert (BigQuery and
  Cassandra); fixed and pinned;
- a **regression** from the HBase naming-variant fix: a scalar could take a nested bean's only cell slot, or change which of
  two nested beans in one family received a cell; the claims now run after registration and never displace a bean's
  only entry (randomized differential over ~10,000 layouts shows only gains over HEAD);
- a **regression** from two fixes interacting: an empty `Optional` used as an `AnyScan` row bound became a null row again,
  the bug the null-bound fix had just removed; it is now treated as unbounded;
- the MongoDB array-text fix missing dotted keys;
- a new MongoDB test that was flaky under `mvn` (Mockito self-attach interrupted);
- the `$`-leading Cosmos names;
- the ParsedCql mask-character message leak.

## Proposals applied on maintainer request

The review ended with 12 proposals. The maintainer asked for all of them to be applied. Where a proposal offered options,
the option the reviewers recommended was taken. Each change has a test that fails on the tree before this step and passes
after it. Independent verifiers re-checked items 1–9 and 11 against that tree; the HBase verification (items 10 and 12)
was stopped early on request, after it found and fixed one more issue (see item 12).

1. **DynamoDB `Fraction` (v1, v2).** A finite number whose text is not a decimal, such as an abacus `Fraction` (`"3/4"`), is
   stored as S instead of an N value DynamoDB rejects; it reads back into a `Fraction`. Every decimal number is still written
   as the same N text (339,816 random values compared).
2. **DynamoDB `query(QueryRequest, Class)` (v1, v2, sync and async).** A row type that is not a `Map` or a bean is rejected
   with `IllegalArgumentException` before any request is sent. It used to read every page first; an empty result used to
   succeed. A few single-value types that happened to work for particular data (for example `Object` when every item had one
   map attribute) now fail up front; `list(QueryRequest, Class)` is the method for them.
3. **Cassandra row converter (v3, v4).** The `readRow` converter is also registered for the concrete driver row class, so
   `N.convert(row, Bean.class)` / `Map` / `Object[]` / single values now work. `N.convert(multiColumnRow, String.class)` now
   throws "Column count must be 1" (it returned the row's `toString()`).
4. **Cassandra lossy binds (v3, v4).** Binding a fractional value to an integer column (`1.5` → `1`) or a finite number that
   becomes infinite in a float/double column now fails with an `IllegalArgumentException` naming the parameter and the CQL
   type. Exact conversions (`2.0` → `2`, `BigDecimal("3.00")`) still bind.
5. **Cassandra blob ↔ String (v3, v4).** Base64 both ways: a `String` bound to a blob column is decoded (invalid base64 →
   `IllegalArgumentException`; it used to bind an empty blob), and a blob read into a `String` is the base64 of its bytes
   (it used to be `""`). A `ByteBuffer` bound to a text column now writes its base64 text; CUSTOM columns also decode base64.
6. **MongoDB arrays.** Java arrays other than `byte[]` are stored as BSON arrays (elements encoded as in a `List`) instead of
   JSON text, so element queries, `$push` and `$addToSet` work (verified live, sync, async and reactive). Storage-format
   change: documents written earlier still read back, but element queries and `$push` do not match them until rewritten;
   an array-valued filter no longer matches them; `UUID[]` needs a UUID representation; an array-typed `_id` is rejected by
   MongoDB; elements the driver cannot encode (a `BigDecimal` outside `Decimal128`, a `Map` with non-`String` keys) now fail on
   write, as inside a `List`.
7. **ParsedCql.** A native `:name` marker after `+ - * / %` inside braces (`{f: 1 + :x}`) is rewritten and bound. A `:`
   there that is not a valid marker now fails at parse time. A marker after a type hint (`{f: (int) :x}`) stays a documented
   limitation.
8. **CqlBuilder.** `NaN`, `Infinity` and `-Infinity` render as CQL literals. Reserved CQL keywords used as column or table
   names (`desc`, `order`, `select`, …) are quoted automatically through abacus-query's `appendColumnName` hook; output
   changes only for statements Cassandra rejected before (16,797-case differential, 0 valid statements changed).
9. **Neo4j `count(Class, Iterable<Filter>)`.** A plain list of filters is copied into a fresh `Filters`, so parameter names
   no longer collide (the count was silently wrong) and the caller's filters are left untouched. The copy reads OGM's
   private comparison function by reflection, with a fallback to the old behavior if OGM's layout differs from 5.0.10.
10. **HBase `AnyPut` / `AnyDelete`.** `AnyPut.create(Object)` rejects an HBase operation or `Any*` wrapper with a clear
    `IllegalArgumentException` (it was mapped as a bean); collection factories pass a native `Put` through. A failed
    `AnyDelete` add no longer leaves an empty family entry that would turn the delete into a whole-row delete.
11. **Cosmos integration test.** Queries use `c["value"]`; the localhost skip is gone and a probe query (which must return its
    own item) decides whether query tests run. Compile-verified only: the emulator was down.
12. **HBase mapping.** The remaining naming-variant shapes (`@Column` scalar, unannotated nested bean without a fixed family)
    read back their own cells, and nested beans are searched in a fixed order. Cells that two properties write keep their
    previous routing. Randomized differential over 722,096 checks: 4,205 gains, 0 losses. The stopped verifier also fixed a
    cell mapped only by the row-key property being dropped on read.
