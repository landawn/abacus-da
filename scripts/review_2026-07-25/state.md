# Review round 2026-07-25 — state / ledger

- **Task:** full line-by-line review of all classes under `abacus-da-all/src/main/java`
  (the repo-root `./src/main/java` in the user's prompt) for bug fixes, javadoc/comment
  improvements, and correctness of every Javadoc sample.
- **HEAD:** `cf9fba1` + a large UNCOMMITTED Javadoc/comment rewrite in the working tree
  (~2085+/1893- across 41 of 67 main files) — the fresh variable this round.
- **Deps:** abacus-common 7.8.8, abacus-query 4.8.9 (unchanged since 07-18).
- **Baseline before any edit:** `mvn -o -pl abacus-da-all compile` = exit 0;
  `mvn -o -pl abacus-da-all test` = **3461 tests, 0 Failures, 147 Errors, 15 Skipped**
  (all 147 errors = known live-Cassandra static-init: `cassandra.CassandraExecutorTest`,
  `cassandra.v3.CassandraExecutorTest`, and `AbacusDATestSuite` mirroring them).
- **Process:** read-only Explore agents over 21 slices in rolling waves; main agent is the
  SOLE editor and verifies every finding against live code/driver bytecode before applying.
- **Briefing:** `scripts/review_2026-07-25/BRIEFING.md`

## Verified up front (main agent)
- Bare `@` at line start inside `<pre>{@code ...}</pre>` (e.g. `@ColumnFamily("info")`,
  `@Id`) renders CORRECTLY with the project JDK — the working tree's `&#64;`→`@` change is
  NOT a regression. (Probe: javadoc run on a synthetic class; output `<pre><code>@ColumnFamily…`.)

## Slice assignment (21 slices, all 67 units, no gaps / no overlap)

| # | Slice | Units |
|---|-------|-------|
| A | DDB v1 sync | `aws/dynamodb/DynamoDBExecutor.java` (5132) |
| B | DDB v1 async + aws misc | `aws/dynamodb/AsyncDynamoDBExecutor.java` (2208), `aws/AnyUtil.java`, `aws/AWSRDSUtil.java`, `aws/AWSS3Util.java`, `aws/package-info.java`, `aws/dynamodb/package-info.java` |
| C | DDB v2 sync | `aws/dynamodb/v2/DynamoDBExecutor.java` (5309) |
| D | DDB v2 async | `aws/dynamodb/v2/AsyncDynamoDBExecutor.java` (3644), `aws/dynamodb/v2/package-info.java` |
| E | Mongo sync executor | `mongodb/MongoCollectionExecutor.java` (4670) |
| F | Mongo reactive executor | `mongodb/reactivestreams/MongoCollectionExecutor.java` (4277) |
| G | Mongo async executor | `mongodb/AsyncMongoCollectionExecutor.java` (3821), `mongodb/MongoDB.java`, `mongodb/package-info.java` |
| H | Mongo sync mapper + base | `mongodb/MongoCollectionMapper.java` (3436), `mongodb/MongoDBBase.java` (1814) |
| I | Mongo reactive mapper | `mongodb/reactivestreams/MongoCollectionMapper.java` (3346), `mongodb/reactivestreams/MongoDB.java`, `mongodb/reactivestreams/package-info.java` |
| J | HBase sync executor | `hbase/HBaseExecutor.java` (3942), `hbase/annotation/ColumnFamily.java`, `hbase/annotation/package-info.java`, `hbase/package-info.java` |
| K | HBase async + Scan | `hbase/AsyncHBaseExecutor.java` (2091), `hbase/AnyScan.java` (2118) |
| L | HBase Put/Get/Query | `hbase/AnyPut.java` (1529), `hbase/AnyGet.java` (1144), `hbase/AnyQuery.java` (556) |
| M | HBase other mutations | `hbase/AnyDelete.java`, `hbase/AnyIncrement.java`, `hbase/AnyAppend.java`, `hbase/AnyMutation.java`, `hbase/AnyRowMutations.java`, `hbase/AnyOperation.java`, `hbase/AnyOperationWithAttributes.java` |
| N | Cassandra v4 sync | `cassandra/CassandraExecutorBase.java` (3502), `cassandra/CassandraExecutor.java` (2558) |
| O | Cassandra v4 async | `cassandra/AsyncCassandraExecutorBase.java` (2861), `cassandra/AsyncCassandraExecutor.java`, `cassandra/ResultSets.java`, `cassandra/package-info.java` |
| P | Cassandra v3 | `cassandra/v3/CassandraExecutor.java` (2813), `cassandra/v3/AsyncCassandraExecutor.java`, `cassandra/v3/package-info.java` |
| Q | CqlBuilder | `cassandra/CqlBuilder.java` (2895) |
| R | CqlMapper + ParsedCql | `cassandra/CqlMapper.java` (942), `cassandra/ParsedCql.java` (841) |
| S | BigQuery | `gcp/BigQueryExecutor.java` (2799), `gcp/package-info.java` |
| T | Cosmos | `azure/CosmosContainerExecutor.java` (2373), `azure/package-info.java` |
| U | Neo4j + stubs | `neo4j/Neo4jExecutor.java` (2544), `neo4j/package-info.java`, `search/*` (4), `hadoop/*` (3), `blink/*` (3), `spark/*` (3) |

## Probe harness (working — reuse this)
The cached `scripts/.testcp.txt` is STALE (pins abacus-query 4.7.3; the module needs 4.8.9 →
`NoClassDefFoundError: com/landawn/abacus/query/SqlDialect`). Regenerate to the scratchpad:
```
mvn -o -pl abacus-da-all dependency:build-classpath -Dmdep.outputFile=<scratch>/cp.txt
CP="abacus-da-all/target/classes;$(cat <scratch>/cp.txt)"
javac -cp "$CP" -d <scratch>/probe Probe.java && java -cp "$CP;<scratch>/probe" Probe
```
Note: the condition factory is `com.landawn.abacus.query.Filters` (NOT `...query.condition.Filters`).

## Wave log
- Wave 1: A, C, E, F, J, N (largest units)
- Wave 2: Q (CqlBuilder — highest risk), T (Cosmos), U (Neo4j+stubs), S (BigQuery)
- Wave 3: H, K, O, P
- Wave 4: B, D, G, I
- Pending: L, M (HBase Any*), R (CqlMapper/ParsedCql — diff already main-agent-verified)

## Main-agent verifications (done before/independently of agent reports)
1. **Bare `@` in `{@code}` renders fine** — the rewrite's `&#64;`→`@` change is NOT a regression.
2. **ParsedCql doc rewrite is CORRECT and fixes a real prior error.** The old doc said non-data-op
   statements (DDL) are "stored as-is"; the constructor's else-branch actually runs
   `stripTrailingSemicolons(Strings.stripToEmpty(this.cql))` and sets `parameterCount = 0`
   (ParsedCql.java:327-329). New wording ("statement-level normalization; placeholder scanning
   skipped"; "report zero") matches. `hashCode`/`equals` both use the TRIMMED `cql` field
   (:205, :207, :811) — new "after surrounding whitespace has been trimmed" wording is accurate.
3. **AnyPut ByteBuffer position claim CORRECT** — `javap` on hbase-client 2.6.4
   `Put(ByteBuffer,long)`: `checkRow(bb)` (→ IAE on null) then `new byte[bb.remaining()]` +
   `bb.get(byte[])`, a RELATIVE bulk read that advances position to limit. The added
   `// equals keyBuffer.limit()` assertions and the "position is advanced" prose are right.
4. **CqlBuilder COUNT/WRITETIME examples CORRECT** (live probe, abacus-query 4.8.9):
   `select("COUNT(*)")` → `SELECT COUNT(*) …` (:2386 correct);
   `Dsl.PSC.count("account")` → `SELECT count(*) …` (:2818/:2843 correct — lowercase comes from
   the internal COUNT_ALL constant, so the two casings are NOT an inconsistency);
   `select("WRITETIME(lastUpdateTime) AS lastUpdatedAt")` → `SELECT WRITETIME(last_update_time)
   AS lastUpdatedAt` (the rewritten :2388 example is correct — identifier normalized, function
   name and alias preserved verbatim).
5. **CqlBuilder OR-junction rejection pre-dates this WIP** (:1491) — so dropping the per-member
   parentheses in WHERE is safe for the remaining AND-only case (AND is associative).

## Findings ledger — APPLIED

### KEY CONTEXT DISCOVERED THIS ROUND
The uncommitted working-tree change was described as a "Javadoc rewrite" but **five files contain
real CODE changes** that had never been reviewed: `CqlBuilder` (large CQL-validity hardening),
`CosmosContainerExecutor` (+300 lines of new SQL-rewrite machinery; switched from raw-SQL SCSB/ACSB/
LCSB to positional PSC/PAC/PLC), `Neo4jExecutor` (eager session release in the 3 `stream` overloads,
`newCloseHandle` deleted), `AnyRowMutations`/`AnyAppend`/`AnyMutation` (new validation guards),
`aws/dynamodb/{,v2/}DynamoDBExecutor` (`Filters.in` empty guards), `gcp/BigQueryExecutor`
(`prepareQuery(..., 1)` LIMIT). Agents verified all of these as CORRECT except the Cosmos defects below.

### WS1 code bugs — FIXED + regression-tested
1. **[P1] Cosmos `IS NULL` rewrite silently corrupted predicates with lower/mixed-case `and`/`or`.**
   `findCosmosPropertyStart` located the left operand via a CASE-SENSITIVE `endsWith`, while the
   keyword detection itself uses `regionMatches(true, ...)`. A lowercase boundary was not recognized,
   so the operand scan walked back to ` WHERE ` and swallowed the whole preceding predicate.
   Reproduced by executing the compiled class: `Filters.expr("price = 1 or category IS NULL")` →
   `WHERE IS_NULL(c.price = 1 or c.category)`. Cosmos ACCEPTS `IS_NULL(<boolean>)` and evaluates it
   to false → **zero rows, no exception** (silent wrong results). FIX: `endsWith` now compares
   case-insensitively.
2. **[P2] Cosmos: `NOT` was not an operand boundary** → `NOT x IS NULL` became `IS_NULL(NOT c.x)`
   (again silently false). FIX: added `" NOT "` to the boundary set. Safe because the whole-token
   `" IS NOT NULL"` branch is matched BEFORE `findCosmosPropertyStart` is ever called.
   Test: `CosmosContainerExecutorTest#testIsNullOperandScanHonoursLowercaseKeywordsAndNot` (6 cases).
3. **[P2] `CqlBuilder.onlyIf(Condition)` was not transactional.** The WIP's hardening made
   `appendCondition` throw in ~10 new situations, but `onlyIf` had already appended `" IF "` and does
   not run inside the parent's `mutateAtomically`. After a rejected condition `build()` silently
   returned corrupt CQL — reproduced: `"... IF "`, `"... IF s = ? AND "`, `"... IF s IN (?, "`, with
   stale bind values left in `_parameters`. (`where(...)` rolls back correctly — the asymmetry.)
   FIX: snapshot `_sb.length()`/`_parameters.size()` and restore in a `catch (RuntimeException | Error)`.
   Test: `CqlBuilderTest#test_onlyIf_rejectedCondition_leavesBuilderUnchanged`.

### WS2 non-compiling / factually wrong Javadoc samples — FIXED
- **`Filters.gte`/`Filters.lte` do not exist in abacus-query 4.8.9** (only `ge`/`le`) — 6 sites in
  `CassandraExecutorBase` (3) + `CqlBuilder` (3) → `ge`/`le`. NOTE: the ~50 `Filters.gte/lte` uses in
  the mongodb packages are the DRIVER's `com.mongodb.client.model.Filters`, which DOES have them —
  correct, do not "fix".
- **`toMap(item, TreeMap::new)`** (DDB v1 :1281) does not compile — `IntFunction.apply(int)` and
  `TreeMap` has no `(int)` ctor (javac-verified) → `IntFunctions.ofTreeMap()`.
- **`thenRunAsync(System.out::println)`** does not compile — inexact method reference is ambiguous
  between the `Runnable` and `Consumer` overloads (javac-verified). 2 sites: `AsyncHBaseExecutor`:1876,
  `AsyncCassandraExecutorBase`:2833 → one-arg lambdas.
- **`MongoDBBase` `Map.class` samples** (:1006, :1581) — `Class<Map>` is raw so `List<Map<String,Object>>`
  / `Stream<Map<String,Object>>` targets don't compile → `List<Map>` / `Stream<Map>`.
- **Reactive `MongoCollectionExecutor` insertOne re-subscribe example** claimed "a second document is
  written". Driver 5.8.0 bytecode: `createWriteOperationMono` calls `operationSupplier.get()` EAGERLY
  (outside any defer) and `DocumentCodec.generateIdIfAbsentFromDocument` does `document.put("_id", ...)`
  on the CALLER's instance → the retry re-sends the same `_id` and fails E11000. Rewritten + `Mono.defer`
  alternative shown.
- **`Nullable.orElse` does NOT replace a present-but-null value** (probe: `Nullable.of(null).orElse("fb")`
  → `null`; `orElseIfNull("fb")` → `"fb"`). 3 sites in sync `MongoCollectionMapper` → `orElseIfNull`.
- **`EstimatedDocumentCountOptions` has no read-preference setter** (javap: only `maxTime`/`comment`) —
  sync executor :431 reworded.
- Mongo async: 6 `@param update ... "replacement document"` contradicted the same block's new prose —
  the executor always wraps a non-operator payload in `$set` (`replaceOne` is the replace API).
- Mongo mapper: Dataset "(plus _id)" (there is no `_id` column); `getModifiedCount` ×2 "existed" →
  "existed and changed"; `findOneAndUpdate` "or null if no match" under `upsert(true)`+`AFTER`.
- Cassandra base: `batchUpdate(Collection, BT)` `@throws` claimed an instance check that does not exist
  on that path and "no single-column key" — `entityToCondition(Object)` SUPPORTS composite keys
  (`Filters.and`). The `isInstance` guard exists only at :515 on the batchDelete path. 3 sibling
  `@throws` gaps for the new `checkNoPrimaryKeyProperties` guard also closed.
- v3 Cassandra: `{@code TypeCodec#NULL_STR}` is a phantom member (driver 3.11.5 has no such field) →
  `{@link CassandraExecutorBase#NULL_STR}`, matching v4.
- v4 Cassandra: "retry policies" is a v3-ism (driver 4.x has no `Statement.setRetryPolicy`);
  ctor `@param` said "subclass default" for its SUPERclass + undocumented eager namingPolicy IAE;
  the positional-collection UDT paragraph moved off the abstract class onto `create(...)` (+`@throws`).
- HBase: `registerRowKeyProperty` `hc1` example depended on a test-only entity; `// throws IOException`
  sat on `getTable(...)` which throws `UncheckedIOException`; `AnyRowMutations` class-doc "deferring an
  invalid batch to the server" — the driver rejects client-side in `RequestConverter` (bytecode-verified).
- Neo4j: class doc still recommended try-with-resources for streams that no longer register any
  `onClose` handler (grep confirms `newCloseHandle` is gone); `delete` example did N round-trips while
  its own `@param` advertises `Iterable` support.
- DDB: v2 `Filters.in` example asserted a message the code never produces (real text is
  `'attrValues' cannot be null or empty`); v2 async class-doc `exclusiveStartKey` bullet was false for
  `stream`/`scan` (they resume and continue); `Product` mapper examples lacked `@Id` so the "succeeds"
  assertions would actually throw (empirically reproduced); v1 javadoc indentation regression.
- CqlBuilder: `into(String)` `@throws ISE ... not INSERT` was false on the non-batch path (probe:
  `select(...).into(...)` is accepted); `_renderingIfCondition` comment described parenthesisation the
  flag no longer controls; `InSubQuery`/`SubQuery` rejected with a "NOT IN" message.

### DEFERRED / NOT changed (recorded, with reasons)
- CqlBuilder `select(...).into(...)` emits `)SELECT` with no space (pre-existing; CQL has no
  INSERT…SELECT so the statement is invalid either way). Rejecting `_op != ADD` outright would be the
  clean fix but is a behavior change beyond this round's scope.
- CqlBuilder: raw `SqlExpression` junction members lose their protective parentheses (only bites on
  Cassandra 5.0+, which accepts `OR` in WHERE); duplicated `checkCqlTableReference` calls (3-4×) and
  untrimmed `_tableName` in `Dsl.update`/`deleteFrom`.
- BigQuery `update(Object)` on a key-less entity throws `"primaryKeyNames cannot be null or empty"`,
  naming a parameter the caller never passed (WS3 P3).
- Mongo: arrayFilters-vs-pipeline caveat present on only 1 of 4 pipeline+options overloads;
  pipeline `$set` expression-evaluation caveat.
- DDB v1 `stream(...)` doc does not state that a caller-supplied `exclusiveStartKey` does NOT limit it
  to one page (unlike `list`/`query`).

### RESOLVED — FIXED + 5 tests (see "Resolution" at the end of this section)

### [WS1][P2] ParsedCql — single-field unspaced UDT/map literal drops its named parameter
Pre-existing (this round's ParsedCql diff is doc-only). Probe-verified against abacus-query 4.8.9:

| input | SqlParser tokens | parameterizedCql | count |
|---|---|---|---|
| `{street::street, city::city}` | `{street::street` + `,` + `city::city}` | `{street:?, city:?}` ✅ | 3 |
| `{street::street}` (single field) | `{street::street}` (ONE token) | `{street::street}` ❌ unchanged | 1 |

Cause: the embedded-marker branch is gated on `(prevCurlyDepth > 0 || literalState[0] > 0)`.
In the multi-field form the first token `{street::street` leaves an UNBALANCED `{`, so
`literalState[0]` is 1 and the branch fires. In the single-field form the whole literal
`{street::street}` is ONE token with BALANCED braces, so `prevCurlyDepth == 0` and
`literalState[0] == 0` → the branch is skipped, `::street` is left verbatim and is never
registered as a parameter.

Impact: the emitted CQL keeps a native named bind marker `:street` while the other parameters
became `?` — Cassandra rejects mixing named and positional markers, and ParsedCql's own
"Cannot mix parameter styles" guard does not catch it (it only inspects markers it recognized).
Same logical CQL succeeds or fails purely on whether the literal has ≥2 fields.

Suggested fix: track brace depth WITHIN the token while locating the marker (i.e. the marker's
own position is inside a `{...}`) instead of relying only on the depth before/after the token.
STATUS: needs confirmation that the fix does not reintroduce the 2026-06-06/-07 regressions that
the `prevCurlyDepth` gate was added to prevent. Regression tests exist in `ParsedCqlTest`.

### Resolution of the ParsedCql candidate (applied)
The slice-R agent built a differential harness (old gate vs proposed depth-at-marker gate, diffed
token-by-token over every existing `ParsedCqlTest` input plus adversarial ones): only the two
intended inputs changed, everything else SAME. Neither prior regression can return — regression #1
never reaches this branch (it needs a `::` PAIR; map literals have single colons, and the gate that
guards it is the separate one at :295), and regression #2 is preserved by the scanner's own quote
state machine. APPLIED: `indexOfEmbeddedLiteralNamedParameter(word, startDepth)` tracks brace depth
WITHIN the token and requires depth > 0 AT the marker; the call site computes it once behind a cheap
`(prevCurlyDepth > 0 || word.indexOf('{') >= 0)` pre-check (the old code scanned twice, and dropping
the old `literalState[0] > 0` term makes the gate strictly safer). 5 tests added. Also: the
mixed-parameter-styles message now names the offending CQL; `isBlank` collapse.

### NEW OPEN BUG — documented, not fixed
`ParsedCql` silently drops a bind marker that is not at the START of a token: `tags + {:tag}`,
`{ :tag }`, `l + [:v]`, `m[:k]`, `{:a, :b}`, `{#{tag}}` (positional `{?}` works — `?` IS a separator).
Downstream, `prepareStatement` then binds the wrong value into the wrong slot and throws
"Parameter name at index N is null". Fixed by DOCUMENTATION only this round (a "Known limitation"
block on `parameterizedCql()`), because a real fix touches the gate guarding regression #1. Two
candidate approaches are recorded in memory; either needs the same differential-harness treatment.

### FINAL VERIFICATION
- `mvn -o -pl abacus-da-all compile` clean; `javadoc -Xdoclint:reference` over the touched packages clean.
- `mvn -o -pl abacus-da-all test` = **3468 tests, 0 Failures, 147 Errors, 15 Skipped**
  (baseline 3461 + 7 new tests; the 147 errors are the usual live-Cassandra static-init failures).
