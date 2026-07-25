# Review round 2026-07-25 — BRIEFING for read-only review agents

You are ONE read-only reviewer in a multi-agent full line-by-line review of
`abacus-da-all/src/main/java` (67 units, ~80k lines). Working tree = HEAD `cf9fba1` **plus a
large uncommitted Javadoc/comment rewrite** (see "NEW THIS ROUND"). The codebase has had MANY
deep review rounds (2025-H1 through 2026-07-22); it is MATURE. Expect zero-to-few genuine
findings. A finding must carry evidence (file:line + the exact code + why it's wrong);
speculative or style-only reports without a concrete defect are noise.

**Review the WORKING TREE (current file contents), not HEAD.**

## NEW THIS ROUND — the one fresh variable
The working tree contains an **unreviewed, uncommitted Javadoc/comment rewrite**: ~2085 inserted
/ 1893 deleted lines across 41 of the 67 main files. Dependency versions did NOT change
(abacus-common 7.8.8, abacus-query 4.8.9 — same as the last two rounds), so **the doc rewrite is
where new defects are most likely to be**. Observed change classes in that rewrite (verify each
in your slice against the ACTUAL enforcement path / driver behavior):
- HTML entities inside `{@code}` → raw characters (`&lt;`→`<`, `&#64;`→`@`). Raw `<>` and raw `@`
  inside `<pre>{@code ...}</pre>` are CORRECT and render fine (empirically verified with the
  project JDK this round) — do NOT flag that direction.
- `http://` → `https://` in `@see` links.
- Newly-added inline assertion comments in examples (`// returns …`, `// equals …`, `// throws …`).
  **These are the highest-risk items: each asserts a concrete value/behavior. Verify them.**
- Rewritten semantic prose (e.g. AnyPut cell-overwrite-vs-duplicate wording, ByteBuffer
  position-advance claims, entity-mapping examples gaining constructors).
- Examples restructured / split across multiple `<pre>` blocks.
Judge these on their merits: if a rewritten sentence is now WRONG or an added `// comment` asserts
a value the code does not produce, that is a P1/P2 WS2 finding. If it is right, say so and move on.

## Scope dimensions (report under these workstreams)
- WS1 bugs (correctness) — highest bar: show the failing input/state and wrong outcome.
- WS2 javadoc/comment errors — doc contradicts actual behavior (verify the enforcement path).
  **Includes: every code sample must COMPILE conceptually and produce the asserted values.**
- WS3 exception/log message defects — wrong/misleading text in thrown messages or logs.
- WS4 performance — real, measurable waste on a hot path (not micro-nits on network-bound code).
- WS5 simplification — only if it removes genuine complexity with zero behavior change.
- Edge-case test gaps — only where a REAL untested behavior boundary exists in code you can name.

## How to verify (this repo's proven techniques)
- Driver/library ground truth: jars are in `~/.m2/repository` and
  `~/.m2/.lemminx-maven/...`; use `javap -c -p` on the relevant class, or extract a `-sources` jar.
  Versions in play: hbase-client 2.6.4, azure-cosmos 4.81.0, google-cloud-bigquery 2.67.0,
  neo4j-ogm 5.0.7, DataStax driver v4 (`cassandra/`) and 3.11.x (`cassandra/v3/`),
  mongodb-driver-sync / -reactivestreams, AWS SDK v1 (`aws/dynamodb/`) + v2 (`aws/dynamodb/v2/`).
- Compiled module classes: `abacus-da-all/target/classes`; test classpath file `scripts/.testcp.txt`.
- A tiny throwaway probe/`jshell` against that classpath is the gold standard for "does this
  example really return X" — prior rounds settled many disputes that way.
- Do NOT run `mvn` yourself (the main agent owns builds) and do NOT edit any file.

## DO NOT RE-FLAG (settled, verified across prior rounds)

### Global / cross-cutting
- NO executor implements AutoCloseable — user-settled THREE times (last: 2026-07-16, removed from
  HBaseExecutor again, pinned by a test). Explicit `close()` + try/finally examples are the
  contract. Do NOT propose `implements AutoCloseable` or try-with-resources on executors.
  (try-with-resources on Stream/ResultScanner/Table/MongoCursor IS correct — leave.)
- Empty stubs (search/*, hadoop/*, blink/*, spark/*, aws/AWSRDSUtil, AWSS3Util) are deliberate
  placeholders. Ctors must stay PACKAGE-PRIVATE (tests do same-package `new X()`; making them
  private BROKE test-compile once). `aws/package-info` "shared helpers" wording is ACCURATE
  (aws/AnyUtil is a real helper) — settled 07-22, do not re-flag.
- Deliberately-left dead code: DDB dead-store `newQueryRequest` family + dead `rowClass == null`
  branches; BigQuery `entityToCondition` unreachable `isEmpty(conds)`; Cosmos
  `toSqlQuerySpec`/`rewritePositionalParameters` parameterization branch (callers use RAW_SQL);
  Cosmos `count > 0 → limit` dead branch; Cassandra prepareStatement dead
  `else if (N.isEmpty(parameters))`.
- abacus-common `N.asMap(...)` returns ImmutableMap — existing sites wrap
  `N.newLinkedHashMap(N.asMap(...))`. Only flag a NEW unwrapped site whose result is mutated.
- `N.convert` short-circuits when the raw container class is assignable → element conversion uses
  the `type.valueOf(N.toJson(value))` idiom (DDB v1/v2 toEntity, BigQuery). The v1/v2 duplication
  of that block is user-DEFERRED — do not re-propose extraction. `isElementConversionNeeded`
  skips Object-slot rebuilds (byte[] preservation) — settled with regression tests.
- DDB package convention: `@throws IAE` for null args even where SDK-delegated; async variants
  defer the documented IAE into the future (house convention). Do not mass-rewrite.
- "Add more logging" is OUT OF SCOPE (WS3 = fix existing messages, not add new ones).

### Cassandra (NOTE: `cassandra/` = DataStax driver v4, `cassandra/v3/` = driver 3.x)
- `prepareStatement`: null query → IAE (checkArgNotNull since a97c61e — the old "null → NPE" docs
  were corrected); explicit null bind values stay null; caller's array is defensively cloned.
- `toList` Row-passthrough guard `targetClass.isAssignableFrom(Row.class)` is CORRECT — never flip.
- `toEntity` sets non-Row driver-decoded values as-is (only nested Row→bean) — intentional v3+v4.
- `execute(Statement)` null → v4 IAE from RequestProcessorRegistry ("No request processor found"),
  v3 NPE. String overloads route through prepareStatement (IAE).
- v3/v4 `findFirst` null-targetClass documented-IAE-but-no-throw edge — left, consistent siblings.
- Async: prepare-time failures (malformed CQL, unknown table/column, missing named param) throw
  AT THE CALL SITE (session.prepare is sync) — 53 example blocks already reworded; only driver
  EXECUTION failures arrive ExecutionException-wrapped; `ContinuableFuture.map` mapper exceptions
  rethrow UNWRAPPED from `get()`. `ContinuableFuture` has `thenRunAsync(Runnable|Consumer|
  BiConsumer)`/`thenCallAsync`/`map` but NO `thenAccept`/`thenAcceptAsync`.
- CqlBuilder: the 11 `Dsl` builders have been verified consistent repeatedly; string-select
  aliases only policy-changed columns while entity-select aliases unconditionally (both correct,
  different paths); naming policy lowercases selectModifier tokens (CQL is case-insensitive);
  IN/NotIn named-param sanitization happens inside `setParameter`; BETWEEN pre-sanitizes because
  of its "min"/"max" prefix; `selectFrom(Class, includeSubEntities=true)` emits a multi-table FROM
  documented as SQL-parity-only; `select("expr AS alias")` keeps the alias VERBATIM; the 3 ISE
  messages use `opCqlKeyword()`; `ifNotExists` is INSERT-only; `onlyIf` is UPDATE/DELETE-only.
- CqlBuilder overrides `from(Class)`/`from(Class,alias)`/`from(expr,Class)` with
  `checkCanAppendCqlFrom()` because abacus-query 4.8.9 made `AbstractQueryBuilder.from` SELECT-only
  and CQL legally allows `DELETE col1, col2 FROM tbl`. Do NOT propose removing these as redundant.
- v4 class header: PreparedStatement-only cache (BoundStatement pooling deleted in 4034f47).
- `CqlMapper.load(String)` missing file → descriptive RuntimeException (deliberate divergence from
  SqlMapper's bare NPE). ParsedCql curly-depth + in-quote tracking both fixed and tested.
- `CassandraExecutorBase` ctor rejects a namingPolicy outside {SNAKE_CASE, SCREAMING_SNAKE_CASE,
  CAMEL_CASE} eagerly with IAE (contract change, settled). Condition factory is `Filters.eq(...)`
  (no `CF`/`ConditionFactory` class exists in abacus-query 4.8.9).

### MongoDB
- EVERY Bson-filter read AND write method rejects a null filter with IAE — one guard at the
  private `executeQuery` chokepoint (sync + reactive) plus explicit guards on count/distinct/
  driver-direct methods; async inherits via delegation. Do NOT propose terminal guards or removal.
- sync `queryForSingleValue` matched-doc-but-missing-field → PRESENT `Nullable.of(null)` (and
  `queryForSingleNonNull` → NPE); reactive → completes EMPTY. GENUINELY different; each doc correct.
- `objectIdToFilter` examples show `{"_id": {"$oid": "…"}}` — genuine `Document.toJson()`
  extended JSON. RECURRING false positive (wrongly proposed 2×) — NEVER change to `ObjectId(...)`.
- `toBson` copies before stripping `_id`; empty-`$set` throws IAE; `resetObjectId` only converts
  24-hex Strings / 12-byte arrays. `estimatedDocumentCount(null options)` = defaults.
- sync `MongoCollectionExecutor`'s `toBson` rationale comment is correct as written (the
  "protected toDocument invisible" claim was corrected for the sync class in 07-17 r2; the
  reactive sibling's identical-looking comment is CORRECT there — different package).
- `findFirst` terminals (Collection- and Bson-projection, sync + reactive) short-circuit
  `rowType.isAssignableFrom(Document.class)` → raw document identity, matching list/stream —
  INTENDED, 4 regression tests. Do not propose reverting.
- The async executor deliberately lacks 6 sync-only conveniences (`stream()`, `stream(Class)`,
  typed `groupBy`/`groupByAndCount` ×4) — documented; adding them is a user-DEFERRED API decision.
- Reactive `.map`→`.mapNotNull` migration is complete. Reactive examples use `Flux.from(...)` /
  `Mono.from(...)` because the executor/mapper return `org.reactivestreams.Publisher`.
- `MongoDBBase.toJson(Bson)` = standard JSON (abacus `N.toJson`), not extended JSON.

### DynamoDB
- list/query auto-pagination treats `Limit` as PAGE size — documented, intentional.
- v2 async list/query/stream/scan blocking `.get()` pagination inside `thenApplyAsync` = accepted
  tradeoff; `scanFilter` is nullable; v1 `getItem` null-targetClass tolerance is documented.
- toEntity container rebuild (v1+v2) + byte[]-preservation skip — settled with regression tests.
- v2 `toKeyAttributeValue` admits byte[] keys while v1 rejects all arrays — deliberate v2
  improvement, not drift.
- v2 fixed-arity `asItem`/`asUpdateItem` deliberately do NOT validate attr names (N.asMap accepts
  null keys; `asKey` validates, `asItem` does not — matches v1 + the varargs form). The
  "`@throws IAE if attrName null`" tags were REMOVED as phantom — do not re-add them or add guards.
- Batch-limit overruns surface as a service ValidationException, not a client-side IAE.

### HBase (verified via hbase-client 2.6.x decompile — do not flip these)
- Ctor NPE-vs-IAE split: single-arg `Put(byte[])`/`Delete(byte[])` deref `row.length` → **NPE**;
  `Get(byte[])` routes through `checkRow` → **IAE**; offset/length variants → IAE;
  `Get(row,-1,2)` → **AIOOBE**; `Put(byte[],boolean)` → IAE (checkRow before any deref);
  `Mutation(row,ts,familyMap)` → NPE for null row AND null familyMap, IAE for empty row.
- `AnyPut.addColumn` family validation is PHANTOM (no client-side check; null/empty silently
  accepted, only `ts < 0` throws) — `@param` caveats only, NEVER `@throws IAE`. AnyAppend/AnyDelete
  DO check via `Mutation.add`; `AnyIncrement.addColumn` checks null only — deliberate asymmetry.
- equals/hashCode: Get/RowMutations/Increment are row-key-ONLY; Scan/Query/OperationWithAttributes
  have no equals → reference IDENTITY. `compareTo` is row-key-only, deliberately inconsistent
  with equals.
- Coprocessor exception-handling divergence is SIGNATURE-driven (the no-callback
  `coprocessorService` declares only `throws UncheckedIOException` so it wraps; the other three
  declare `throws Exception` and rethrow checked as-is; Error passthrough added where missing).
- `toEntity` EMPTY_QUALIFIER fallback is bean-typed-only (fixed); byte[]/ByteBuffer single-value
  props get RAW cell bytes; live-map exposure is a documented contract; Append-vs-Increment
  `setTimeRange` asymmetry mirrors the driver; `AnyScan` stores `readVersions < 1` as-is (a
  documented divergence from AnyGet, pinned by a test).
- `getFingerprint`: Scan puts the String `"ALL"` when familyMap is empty; Get/Mutation always put
  a List — the entry is always present.
- `AnyPut` deliberately never shares/interns qualifier byte arrays (live-map corruption risk) —
  the old "pool/interning" prose was removed as false; do not re-add it.
- `Mapper.put(Collection)` resolves to `AnyPut.create(Collection, NamingPolicy)` — correct.
- Any* wrappers cannot be passed to native HBase batch APIs (RequestConverter rejects non-driver
  types); `Row` is implemented only for `getRow()`/ordering.

### BigQuery / Cosmos / Neo4j
- BigQuery DML uses the POSITIONAL `PSC`/`PAC`/`PLC` builders (fixed — named `:param` SQL cannot
  bind); SELECT alias backtick rewrite verified against real BigQuery; the IAE-vs-NPE split
  between select-vs-exists/delete is verified correct BOTH ways; null non-key props are excluded
  from SET; a keyless class → `SELECT *` for exists; `QueryParameterValue` has only BOXED
  int64/float64 overloads and `Byte/Short/Integer→intValue()` / `Long→longValue()` is lossless;
  nested rows use `field.getSubFields()`.
- Cosmos: `COSMOS_ALIAS = "c"` qualification is settled (bare identifiers are invalid Cosmos SQL);
  Condition-based `streamItems` null targetClass → IAE from `prepareQuery` on the no-projection
  path / possible NPE on the projection path — docs already reflect this; the SDK-delegated-NPE
  convention holds for the rest; `setMaxItemCount` is package-private in azure-cosmos (examples
  removed — do not re-add); `replaceItem(oldItemId, pk, newItem, opts)` → SDK
  `replaceItem(newItem, oldItemId, pk, opts)` reorder is CORRECT (javap-verified).
- Neo4j: `closeSession` LOOPS the nested-tx unwind (OGM "extended" transactions need one
  rollback/close pair per nesting level); pool poll/offer are non-blocking; OGM's no-depth
  `loadAll` default depth is 1 (loads immediate relationships — "properties only" is WRONG);
  queries execute EAGERLY and only iteration is lazy; constructor-only validation is
  SDK-delegated by design; OGM logs at DEBUG level.

### Naming (settled policy — see each executor's class Javadoc)
Two-tier house-vs-driver policy: house APIs follow abacus conventions (`gett`, `onlyIf`,
`queryForXxx`); driver-mirroring wrappers keep driver names. `iF` removed; `objectIdToFilter`,
`asProps`, `toRowKeyBytes` renames are done. Major-version naming items are user-DEFERRED — do
not re-propose them.

## Reporting format
For each finding: `[WS#][P1|P2|P3] file:line — one-line claim`, then evidence (code excerpt, the
exact failing scenario or the contradicting doc sentence, and HOW you verified it), then the
minimal suggested fix. End with a per-file verdict list: CLEAN, or the findings.
Be explicit about confidence and about anything you could not verify.
You are READ-ONLY — do not edit anything, do not run maven.
