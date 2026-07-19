# Review round 2026-07-18 — BRIEFING for read-only review agents

You are ONE read-only reviewer in a multi-agent full line-by-line review of
`abacus-da-all/src/main/java` (HEAD `97adef4` + the two uncommitted 2026-07-17 review rounds'
fixes in the working tree). The codebase has had MANY deep review rounds (2025-H1 through
2026-07-17 ×2); it is MATURE. Expect zero-to-few genuine findings. A finding must carry evidence
(file:line + the exact code + why it's wrong); speculative or style-only reports without a
concrete defect are noise.

## NEW THIS ROUND — dependency bump (the one fresh variable)
The poms were bumped to **abacus-common 7.8.8** (was 7.8.7) and **abacus-query 4.8.9**
(was 4.8.8), both present in `~/.m2/repository/com/landawn/abacus/...`. Any behavior in your
slice that leans on those libraries' internals (N.asMap immutability, N.convert short-circuit,
type.valueOf(N.toJson(...)) element rebuild, SqlBuilder/CqlBuilder-inherited AbstractQueryBuilder
behavior, SqlParser tokenization, ParserUtil/Beans reflection, checkArg* semantics) deserves a
spot-check against the NEW jars (javap -c -p, or extracting sources if the -sources jar exists).
Report a finding ONLY if the new version actually changes observable behavior of this module.

## Scope dimensions (report under these workstreams)
- WS1 bugs (correctness) — highest bar: show the failing input/state and wrong outcome.
- WS2 javadoc/comment errors — doc contradicts actual behavior (verify the enforcement path).
- WS3 exception/log message defects — wrong/misleading text in thrown messages or logs.
- WS4 performance — real, measurable waste on a hot path (not micro-nits on network-bound code).
- WS5 simplification — only if it removes genuine complexity with zero behavior change.
- WS6 naming consistency — report only NEW inconsistencies; the naming policy is settled (below).
- Edge-case test gaps — only where a REAL untested behavior boundary exists in code you can name.

## DO NOT RE-FLAG (settled, verified across prior rounds)

### Global / cross-cutting
- NO executor implements AutoCloseable — user-settled TWICE. Explicit `close()` + try/finally
  examples are the contract. Do NOT propose `implements AutoCloseable` or try-with-resources on
  executors. (try-with-resources on Stream/ResultScanner/Table/MongoCursor IS correct — leave.)
- Empty stubs (search/*, hadoop/*, blink/*, spark/*, aws/AWSRDSUtil, AWSS3Util) are deliberate
  placeholders. Ctors must stay PACKAGE-PRIVATE (tests instantiate same-package).
- Deliberately-left dead code: DDB dead-store `newQueryRequest` family + dead `rowClass == null`
  branches; BigQuery `entityToCondition` unreachable `isEmpty(conds)`; Cosmos
  `toSqlQuerySpec`/`rewritePositionalParameters` parameterization branch (callers use RAW_SQL).
- abacus-common `N.asMap(...)` returns ImmutableMap — existing sites wrap
  `N.newLinkedHashMap(N.asMap(...))`. Only flag a NEW unwrapped site whose result is mutated.
- `N.convert` short-circuits when raw container class is assignable → element conversion uses
  `type.valueOf(N.toJson(value))` idiom (DDB v1/v2 toEntity, BigQuery). The v1/v2 duplication of
  that block is user-DEFERRED — do not re-propose extraction. `isElementConversionNeeded` skips
  Object-slot rebuilds (byte[] preservation) — settled with tests.
- DDB package convention: `@throws IAE` for null args even where SDK-delegated; async variants
  defer documented IAE into the future (house convention). Do not mass-rewrite.

### Cassandra (NOTE: `cassandra/` = DataStax driver v4, `cassandra/v3/` = driver 3.x)
- `prepareStatement`: null query → IAE (checkArgNotNull since a97c61e); explicit null bind values
  stay null; caller's array defensively cloned (since 72a64fc).
- `toList` Row-passthrough guard `targetClass.isAssignableFrom(Row.class)` is CORRECT — never flip.
- `toEntity` sets non-Row driver-decoded values as-is (only nested Row→bean) — intentional v3+v4.
- `execute(Statement)` null → NPE (docs corrected); String overloads route through prepareStatement.
- v3/v4 `findFirst` null-targetClass documented-IAE-but-no-throw edge — left, consistent siblings.
- Async: prepare-time failures (malformed CQL etc.) throw AT THE CALL SITE (session.prepare is
  sync); only driver execution failures arrive ExecutionException-wrapped; `ContinuableFuture.map`
  mapper exceptions rethrow UNWRAPPED from get().
- CqlBuilder: 11 Dsl builders verified consistent repeatedly; string-select aliases only
  policy-changed columns while entity-select aliases unconditionally (both correct, different
  paths); naming policy lowercases selectModifier tokens (CQL case-insensitive); IN/NotIn
  named-param sanitization happens inside setParameter; BETWEEN pre-sanitizes because of its
  "min"/"max" prefix; `selectFrom(Class, includeSubEntities=true)` multi-table FROM documented as
  SQL-parity-only; `isBatchInsert(String)` perf hoist already applied; `select("expr AS alias")`
  keeps the alias VERBATIM (only expression identifiers normalized); the 3 ISE messages now use
  opCqlKeyword() (ADD→INSERT, QUERY→SELECT); the :1402 literal-normalization comment corrected.
- v4 class header: PreparedStatement-only cache (no BoundStatement pooling — deleted 4034f47).
- `CqlMapper.load(String)` missing file → descriptive RuntimeException (deliberate divergence from
  SqlMapper). ParsedCql curly-depth + in-quote tracking both fixed and tested.

### MongoDB
- EVERY Bson-filter read AND write method rejects null filter with IAE — one guard at the private
  `executeQuery` chokepoint (sync + reactive) + explicit guards on count/distinct/driver-direct
  methods; async inherits via delegation. Do NOT propose terminal guards or removal.
- sync `queryForSingleValue` matched-doc-but-missing-field → PRESENT `Nullable.of(null)` (and
  queryForSingleNonNull → NPE); reactive → completes EMPTY. GENUINELY different; each doc correct.
- `objectIdToFilter` examples show `{"_id": {"$oid": "…"}}` — genuine Document.toJson()
  extended-JSON. RECURRING false positive (proposed wrongly 2×) — never change to `ObjectId(...)`.
- `toBson` copies before stripping `_id`; empty-$set throws IAE; resetObjectId only converts
  24-hex String / 12-byte arrays. `estimatedDocumentCount(null options)` = defaults.
- sync `MongoCollectionExecutor` toBson rationale comment now states the accurate dead-flag
  justification (the "protected toDocument invisible" claim was corrected in r2 — sync only;
  the reactive sibling's identical-looking comment is CORRECT there, different package).
- findFirst terminals (Collection- and Bson-projection, sync + reactive) short-circuit
  `rowType.isAssignableFrom(Document.class)` → raw document identity, matching list/stream —
  INTENDED, 4 regression tests. Do not propose reverting.
- Async executor deliberately lacks 6 sync-only conveniences (stream(), stream(Class), typed
  groupBy/groupByAndCount ×4) — documented; adding them is a user-deferred API decision.
- Reactive `.map`→`.mapNotNull` migration complete. Reactive examples use `Flux.from(...)`
  (executor returns Publisher).

### DynamoDB
- list/query auto-pagination treats Limit as PAGE size — documented, intentional.
- v2 async list/query/stream/scan blocking `.get()` pagination inside thenApplyAsync = accepted
  tradeoff; scanFilter nullable; v1 getItem null-targetClass tolerance documented.
- toEntity container rebuild (v1+v2) + byte[]-preservation skip — settled with regression tests.
- v2 `toKeyAttributeValue` admits byte[] keys while v1 rejects all arrays — deliberate v2
  improvement, not drift.

### HBase (verified via hbase-client 2.6.x decompile — do not flip these)
- Ctor NPE-vs-IAE split: single-arg `Put(byte[])`/`Delete(byte[])` deref row.length → NPE;
  `Get(byte[])` routes checkRow → IAE; offset/length variants → IAE; `Get(row,-1,2)` → AIOOBE.
  `Mutation(row,ts,familyMap)` ctor → NPE null row AND null familyMap, IAE empty row.
- `AnyPut.addColumn` family validation is PHANTOM (no client-side check) — @param caveats only,
  never `@throws IAE`. AnyAppend/AnyDelete DO check via Mutation.add; AnyIncrement.addColumn
  checks null only — deliberate asymmetry.
- equals/hashCode: Get/RowMutations/Increment row-key-only; Scan/Query/OWA identity. compareTo
  row-key-only, deliberately inconsistent with equals.
- coprocessor exception-handling divergence is signature-driven (one overload declares only
  UncheckedIOException); Error passthrough added where missing.
- `toEntity` EMPTY_QUALIFIER fallback bean-typed-only (fixed); live-map exposure documented;
  Append-vs-Increment setTimeRange asymmetry mirrors driver; AnyScan stores readVersions <1 as-is.
- `getFingerprint`: Scan puts String "ALL" when familyMap empty; Get/Mutation always put a List.
- `Mapper.put(Collection)` overload resolution → `AnyPut.create(Collection,NamingPolicy)` — correct.

### BigQuery / Cosmos / Neo4j
- BigQuery DML uses positional PSC/PAC/PLC (fixed); SELECT alias backtick rewrite verified vs
  real BigQuery; IAE-vs-NPE split between select-vs-exists/delete verified correct BOTH ways;
  null non-key props excluded from SET; keyless exists → SELECT *; QueryParameterValue has only
  BOXED int64/float64 overloads — Byte/Short/Integer→intValue()/Long→longValue() is lossless.
- Cosmos: `COSMOS_ALIAS = "c"` qualification settled; Condition-based streamItems null targetClass
  → IAE from prepareQuery (no-projection) / NPE possible (projection path) — docs correct;
  SDK-delegated-NPE convention for the rest; setMaxItemCount package-private (examples removed);
  `replaceItem(oldItemId, pk, newItem, opts)` → SDK `replaceItem(newItem, oldItemId, pk, opts)`
  reorder is CORRECT (javap-verified).
- Neo4j: closeSession LOOPS the nested-tx unwind (OGM extended transactions); pool poll/offer
  non-blocking; OGM no-depth loadAll default depth = 1 (loads immediate relationships); query
  executes eagerly, only iteration lazy; constructor-only validation is SDK-delegated by design.

### Naming (settled policy — see executor class Javadocs)
Two-tier house-vs-driver policy: house APIs follow abacus conventions (gett, onlyIf, queryForXxx);
driver-mirroring wrappers keep driver names. `iF` removed; `objectIdToFilter`, `asProps`,
`toRowKeyBytes` renames done. Major-version naming items are user-DEFERRED — do not re-propose.

## ALREADY FOUND+FIXED THIS ROUND (2026-07-18) — verify, don't re-flag
- **abacus-query 4.8.9 made `AbstractQueryBuilder.from(...)` SELECT-only** (new private
  `checkCanAppendFrom` gate: op==QUERY, !_hasFromBeenSet, columns-set). This broke CQL's legal
  `delete(cols).from(...)` chain (CQL supports `DELETE col1, col2 FROM tbl`): the Class-typed
  overloads `from(Class)`/`from(Class,alias)`/`from(expr,Class)` threw ISE. FIXED in local
  CqlBuilder: those 3 overloads are now overridden with a CQL-aware `checkCanAppendCqlFrom()`
  (SELECT or DELETE + closed/double-from/columns checks), and the local
  `appendOperationBeforeFrom` gained the same double-from guard + CQL-keyword message.
  The String-typed `from(String)`/`from(String...)`/`from(Collection)` overloads were never
  gated (they funnel to the protected `from(String,String)` → overridden
  `appendOperationBeforeFrom`). `deleteFrom(...)` never calls `from()` (sets _tableName directly).
  Do NOT propose removing these overrides as redundant.

## Reporting format
For each finding: `[WS#][P1|P2|P3] file:line — one-line claim`, then evidence (code excerpt,
the exact failing scenario or contradicting doc sentence), and the minimal suggested fix.
End with a per-file verdict list: CLEAN or findings. You are READ-ONLY — do not edit anything.
