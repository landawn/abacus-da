# Review round 2026-07-15 — shared briefing for review agents

You are a READ-ONLY line-by-line reviewer of `abacus-da-all/src/main/java` (repo root: `C:\Users\haiyangl\Landawn\abacus-da`). Do NOT edit any file. Report findings only.

## Scope (three workstreams)
- **WS1 BUG**: genuine correctness defects (wrong result, exception contract violated by code, data corruption, resource leak, thread-safety). Each finding MUST include a concrete failure scenario (inputs/state → wrong outcome) grounded in the actual code you read.
- **WS2 DOC**: Javadoc/comment defects — factually wrong claims (wrong `@throws` type, examples that don't compile or produce a different result, over/under-claiming), broken `{@link}`s, sibling drift (sync vs async vs reactive documenting different behavior for identical delegation). NOT style/wording preferences.
- **WS3 LOG/PERF**:
  - LOG: existing logger/exception MESSAGES that are wrong or misleading. Do NOT propose adding new logging (out of scope by prior user decision).
  - PERF: real inefficiencies — repeated work in loops that could be hoisted, unnecessary per-call allocation on hot paths, O(n^2) where O(n) is natural, redundant copies/conversions. Must not change public behavior. Micro-nits (single extra object per call on a network-bound path) are NOT findings unless trivially fixable with zero risk.

## Where new bugs are most likely (recent churn — extra scrutiny)
The last 4 commits (9bcd57a "Modernize query APIs + Cassandra batch support", a97c61e "harden database executors", 4034f47, ff955c1 "DynamoDB key/ID validation") changed ~2280 lines. Newest code = least reviewed:
- **DynamoDB v1+v2 sync+v2 async**: brand-new validation added at HEAD — `asKey` now requires exactly 1 or 2 name/value pairs, key values must be nonempty scalar String / finite Number / nonempty binary; `toAttributeValue` rejects NaN/Infinity; Mapper get/put/update/delete/batch* validate entity IDs (null/empty rejected); ID-only update rejected (empty attribute-update map); naming-policy composite-ID collisions rejected; 3-pair `asKey` overload deprecated + always throws. These are DELIBERATE new contracts — do not flag them as over-strict — but DO review the new validation code itself for defects (wrong boundary, wrong exception, broken valid-input path, sibling drift between the 3 executors).
- CqlBuilder (+521 lines), ParsedCql, CqlMapper, HBaseExecutor, both Mongo executors, CassandraExecutor v3/v4 — all had recent churn.

## Ground rules
- This codebase has had ~12 deep multi-agent review rounds in 2026 (all findings fixed & committed). It is MATURE — recent rounds found 0–3 real issues across all 51 classes. Expect to find NOTHING in most files; that is the correct outcome. Be false-positive-averse: a wrong finding costs more than a missed one.
- Before flagging, verify against the ACTUAL delegate/driver behavior when the claim depends on it (read the delegating code in this repo; driver jars are in `~/.m2`).
- Most files are thin faithful delegation wrappers around DB drivers — delegation itself is never a finding.
- Backward compatibility is REQUIRED: no public API renames/removals/signature changes. Naming-convention items were already reviewed and deferred to a major version — do not re-propose (see two-tier house-vs-driver naming policy documented in each executor's class Javadoc).

## Known non-bugs / prior decisions — DO NOT RE-FLAG (all verified in prior rounds)
General:
- Executors deliberately do NOT implement AutoCloseable; examples use explicit try/finally with close() (user decision). Do not re-propose `implements AutoCloseable`.
- `N.asMap(...)` in abacus-common returns ImmutableMap; sites wrapping it in `N.newLinkedHashMap(N.asMap(...))` are REQUIRED for the mutable contract — not a perf issue.
- Dead-but-harmless defensive code deliberately left: DDB v1/v2 dead-store `newQueryRequest` copies + dead `rowClass == null` branches; Cassandra `prepareStatement` dead `else if (isEmpty(parameters))` + redundant null ternary (symmetric v3/v4); Cosmos `toSqlQuerySpec` parameterization branch (callers use RAW_SQL); Cosmos `count>0 → limit` dead branch; BigQuery `entityToCondition` unreachable `if (N.isEmpty(conds))`.
- "Missing logging" suggestions are out of scope. The ~19 real logger statements across 11 files were audited correct on 2026-07-02.

Cassandra:
- Since commit `72a64fc`, v3+v4 `prepareStatement` keeps explicit null bind values as null AND defensively clones the caller's array — the OLD "null→defaultValueOf + caller-array aliasing" notes are stale; current docs match current behavior.
- `toList` Row-passthrough guard is `targetClass.isAssignableFrom(Row.class)` — CORRECT direction, do NOT flip. `toList(rs, Object.class)` returns raw rows by design.
- v4 missing `javaClass == null` guards that v3 has: unreachable (protocolCodeDataType is total).
- `execute(String)`/`queryForSingleValue(valueClass, query, params)`: null query → NPE without params / IAE with params — long-standing prepareStatement quirk, docs settled, don't churn.
- v3/v4 `toEntity` sets non-Row driver-decoded values as-is (only nested Row→bean converted) — intentional.
- Package gotcha: `cassandra/` = DataStax driver v4 (`com.datastax.oss.*`), `cassandra/v3/` = driver 3.x — opposite of dir-name intuition.
- CqlBuilder: IN/NotIn named-param sanitization happens inside setParameter (BETWEEN pre-sanitizes only because it builds a prefixed name) — no inconsistency. `Dsl` cache only hits for the registered SqlDialect instances (identity keying) — bounded, harmless. `selectFrom(Class, includeSubEntities=true)` multi-table FROM is SQL-parity-only, documented. `usingTimestamp` ArithmeticException documented.
- CqlBuilder string-select vs entity-select aliasing DIFFER by design (probe-verified): `select(String...)` aliases only policy-changed columns; `select(Class)`/`selectFrom(Class)` alias unconditionally. Both doc families byte-exact. Not inconsistent.
- CqlBuilder naming policy LOWERCASES selectModifier identifier tokens (`selectModifier("JSON")` → `SELECT json …`) — CQL keywords case-insensitive, not a bug.
- CqlMapper.load(String) missing file → descriptive RuntimeException — deliberate divergence from SqlMapper. CqlMapper public API deliberately locked to mirror SqlMapper (Map namedParameters, attrs-in-ParsedCql).
- Async Cassandra: prepare-time failures throw synchronously AT THE CALL SITE; driver-failures from the async task ARE ExecutionException-wrapped; `ContinuableFuture.map(...)` mapper exceptions rethrow UNWRAPPED from get(). Docs are correct — verify before flagging.
- `ContinuableFuture.thenRunAsync` HAS a Consumer overload — value-consuming examples are correct.

MongoDB:
- Sync `queryForSingleValue`: matched doc with missing field → PRESENT `Nullable.of(null)`; reactive → completes EMPTY. GENUINELY different, each doc correct for its variant.
- `MongoDBBase.objectIdToFilter` examples show `{"_id": {"$oid": "…"}}` — genuine `Document.toJson()` extended JSON. Recurring false positive (proposed & rejected 3×): do NOT change to `ObjectId(...)`.
- All Bson-filter read AND write methods guard `N.checkArgNotNull(filter)` (chokepoints: `executeQuery`, plus explicit guards on count/distinct/findOneAndDelete etc.) — guard placement is settled; don't propose moving them.
- `estimatedDocumentCount(null options)` = defaults (family convention). toDocument bean null-skip: known, left.
- Reactive `aggregate(List, Class)` null-pipeline IAE is EAGER (driver notNull before Flux.from) — docs correct.
- AsyncMongoCollectionExecutor deliberately lacks 6 sync-only conveniences (`stream()`, `stream(Class)`, typed `groupBy`/`groupByAndCount` ×4) — class doc names them; adding async parity is a DEFERRED user decision, do not add or re-propose.

HBase:
- `Any*` hashCode/equals delegate to wrapped HBase object; compareTo row-key-only & inconsistent with equals (mirrors HBase). Live-map exposure (`AnyMutation.getFamilyCellMap`, `AnyRowMutations.val`) documented contracts.
- NPE-vs-IAE per constructor is EXACT and settled (decompiled hbase-client): single-arg `Put/Delete(byte[])` deref row.length → NPE; `Get(byte[])` → checkRow → IAE; offset/length & boolean variants → checkRow → IAE; `AnyGet.of("abc",-1,2)` → AIOOBE. Do NOT flip any of these.
- `AnyPut.addColumn` family validation is PHANTOM (no client-side check; docs use @param caveats, not @throws) — do NOT re-add IAE claims. AnyAppend/AnyDelete DO check via Mutation.add; AnyIncrement.addColumn checks null only — deliberate asymmetry.
- Coprocessor exception-handling divergence is signature-driven (no-callback overload declares only UncheckedIOException, wraps; others rethrow) — docs accurate; `coprocessorService(Map)` has an Error passthrough.
- AnyScan redundant setColumnFamilyTimeRange overrides: known, left. AnyScan stores readVersions(<1) as-is (mirrors client), cross-referenced in doc.
- HBaseExecutor.toEntity EMPTY_QUALIFIER fallback is bean-typed-only — current form correct.
- `AnyOperation.getFingerprint`: Scan puts the String "ALL" (not a List) for "families" when familyMap empty; Get/Mutation always put a List — docs refined, correct.

DynamoDB:
- IAE-for-null `@throws` = established package convention (SDK-delegated, internally consistent) — do NOT mass-rewrite. `scan(String,…)` tableName IAE is lazy/SDK-side (consistent with lazy stream).
- list/query auto-pagination treats Limit as page size — documented.
- v2 async list/query/stream/scan blocking .get() pagination inside thenApplyAsync = accepted long-standing tradeoff (completes on commonPool; documented). NOT a bug; a non-blocking rewrite is out of scope.
- v2 async list/stream/scan(ScanRequest,Class) DEFER the documented IAE via the future — consistent with the "v2 async defers work" design.
- toEntity container rebuild (v1+v2): `isElementConversionNeeded(Type)` skips rebuild when element/value slot is Object (byte[] drift protection). REFUTED around it: NUL→primitive coalesces to default; bare scalar into collection prop is bracket-lenient; jsonXmlType never null; List<byte[]> BS round-trips. Extracting the duplicated v1/v2 block into a shared helper was DEFERRED by user — do not re-propose.
- v1 getItem null-targetClass tolerance is the documented v1 contract.
- v2 eager guards on async `list(QueryRequest,Class)` + sync/async `batchGetItem(BatchGetItemRequest,Class)` were added deliberately (billable-call protection).

BigQuery/Cosmos/Neo4j:
- BigQuery DML uses positional PSC/PAC/PLC builders — SELECT also positional; don't propose named. prepareQuery rewrites double-quoted SELECT aliases to backticks (BigQuery rejects double quotes) — deliberate.
- Cosmos `prepareQuery` uses alias `c` (required by Cosmos SQL); setMaxItemCount is package-private in azure-cosmos — examples deliberately omit it. The 4 Condition-based `streamItems` overloads document IAE (no-projection / null-empty selectPropNames) or NPE (projection + non-empty props, null class) — probe-verified split, settled.
- Neo4j: constructor-only validation is SDK-delegated by design; `closeSession` loops `while ((tx = session.getTransaction()) != null)` to unwind OGM extended transactions — deliberate (do not "simplify" to one rollback); non-blocking pool poll/offer deliberate; OGM no-depth loadAll default depth = 1 (docs corrected).
- N.convert(Object, Type) SHORT-CIRCUITS when raw container class assignable — sites using `type.valueOf(N.toJson(value))` do so DELIBERATELY to force element conversion.

Stubs: search/{Elasticsearch,Lucene,Solr}Executor, aws/{AWSRDSUtil,AWSS3Util}, hadoop/*, blink/*, spark/* are empty placeholder stubs (package-private explicit ctors are REQUIRED by committed tests — do not make private). Only confirm they're still trivial. `package-info.java` files are new — check their claims are accurate but expect prose only.

## Output format
Return a report (message text, not files):
1. Files reviewed, with line coverage confirmation.
2. Findings list: `[WS1|WS2|WS3][P1|P2|P3] file:line — one-sentence defect + concrete failure scenario / evidence + suggested fix`.
3. Explicitly list anything you SUSPECTED but rejected after verification (so the main agent knows it was considered).
If nothing found, say so plainly — that is an acceptable and expected outcome.
