**CqlBuilder and ParsedCql — performance and memory review, 2026-10-19**

This document records the baseline before implementation; its source line numbers refer to that baseline. See the [implementation results](cql-performance-implementation-2026-10-19.md) for the subsequent changes and validation.

The largest opportunities are eliminating quadratic copying, shortening the parser's global critical section, and reducing the memory retained by its cache. Small keyword and flag checks are secondary. No production implementation was changed during this review.

The review covers every executable section of `CqlBuilder.java` (3,481 lines) and `ParsedCql.java` (1,302 lines), including their nested classes. It also follows the relevant executor, tokenizer, rollback, immutable-map, and object-pool implementations. The installed `abacus-query:4.9.5` and `abacus-common:8.2.1` source jars were compared with the sibling source checkouts; all inspected dependency files matched. Repository HEAD was `507f29b1ac65f589b173135f83f3ec173392ff80`. Neither reviewed production file had a working-tree diff.

The priorities below describe optimization impact for the stated workload, rather than correctness defects. Timings are local diagnostic measurements, not production throughput promises.

| Priority | Finding | Main workload |
|---|---|---|
| High | F1. Repeated batch-buffer insertion is quadratic | Large batches with TTL, TIMESTAMP, or IF NOT EXISTS |
| High | F2. A global monitor serializes all cache misses | Concurrent cold, dynamic, or expired queries |
| High, dependency change | F3. CQL builders miss the parent's efficient rollback path | Repeated SET calls and large binding histories |
| Medium | F4. Splitting tokens shifts the remaining token array | Many glued MyBatis markers |
| Medium | F5. MyBatis parsing repeatedly rescans accumulated text | Long multi-token or unterminated markers |
| Medium | F6. Cache capacity limits entries, not bytes | Large or high-cardinality query strings |
| Medium | F7. Mask/tokenizer construction runs without brackets | Queries containing U+E000 or U+E001 |
| Medium, small change | F8. Empty named-parameter maps are retained per entry | Positional-only and parameterless statements |
| Medium, small change | F9. Equal original and normalized strings are duplicated | Already-normalized positional statements |
| Medium | F10. Non-DML statements are fully preprocessed unnecessarily | DDL, especially large function bodies |
| Medium | F11. Clause checks copy and rescan the whole builder | Long UPDATE/DELETE/INSERT statements |
| Lower; measure first | F12. Token and restoration intermediates add allocation | Cold parsing generally |

**F1. Render batch modifiers without repeated insertion into the completed buffer.**

Locations: [CqlBuilder.java:711](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CqlBuilder.java:711), lines 721–723 and 903–917.

`appendUsingOption()` and `appendIfClause()` find the position in every batch row and then call `_sb.insert(...)` in reverse order. Reverse order preserves the recorded indexes, but each insertion still shifts the entire suffix, including modifiers already inserted into later rows. For R fixed-width rows, the total characters moved grow as O(R²). Adding both TTL and IF NOT EXISTS performs this process twice. The methods also allocate a complete CQL snapshot, boxed integer indexes, and, for USING, a prefix list and per-row concatenated strings.

Measured with two columns per row, excluding batch setup and final `build()`:

| Rows | Add TTL | Add IF NOT EXISTS after TTL | Combined modifier allocation |
|---:|---:|---:|---:|
| 1,000 | 0.538 ms | 0.684 ms | 383,408 B |
| 4,000 | 4.868 ms | 6.072 ms | 1,503,240 B |
| 16,000 | 85.673 ms | 121.397 ms | 6,169,488 B |

Build the modified statement in one forward pass, appending source spans and modifiers into a second buffer, then publish it after successful rendering. That changes each modifier pass to O(statement length + inserted text). A larger redesign can retain options as state and emit each row once. Preserve clause ordering, both call orders, quotes, dollar quotes, comments, raw expressions, named-marker numbering, and failure rollback. These large batches are scaling probes, not a recommendation to send batches of that size to Cassandra.

**F2. Move expensive parsing out of the global cache-miss critical section.**

Location: [ParsedCql.java:1042](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:1042), especially lines 1045–1050.

`new ParsedCql(cql)` executes while holding the single static `PARSE_LOCK`. A miss for one statement therefore blocks misses for every unrelated statement. Long literals, the quadratic paths in F4/F5, high query cardinality, and cache expiry all increase this delay. The expensive parse is serialized even when keys differ.

Parse outside the monitor, then recheck and publish inside a short critical section. Return the already-published instance if another thread won, preserving same-key cache identity at the expense of occasional duplicate parsing. If duplicate work is unacceptable, use per-key loading coordination or a bounded loading cache. Do not replace the pool with an unbounded concurrent map.

Cache hits do **not** acquire `PARSE_LOCK`. They do acquire the pool's own lock: `GenericKeyedObjectPool.get()` lines 827–859 checks expiry and updates access metadata under that lock. Thus changing the miss monitor alone does not make the hit path lock-free. Measure hit and miss contention separately before replacing the cache implementation. No concurrent throughput benchmark was run for this finding; the serialization is established by the source.

**F3. Extend efficient rollback to the CQL builder, while preserving subclass guarantees.**

Locations: [CqlBuilder.java:207](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CqlBuilder.java:207), calls at 255, 335, 1078, 1139, 1440, 1575, and 1603. The principal cause is `AbstractQueryBuilder.java` lines 6886–6923 and 7403–7411 in abacus-query 4.9.5.

The parent's mutation journal is enabled only when `getClass() == SqlBuilder.class`. `CqlBuilder` always misses that path. Each inherited structured mutation therefore snapshots accumulated parameters and named-parameter collections. Repeated `set(column, value)` calls copy histories of size 0, 1, 2, …, producing quadratic allocation. Additionally, the protected `mutateAtomically(Runnable)` used by CQL-specific methods always requests a complete text snapshot; `onlyIf(...)` copies an already-built WHERE clause even though its normal rendering only appends.

Measured end-to-end construction of a 2,000-column UPDATE, with input names/maps prepared outside measurement:

| Policy | Repeated single-column SET | One bulk SET |
|---|---:|---:|
| Positional | 4.032 ms; 9,002,240 B | 0.284 ms; 218,912 B |
| Named | 15.381 ms; 33,317,296 B | 0.610 ms; 497,024 B |

Bulk `set(Map)` is an immediate caller-level improvement when semantically suitable. The library fix belongs primarily in abacus-query: provide an explicit, protected append-only transactional capability and a safe way for the standard CQL implementation to opt into the journal. Retain full snapshots for arbitrary subclasses and custom parameter handlers that can alter previous text or state. Simply deleting checkpoints would break the existing retry-after-failure contract.

**F4. Avoid inserting split tokens into the middle of an ArrayList.**

Location: [ParsedCql.java:545](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:545), particularly lines 558–559; callers at 276 and 317.

`words.add(index + 1, ...)` shifts every remaining token. Repeating this for many fields such as `field:#{value}` creates O(S × T) reference movement for S splits and T tokens; both can grow with statement size. For 16,000 synthetic UDT fields, the glued form took 14.261 ms versus 4.735 ms for the spaced form, despite the spaced input being longer. At 1,000 fields the difference was much smaller: 0.567 versus 0.514 ms. These are lexical stress inputs, not validated Cassandra schemas.

Process a pending token suffix before advancing the original token index, or split into a second list in a single forward pass. A token cursor with a small pushback buffer is another option. Preserve state updates exactly once per consumed span, especially for a marker spanning multiple tokens and a second marker after its closing brace. A linked list would avoid array shifts but add node allocations and make the current indexed access expensive; it is not a suitable drop-in fix.

**F5. Search only newly appended text when assembling a MyBatis marker.**

Location: [ParsedCql.java:304](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:304), especially line 311, with a second search at 327.

The loop calls `ibatisTokenBuilder.indexOf("}")` from the beginning after every appended token. A marker split over many comma/whitespace tokens repeatedly scans the same growing prefix, making the search quadratic. An unterminated marker pays this cost before throwing. Adjacent markers in a single token also repeatedly copy the remaining tail through `toString()` and `substring()`.

For the parser-accepted stress form `#{id,x,x,...,last}`, increasing the repeated attribute fragments from 1,000 to 4,000 to 16,000 produced 0.384 ms, 2.405 ms, and 70.384 ms. The largest statement was only 32,035 characters and had one actual parameter. This form tests the parser's documented behavior of discarding text after the first comma; it is not a claim that such metadata is useful in MyBatis.

Track a search offset or examine each incoming token for the first closing brace, then retain source indexes rather than copying successive suffixes. Since the delimiter is one character, no multi-character delimiter overlap is needed. Keep malformed-marker errors, property expressions such as `ids[0]`, trimming, and literal-state handling unchanged.

**F6. Give the parse cache a byte budget and an oversized-input admission rule.**

Locations: [ParsedCql.java:146](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:146), lines 154, 224, 1042, and 1050.

The cache is bounded at 10,000 entries with a 24-hour lifetime, but uses the pool overload without a memory measure. It can retain large raw keys, trimmed originals, normalized strings, and parameter maps. A single parsed ASCII statement containing a 100,000-character literal had a reachable object graph of 200,288 bytes in the probe, before the cache wrapper and cache node. Ten thousand similarly sized, distinct entries would represent about 1.87 GiB, before that overhead. This is a conditional size estimate, not an observed production heap.

The raw key also retains surrounding whitespace that `originalCql()` discards. Padding variants produce equal ParsedCql values but distinct cached instances, which the probe confirmed. Unique inline timestamps or literal values likewise produce separate cache entries even when the executable query shape is otherwise stable.

Use the pool's existing memory-measured overload or another bounded cache, and bypass retention for oversized/one-off statements while still parsing them. Account for the raw key separately when it differs from the stored original. Making capacity, lifetime, and admission configurable would let applications choose their tradeoff. Trimming keys is a separate, smaller option: it preserves the currently returned original text, but changes the documented raw-key caching behavior and needs a deliberate compatibility decision. Do not canonicalize internal whitespace, comments, or semicolons in keys without addressing `originalCql()` and equality semantics.

**F7. Skip bracket-mask selection when there are no brackets.**

Locations: [ParsedCql.java:233](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:233) and lines 889–907.

`BracketMask.of(this.cql)` runs before `hasBrackets` is computed. If the statement contains U+E000 or U+E001, even inside an ordinary quoted literal with no brackets anywhere, it allocates a 65,536-bit BitSet, scans the statement, and builds a complete replacement tokenizer. That BitSet alone contains 8 KiB of long-array payload.

Two otherwise equivalent 30-character queries containing quoted U+E002 and U+E000 allocated approximately 928 and 15,784 bytes per cold parse respectively. The measured times were 2.154 and 13.628 microseconds. No bracket masking was needed in either query.

Determine whether masking is needed first. Use a shared CQL tokenizer without mask separators for the no-bracket path, retaining the CQL-specific operator changes. Blindly selecting `DEFAULT_BRACKET_MASK.tokenizer` could instead split a real private-use character outside quotes; avoid introducing that behavior. Preserve collision checking when brackets really are masked.

**F8. Allocate named-parameter storage only when a named marker is found.**

Locations: [ParsedCql.java:225](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:225) and 416.

Every constructor creates a HashMap and calls `ImmutableMap.wrap(...)`, including positional-only and DDL statements. In the installed dependency, wrapping an empty non-null map does not return the shared empty map: it creates wrappers around that map. The probe confirmed distinct empty maps for separate parsed objects and a 112-byte reachable map graph per instance. At 10,000 positional-only cached entries, that is roughly 1.07 MiB of avoidable object graphs, plus cold-parse allocation/GC overhead.

Keep the local map null until the first named marker, and use `ImmutableMap.empty()` when no names were collected. This preserves the public return type and immutability. For unusually wide named statements, dense zero-based positions make an array/list-backed representation worth investigating, but preserving the public ImmutableMap API requires more design. The executors primarily perform `get(i)` lookups, so hash nodes and boxed indexes are not intrinsically required by their binding loop.

**F9. Share the original string when normalization makes no change.**

Location: [ParsedCql.java:406](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:406), related fields at 181–183.

For an already-normalized positional query, the token loop reconstructs an equal string through `sb.toString()`. Both `cql` and `parameterizedCql` retain independent String/backing-array objects. The probe confirmed equality without identity for `SELECT * FROM t WHERE id = ?`, and the 100,000-character literal case retained both copies.

Prefer tracking whether any rewriting or normalization occurred and reuse `this.cql` when it did not. A smaller change can compare the final normalized string with the original and retain the original on equality, trading another O(length) comparison on a miss for lower retained memory. That smaller change still allocates the temporary output; it only reduces retention. Avoid global `String.intern()` for arbitrary query text.

**F10. Recognize non-DML statements before preprocessing their entire bodies.**

Locations: [ParsedCql.java:233](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:233), lines 244–248 and 411–413.

The constructor removes slash comments, masks strings/brackets, tokenizes the entire statement, and copies the token list before testing the first meaningful token. If the prefix is CREATE, ALTER, DROP, or another non-DML operation, it discards that work and normalizes only the original statement boundaries. A synthetic 100,081-character CREATE FUNCTION with a dollar-quoted body allocated 201,664 bytes per cold parse, while reporting zero parameters and returning the original body.

Use a leading-token classifier that correctly skips CQL comments and whitespace, then take the non-DML path before masking/token-list creation. Reuse the existing lexical rules rather than testing `trim().startsWith("SELECT")`. Preserve leading-comment handling, exact keyword boundaries, case insensitivity, and the existing DDL normalization contract. An early exit also changes the incidental mask-exhaustion error behavior for pathological DDL inputs; decide and document that explicitly.

**F11. Scan the live query buffer instead of materializing it for every clause check.**

Locations: [CqlBuilder.java:708](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CqlBuilder.java:708), 797, 904, and 961.

`appendUsingOption()`, `checkWhereSpecifiedForIfClause()`, and `appendIfClause()` each call `_sb.toString()`. A normal `ifExists()` call first snapshots the query to find WHERE, then snapshots it again in `appendIfClause()` even though UPDATE/DELETE only need to append the suffix. `onlyIf()` similarly pays for the WHERE-check snapshot plus the inherited transaction snapshot described in F3. USING performs multiple quote-aware scans of the same text.

Adapt the scanner to StringBuilder/CharSequence or track reliable clause positions during rendering. Short-circuit `appendIfClause()` for UPDATE/DELETE before creating a string. For batch rewriting, one deliberate immutable snapshot may still be useful for the forward-pass algorithm in F1. Preserve the supported raw-fragment escape hatch and quote/comment handling; `_hasFromBeenSet` is not a replacement for knowing WHERE exists.

**F12. Reduce token-list and per-token intermediates after the algorithmic changes.**

Locations: [ParsedCql.java:247](C:/Users/haiyangl/Landawn/abacus-da/abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java:247), 304–351, 403, and 787–843.

The tokenizer already constructs a fresh ArrayList and token strings; the constructor immediately copies that list. Even a simple complete `#{id}` marker creates two temporary builders and intermediate strings. Restoration creates a builder and result string for a masked bracket/dollar/backslash token, then copies that result into the main output builder.

Introduce a direct-append restoration helper and a fast path for a complete, single-token MyBatis marker. Once F4 eliminates list mutation, avoid the second list or consider the dependency's token cursor. The cursor is not a drop-in replacement: whitespace normalization and the `-- Keep comments` behavior need explicit preservation. Reusing the current mutable tokenizer result also needs a clear API guarantee, rather than an unchecked assumption about future dependency versions. The allocations are evident from source, but their individual benefit was not isolated in a before/after benchmark.

The following tables record the remaining line-by-line assessment. Ranges group adjacent methods with the same performance conclusion; the individual executable paths were inspected.

| CqlBuilder lines | Assessment |
|---|---|
| 1–160 | Imports, documentation, and usage examples have no query-time cost. |
| 161–208 | Shared constants and per-builder booleans are reasonable. Construction inherits eager collections from the parent; lazy parent state is a secondary allocation project. |
| 210–383 | Batch `into` validates all rows and then renders all rows. Column names/table prefix are rendered repeatedly; caching the common rendered prefix per batch could help after F1. Parent staging already checks map key sets, so there is repeated validation, but removing it must account for protected mutable state and subclasses. Preserve atomic failure behavior. |
| 385–447 | Placeholder helpers delegate to QueryUtil's existing bounded/prebuilt strategy. No new cache is justified here. Output-size cost for huge counts is inherent. |
| 449–674 | TTL/timestamp overloads repeat some state checks and allocate numeric strings. These are minor next to buffer copying. Preserve millisecond/microsecond conversion and overflow validation. Stable bindable timestamp templates would also reduce cache cardinality, but a value-binding API must maintain lexical parameter order. |
| 675–814 | Main USING and WHERE-check costs are F1/F11. Numeric-option concatenation and successful-validation error-message construction are smaller candidates. |
| 819–873 | Flag checks and delayed UPDATE initialization are reasonable. Avoid forcing implicit SET rendering early; it would change replacement behavior. |
| 885–901 | Raw string escaping can append spans and doubled quotes directly instead of building an escaped String. Lower priority because raw DSL variants are deprecated; UUID emission is simple. |
| 903–1025 | Batch IF insertion and text snapshots are F1/F11. Scanner helpers are linear over visited text and correctly skip quoted/comment regions. Do not replace them with naive delimiter searches. |
| 1027–1252 | IF validation/order and rollback are necessary. F3/F11 describe avoidable work. Flag assignment after successful rendering is appropriate. |
| 1254–1337 | Deferring ALLOW FILTERING until build avoids repeated reordering. Parent build recycles the buffer in finally and clears staged properties/rows. |
| 1339–1407 | Entity SET adds key exclusions using a new set. Typical key counts are small; a specialized union/filter could reduce allocation, but caching exclusions must honor runtime key registration. |
| 1409–1848 | FROM overloads repeat state/table validation and can nest checkpoints. Checks are mostly allocation-free and table names short. Entity metadata association must remain atomic. The character scanner is preferable to regex/split. |
| 1850–1982 | Binary rendering is linear. IN validates nulls before rendering and preserves value order; keep this validation. Named IN parameters need distinct names. Junction rendering is linear in the tree, with stack depth proportional to nesting; consider an explicit stack only if extreme nesting is supported. |
| 1984–2042 | Alias validation is bounded scanning. Property filtering copies lists; `removeAll(keyPropNames)` can be O(properties × key count). A key-set lookup is useful only for unusually large composite keys; do not add a stale metadata cache. |
| 2057–2191 | `dslCache` contains the predefined constants only. Custom dialects are returned without insertion, so this is not an unbounded dialect-cache leak. Retain/reuse a custom Dsl at the caller if created often. |
| 2193–2214 | Factory cleanup invokes terminal build on failure. This can allocate/throw on the failure path, but it releases resources; a dedicated disposal hook would need parent support. |
| 2216–2545 | Input list/map snapshots preserve staging semantics. Batch staging retains one copied LinkedHashMap per row; compact column-plus-value-array storage could reduce peak memory without dropping snapshot guarantees. This requires coordination with the parent implementation. |
| 2547–2934 | UPDATE/DELETE factories are simple around metadata loading. See property filtering and inherited rollback above. |
| 2936–3178 | SELECT factories snapshot collections/aliases. Do not remove those snapshots solely to save allocation without preserving caller-mutation behavior. |
| 3180–3383 | selectFrom delegates and rejects unsupported shapes before construction where possible. No independent major hotspot. |
| 3385–3481 | count copies the one-element selection list, a small avoidable allocation. renderCondition appropriately reuses the common renderer and cleanup. |

| ParsedCql lines | Assessment |
|---|---|
| 1–143 | Documentation correctly distinguishes original and normalized forms; these semantics constrain cache normalization. |
| 144–190 | Cache policy/retained fields: F2/F6/F8/F9. The keyword set is only six elements; replacing it is low priority. |
| 192–247 | Constructor setup: F7/F8/F10/F12. `Objects.hash(cql)` uses a varargs call, but JIT escape analysis may remove the temporary array. If changed, `31 + cql.hashCode()` preserves the current single-element hash value; plain `cql.hashCode()` does not. |
| 248–290 | Per-token literal tracking includes several scans, with linear aggregate cost outside splitting. A unified lexer is a larger, higher-risk project. |
| 292–348 | MyBatis scanning: F5/F12. Parameter-order accounting and mixed-style detection must be preserved. |
| 350–403 | Native-marker rewriting creates substrings/concatenations. Direct span appending can reduce them; it must preserve trailing braces and literal separators. |
| 406–417 | Final strings/maps: F8/F9. Buffer recycling in finally is already correct. |
| 419–472 | Literal/container-state tracking is necessary. Balanced container pushes/pops do not by themselves demonstrate a quadratic problem. |
| 474–532 | Previous-significant-token lookup and embedded-marker scanning are reasonable. Carrying the previous significant token can remove backward lookups during a future streaming rewrite. |
| 535–613 | In-place token splitting: F4. Preserve quote awareness and brace depth. |
| 615–624 | Trailing-brace exclusion is a small linear suffix scan; no independent tuning priority. |
| 626–697 | Slash-comment removal has a useful absence fast path. If `//` occurs only inside literals, the current path still copies everything; allocating on the first actual removal could help. |
| 699–779 | String/bracket masking is necessary with this SQL tokenizer. Its full-text copy and private-use characters can widen otherwise ASCII intermediate text. A CQL-native bracket tokenization mode would be a larger dependency improvement. |
| 781–849 | Restoration intermediates: F12. Parse placeholder digits directly from a range instead of allocating a substring if profiling warrants it. |
| 851–930 | Mask collision handling: F7. Keep collision avoidance; an unbounded cache of arbitrary mask/tokenizer pairs would introduce another retention issue. |
| 932–984 | First-keyword checks avoid uppercase-string allocation. Six comparisons and bounded trim/substrings are secondary costs. |
| 986–1004 | Trailing-semicolon removal scans only the suffix and returns the same input if unchanged. Good existing behavior. |
| 1006–1060 | Cache lookup/publication: F2/F6. Do not weaken safe publication or same-key reuse in a concurrency change. |
| 1062–1220 | Accessors return immutable stored state without rebuilding it. Keep this behavior. |
| 1222–1302 | Hash/equality are consistent with trimmed original text. toString allocates only when requested; do not cache it and retain another full query copy by default. |

**Validation and measurement limits.** Both production classes and the current CqlBuilder/ParsedCql test sources were compiled into an isolated review output directory with Java 21.0.11 and the installed dependency versions. All **711 tests passed**, with zero skipped or failed tests. Supporting repository classes came from the existing compiled output. No Cassandra server was contacted.

The normal offline Maven command stopped before compilation because `central-publishing-maven-plugin:0.11.0` was unavailable. Direct compilation/test launching avoided that build-extension dependency. Windows sandbox canonical-path checks initially blocked Java, so the successful local validation used an approved execution outside that sandbox, writing review outputs only under the workspace.

Parser probes call the private constructor through a MethodHandle to isolate cold parsing from cache hits and eviction. Inputs are prepared outside measurement. Allocation uses `ThreadMXBean.getThreadAllocatedBytes`; object-graph sizes use Instrumentation and identity-based traversal. Graph sizes describe reachable objects, not a heap-dump dominator calculation. Batch probes measure the modifier calls only. UPDATE probes include builder construction and build, comparing repeated SET with bulk SET using prepared inputs. Results are medians after warmup; runs use a single JVM per probe mode, with no JMH forks or confidence intervals. In particular, do not infer that named parsing is faster than positional parsing from the small-query timings. The useful evidence is the allocation structure and scaling trend, supported by the source.

Local evidence: [probe source](C:/Users/haiyangl/Landawn/abacus-da/target/cql-perf-review/PerfReviewProbe.java), [runner](C:/Users/haiyangl/Landawn/abacus-da/target/cql-perf-review/RunReview.ps1), [test log](C:/Users/haiyangl/Landawn/abacus-da/target/cql-perf-review/tests.log), [parser measurements](C:/Users/haiyangl/Landawn/abacus-da/target/cql-perf-review/parser-probe.log), [batch measurements](C:/Users/haiyangl/Landawn/abacus-da/target/cql-perf-review/batch-probe.log), and [additional measurements](C:/Users/haiyangl/Landawn/abacus-da/target/cql-perf-review/additional-probe.log). These supporting files are under ignored `target/` and can be removed by a normal build clean. The runner covers the original tests/parser/batch modes; the additional mode is available as `PerfReviewProbe additional` on the same classpath.

For implementation, start with F1 and the small retained-memory changes F8/F9, then F2 and a bounded cache-admission policy. Address F4/F5 before a broader tokenizer rewrite. Coordinate F3 with abacus-query rather than recreating transactional bookkeeping in CqlBuilder. Keep the existing semantic suite, add focused regression coverage for changed rollback/normalization/concurrency behavior, and use JMH with allocation profiling for hot hits, unique misses, mixed concurrent hits/misses, positional/named/MyBatis inputs, brackets/comments/dollar strings, and growing batch sizes. Performance thresholds should not be enforced by fragile wall-clock unit-test assertions.
