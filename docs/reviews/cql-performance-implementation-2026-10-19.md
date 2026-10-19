**CqlBuilder and ParsedCql performance improvements, 2026-10-19**

Implemented the local changes for F1, F4-F12, and the monitor-removal part of F2 from the [review](cql-performance-review-2026-10-19.md). The public method signatures, parameter order, normalization, literal handling, and malformed-input behavior are preserved. Cache admission now has an explicit input-length limit, so large statements are parsed without a cache lookup or insertion. The same-key recheck/publication step proposed by F2 is not implemented: reference identity is no longer guaranteed and is documented as such on `ParsedCql.parse(String)`.

| Area | Change |
|---|---|
| Batch modifiers (F1) | TTL, TIMESTAMP, and IF NOT EXISTS clauses are added with a forward pass over the existing buffer. The replacement is rendered before updating the live buffer. This removes repeated suffix shifting, per-row insertion indexes, and per-row prefix strings. |
| Clause checks (F11) | Quote/comment-aware scans accept CharSequence and operate directly on the live builder, avoiding full String snapshots. |
| Cache concurrency (F2) | Cold parsing no longer runs inside a global monitor; the lock that serialized unrelated misses is gone. Publication is not atomic: `parse` looks up, parses on a miss, then puts the result without rechecking, so concurrent misses for the same key may each parse and return distinct but equal instances, and the last `put` wins. Cache hits still take the pool's own lock. |
| Cache memory (F6) | Retention is bounded by entry count (1,000) and by an admission limit on the raw input length (4,096 UTF-16 code units, measured before trimming). Inputs above the limit still parse normally but bypass both the lookup and the insertion. The pool is created without a memory measure, so there is no byte budget and no per-entry (1 MiB) estimate. Within the limits, retained key text is bounded at roughly 1,000 x 4,096 characters; key semantics and 24-hour expiration remain intact. |
| Token splitting (F4) | A token reader consumes pending spans without copying or modifying the tokenizer's list and without shifting unconsumed tokens. |
| MyBatis markers (F5/F12) | Complete markers use a direct substring path. Multi-token markers inspect each incoming token for the closing brace. Adjacent markers use offsets instead of repeatedly copying the remaining text. |
| Brackets and DDL (F7/F10) | No bracket mask is selected when brackets are absent. Leading initials can rule out DML for CREATE/ALTER and similar statements before preprocessing/tokenizing the body. Possible DML initials (including DROP/USE) and unusual prefixes fall back to the existing tokenizer; bracket-mask exhaustion still fails as before. |
| Retained state (F8/F9) | Named-parameter maps are allocated only when needed; empty results share ImmutableMap.empty(). Equal normalized/original text shares one String. The equality check reduces retained memory, although constructing the temporary normalized output still allocates. |
| Other allocation (F12) | Restored token spans append directly to the result buffer; placeholder numbers are parsed from source ranges. The hash computation preserves its previous value without the varargs call. |

Retention is bounded by entry count and by input length rather than by a byte measure: `PoolFactory.createKeyedObjectPool(1000, EVICT_TIME)` installs no `MemoryMeasure`, so the 64 MiB budget and the 1 MiB per-entry estimate described in F6 are not implemented. With the 4,096-character admission limit, at most roughly 1,000 x 4,096 characters of raw key text plus the parsed metadata can be retained. The limits are documented on `ParsedCql` and apply only to retention. No global interning or unbounded secondary cache was introduced.

**Measured changes.** These are before/after medians from the same local diagnostic harness on Java 21.0.11. They demonstrate allocation and scaling improvements; they are not JMH results or production throughput guarantees. Batch measurements exclude setup and build. Parser measurements bypass the cache using the private constructor.

| Workload | Before | After |
|---|---:|---:|
| Add TTL, 4,000 batch rows | 4.868 ms | 0.792 ms |
| Add IF NOT EXISTS after TTL, 4,000 rows | 6.072 ms | 0.961 ms |
| Add TTL, 16,000 batch rows | 85.673 ms | 2.557 ms |
| Add IF NOT EXISTS after TTL, 16,000 rows | 121.397 ms | 3.256 ms |
| Combined modifier allocation, 16,000 rows | 6,169,488 B | 4,177,776 B |
| Parse 16,000 glued MyBatis literal fields | 14.261 ms; 9,046,544 B | 3.732 ms; 5,350,176 B |
| Parse one MyBatis marker with 16,000 metadata fragments | 70.384 ms | 1.149 ms |
| Parse 100,081-character CREATE FUNCTION | 57.500 us; 201,664 B | 3.062 us; 32 B |
| Cold parse allocation, small positional query | 776 B | 586 B |
| Cold parse allocation, small MyBatis query | 1,192 B | 888 B |
| Reachable parsed graph, 100,000-character literal | 200,288 B | 100,232 B |

Small-query timing results were mixed and sensitive to warmup. A follow-up paired probe alternated baseline/current trials in the same JVM after one million warmup calls per implementation, using 11 trials of 100,000 calls each, a fixed 256 MiB heap, and synchronous JIT compilation (-Xbatch). It measured positional queries at 0.477 -> 0.461 us, native-named queries at 0.530 -> 0.577 us, and MyBatis queries at 0.624 -> 0.608 us. The native-named sample is slower despite lower allocation, so these changes do not establish a general short-query throughput gain. Cache hits bypass the constructor work measured here. A forked JMH benchmark is needed for reliable conclusions at this scale. The large-batch and long-marker measurements support the removal of quadratic work. Synthetic large batches/UDTs are scaling probes, not recommended Cassandra workloads.

Reachable graph figures include shared singleton objects and are not exclusive retained sizes from a heap dominator analysis. The empty metadata graph is now shared instead of separately allocated for every parsed object. Original and normalized strings share identity when their contents match.

**Validation.** The changed production classes and all focused CqlBuilder/ParsedCql tests were compiled against installed abacus-query 4.9.5 and abacus-common 8.2.1 and run with the JUnit Platform launcher: all 729 focused tests pass. The untracked regression files contribute 18 test executions (9 methods) covering large batch output and binding order, modifier ordering/failure preservation, dense/glued/long markers, private-use characters, DDL comments, shared empty state, and cache admission. Publication is no longer guarded by a monitor, so the former lock regression no longer applies; the concurrent same-key path is documented on `ParsedCql.parse(String)` and is not covered by a test.

A deterministic differential probe compared 37,703 inputs against the saved baseline classes in a separate class loader and found zero differences. It checks original text, normalized text, parameter count and names, hash values, and exception types/messages. Inputs include comments, Unicode/whitespace prefixes, native/positional/MyBatis markers, nested literals, dollar strings, and malformed fragments.

The normal offline Maven build is unavailable because central-publishing-maven-plugin:0.11.0 is not installed locally. Validation therefore used direct javac compilation and the JUnit Platform launcher; supporting repository classes came from existing compiled output. No Cassandra server was contacted. A complete Maven reactor build remains unverified.

**Remaining dependency work (F3).** Repeated SET calls still take full rollback snapshots in AbstractQueryBuilder. Its efficient journal path is private and limited to SqlBuilder, so safely extending it requires a coordinated abacus-query change. CqlBuilder retains the existing rollback guarantees. Callers that already have all values can use a bulk set(Map) operation to avoid the repeated-call cost.

Implementation locations: [batch rewriting](../../abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CqlBuilder.java), [parser/cache](../../abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/ParsedCql.java), [builder regression tests](../../abacus-da-all/src/test/java/com/landawn/abacus/da/cassandra/CqlBuilderPerformanceRegressionTest.java), and [parser regression tests](../../abacus-da-all/src/test/java/com/landawn/abacus/da/cassandra/ParsedCqlPerformanceRegressionTest.java).

Local evidence is under ignored target/cql-perf-review: implementation-tests.log, parser-differential.log, implementation-parser-probe.log, implementation-batch-probe.log, implementation-additional-probe.log, and paired-small-parser-final-xbatch.log. The baseline logs have a baseline- prefix. PerfReviewProbe.java, ParserDifferentialProbe.java, ParserPairedProbe.java, ReviewTestRunner.java, and RunReview.ps1 contain the diagnostic tooling. These local outputs are removed by a normal clean and are not permanent benchmark infrastructure.
