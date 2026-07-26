# Review round 2026-07-26 — BRIEFING for read-only review agents

You are ONE read-only reviewer in a multi-agent full line-by-line review of
`abacus-da-all/src/main/java` (67 units, ~80k lines). HEAD is `7b33836` and the
working tree is CLEAN. This codebase has had MANY deep review rounds (2025-H1
through 2026-07-25); the 2026-07-25 round alone ran 21 reviewers and fixed the
last 5 real defects. It is MATURE. Expect zero-to-few genuine findings. A
finding must carry evidence (file:line + the exact code + why it's wrong);
speculative or style-only reports without a concrete defect are noise.

**Review the WORKING TREE (current file contents), not any historical commit.**

## MANDATORY FIRST STEP
Read `scripts/review_2026-07-25/BRIEFING.md` IN FULL before reviewing anything.
Its "DO NOT RE-FLAG" section is the accumulated settlement list from every prior
round — violating it is the #1 way to produce noise. Everything in it still
applies verbatim.

## Additional settled items from the 2026-07-25 round (do NOT re-flag)
- `cassandra/v3/CassandraExecutor` `LocalDate.fromMillisSinceEpoch(...)` sample —
  driver 3.x LocalDate has no `now()`/`minusDays()`; current form is correct.
- CqlBuilder `insertInto(Class)` / `selectFrom(Class)` emit ALL non-transient
  columns (no exclusion) — probe-verified byte-identical to `insert(Class).into(...)`.
- The three `USING TIMESTAMP <microseconds>` outputs are intentional placeholders
  for a time-dependent value, not literal assertions.
- Doclint over all 67 files is CLEAN (0 findings) — do not report missing-javadoc
  on protected members; the 22 were documented.
- All 3481 sample call sites + 235 constant references were checked reflectively
  against the real classpath on 07-25; all 50 CqlBuilder expected-CQL assertions
  were executed. Re-verify only what you suspect, not everything by rote.

## Scope dimensions (report under these workstreams)
- WS1 bugs (correctness) — highest bar: show the failing input/state and wrong outcome.
- WS2 javadoc/comment errors — doc contradicts actual behavior (verify the enforcement path).
  **Includes: every code sample must COMPILE conceptually and produce the asserted values.**
- WS3 exception/log message defects — wrong/misleading text in thrown messages or logs.
- WS4 performance — real, measurable waste on a hot path (not micro-nits on network-bound code).
- WS5 simplification — only if it removes genuine complexity with zero behavior change.
- Edge-case test gaps — only where a REAL untested behavior boundary exists in code you can name.

## How to verify (this repo's proven techniques)
- Driver/library ground truth: jars are in `~/.m2/repository`; use `javap -c -p`
  on the relevant class, or extract a `-sources` jar. Versions in play:
  hbase-client 2.6.4, azure-cosmos 4.81.0, google-cloud-bigquery 2.67.0,
  neo4j-ogm 5.0.7, DataStax driver v4 (`cassandra/`) and 3.11.x (`cassandra/v3/`),
  mongodb-driver-sync / -reactivestreams, AWS SDK v1 (`aws/dynamodb/`) + v2
  (`aws/dynamodb/v2/`). Deps: abacus-common 7.8.8, abacus-query 4.8.9.
- Compiled module classes: `abacus-da-all/target/classes`.
- Probe classpath (fresh, generated this round): `CP="abacus-da-all/target/classes;$(cat /tmp/review-0726/cp.txt)"`.
  A tiny throwaway probe in /tmp compiled against that classpath is the gold
  standard for "does this example really return X".
  Note: the condition factory is `com.landawn.abacus.query.Filters` (NOT
  `...query.condition.Filters`).
- Do NOT run `mvn` yourself (the main agent owns builds) and do NOT edit any file.
  You MAY compile probes into /tmp only.

## Reporting format
For each finding: `[WS#][P1|P2|P3] file:line — one-line claim`, then evidence
(code excerpt, the exact failing scenario or the contradicting doc sentence, and
HOW you verified it), then the minimal suggested fix. End with a per-file
verdict list: CLEAN, or the findings. Be explicit about confidence and about
anything you could not verify.
You are READ-ONLY — do not edit anything in the repo, do not run maven.
