# Review round 2026-08-01 — Javadoc-only deep review

You are ONE read-only reviewer in a multi-agent **Javadoc** review of
`abacus-da-all/src/main/java` (51 production classes; package-info ignored unless
assigned). Review the **WORKING TREE**.

## Task constraints (STRICT)
- Review **existing** Javadoc comments only.
- Do **NOT** propose adding Javadoc to undocumented members.
- Do **NOT** propose changing executable code, signatures, or formatting outside comments.
- For methods annotated with `@Override`: only flag incorrect existing docs; do not propose new blocks.
- Prefer **accuracy against the implementation** over style nits.

## Criteria (flag only real issues)
1. **Contract accuracy** — prose matches actual behavior (null handling, return values, side effects, exceptions).
2. **Tags** — `@param` / `@return` / `@throws` / `{@link}` / `@see` match the real signature and thrown exceptions; no phantom `@throws`; no `@return` on void; no wrong param names.
3. **Examples** — every `// returns` / `// throws` / `// equals` assertion must be true; examples must be conceptually compilable.
4. **Clarity** — typos, broken sentences, contradictory sentences, copy-paste from a sibling that diverged.
5. **Sibling consistency** — where v1/v2 or sync/async/reactive deliberately differ, docs must reflect that (do not force false alignment).

## DO NOT RE-FLAG
Read and honor `scripts/review_2026-07-25/BRIEFING.md` "DO NOT RE-FLAG" in full.
Also settled in later rounds (do not re-open):
- ContinuableFuture unwraps ExecutionException (v1 async DDB samples use `ex instanceof X`, not `ex.getCause()`).
- SDK v2 futures use CompletionException → `ex.getCause()` is correct for v2 async DDB.
- Empty stubs (search/*, hadoop/*, blink/*, spark/*, AWSRDSUtil, AWSS3Util) are intentional placeholders.
- No executor implements AutoCloseable.
- Mongo null Bson filter → IAE at chokepoint.
- CqlBuilder SELECT/DELETE from(Class) overrides are intentional.

## Recent code change to check carefully
HBase `Batch.Call` / `Batch.Callback` parameters now reject null with `IllegalArgumentException`
via `N.checkArgNotNull` in both `HBaseExecutor` and `AsyncHBaseExecutor`. Javadoc `@throws`
and examples for those methods must match.

## Verification
- Source: `abacus-da-all/src/main/java/...`
- Classes: `abacus-da-all/target/classes`
- Classpath file: `target/cp.txt` at repo root (Windows `;` separator)
- You MAY read files and run tiny compile probes; do NOT edit the repo; do NOT run full `mvn test`.

## Report format
For each finding:
```
[WS2][P1|P2|P3] path:line — one-line claim
Evidence: (quote doc + contradicting code / probe result)
Fix: exact replacement text or clear edit instruction
```
End with per-file verdict: CLEAN or list of finding IDs.
Only report ACTIONABLE accuracy issues. No style-only noise.
