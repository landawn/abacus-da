# AWS and HBase wrapper review — 2026-10-31

## Scope and method

Reviewed the implementations in all 22 files listed below, including nested mappers/builders, conversion helpers, asynchronous delegation, pagination, mutation rollback, collection construction, and package/annotation documentation. Javadoc review focused on API contracts, exception and ownership behavior, threading, and memory guidance. Read the pinned abacus-common 8.3.2 and HBase 2.6.6 source archives where factory or driver behavior mattered. No cloud or database calls were made.

Paths below are relative to `abacus-da-all/src/main/java/com/landawn/abacus/da/`:

| Files reviewed | Areas checked |
| --- | --- |
| `aws/AWSRDSUtil.java`, `aws/AWSS3Util.java`, `aws/package-info.java` | Placeholder visibility, construction, package claims |
| `aws/dynamodb/DynamoDBExecutorBase.java` | Generic/property conversion, JSON contexts, wrappers, binary handling, number formats, caches, collection factories |
| `aws/dynamodb/DynamoDBExecutor.java` | SDK v1 conversion, row materialization, requests, pagination, streaming, entity mapper, filters, condition builder |
| `aws/dynamodb/AsyncDynamoDBExecutor.java` | Every delegation, validation timing, argument ownership, scheduling and stream behavior |
| `aws/dynamodb/package-info.java` | SDK v1/shared-base description |
| `aws/dynamodb/v2/DynamoDBExecutor.java` | SDK v2 conversion, row materialization, requests, pagination, streaming, entity mapper, filters, condition builder |
| `aws/dynamodb/v2/AsyncDynamoDBExecutor.java` | Completion chains, early completion/cancellation, streaming waits, mapper and request delegation |
| `aws/dynamodb/v2/package-info.java` | SDK v2 asynchronous result description |
| `hbase/AnyOperation.java`, `hbase/AnyOperationWithAttributes.java` | Delegation, attributes, diagnostic serialization, exception translation |
| `hbase/AnyQuery.java`, `hbase/AnyMutation.java` | Fluent delegation, family lookup, timestamps, permissions, scanner/mutation state |
| `hbase/AnyAppend.java`, `hbase/AnyIncrement.java` | Constructors, row conversion, cell addition, native-copy behavior, failure cleanup |
| `hbase/AnyDelete.java` | All delete-marker overloads, rollback, validation precedence, native conversion |
| `hbase/AnyGet.java` | Construction/wrapping, projection/version controls, conversion traversal |
| `hbase/AnyPut.java` | Constructors, bean and nested/versioned mapping, bulk conversion, cell addition/rollback |
| `hbase/AnyRowMutations.java` | Row grouping, delegation, mutation acceptance, equality contract |
| `hbase/AnyScan.java` | Range conversion, pagination controls, result limits, partial-result and memory semantics |
| `hbase/annotation/ColumnFamily.java` | Annotation targets/defaults and mapping documentation |

## Changes

1. **DynamoDB collection rows, both SDKs.** `ArrayBlockingQueue` rows previously failed because row decoding used a no-argument collection factory. They now have capacity for every attribute, with a minimum capacity of one for an empty projected row. `CopyOnWriteArrayList` rows are constructed in bulk instead of copying the growing backing array for every attribute. A shared helper keeps single-item and multi-row reads consistent, preserves native values and immutable collection finalization, and leaves other collection factories unchanged.
2. **AnyDelete validation and lookup cost.** The rollback helper now detects a newly created family using the family count, like the existing Put/Increment helpers. It no longer performs an extra tree lookup before each marker, and a negative timestamp is rejected by HBase before an incompatible caller-supplied family comparator can preempt it. Existing failure cleanup remains intact.
3. **Javadocs.** Documented collection-row construction and native SDK differences; corrected claims that v1 async calls can never block and that v2 continuations always run on a completion pool. Corrected `AnyScan.maxResultSize` guidance: complete-row assembly can retain fragments across RPCs, so this setting is not a heap cap. Added five missing blank lines before usage blocks in `AnyMutation`, `AnyQuery`, and `ColumnFamily`. Replaced seven inaccessible inline links in package-private `AnyMutation`, `AnyOperationWithAttributes`, and `AnyQuery` with explicit code signatures while preserving their `@see` links and API visibility; inherited copies had produced 28 doclint warnings.

## Regression coverage and checks

New tests:

- `com.landawn.abacus.da.aws.dynamodb.DynamoDBCollectionRowTest`
- `com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBCollectionRowTest`
- `com.landawn.abacus.da.hbase.AnyDeleteValidationOrderTest`

The two collection suites each cover single-item and list paths, nonempty and empty bounded queues, ordering, duplicates/nulls, binary preservation, and allocation growth for 512 versus 2,048 attributes. The delete test covers all eight explicit-timestamp delete-marker overloads with a family map whose comparator would otherwise fail first.

Related existing selectors: `AnyDeleteTest`, `HBaseWrapperBoundaryTest`, and the DynamoDB mapping/collection/allocation suites. Maven execution is coordinated by the parent review agent, which reported all 5,351 tests green with 14 Mongo live-service skips before the final documentation-only link fixes. The parent is rerunning full Javadoc generation with `-Xdoclint:all`; consult its aggregate result for the final run.

Read-only Javadoc tooling reports zero usage-layout, sample-member-misuse, and suspicious-string findings for AWS/HBase after the spacing fixes. `git diff --check` passed.

## Limits

No live-service integration tests or external performance benchmarks were run by this reviewer. Allocation regressions use the repository's per-thread allocation counter helper and skip on unsupported JVMs. Not every illustrative Javadoc snippet was independently compiled or executed. Unchanged files in the inventory had no additional actionable defect established during this pass; that is not a proof that they contain no defects.
