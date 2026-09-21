# Exception and validation review

Reviewed the shared production tree at `abacus-da-all/src/main/java`. The repository has no root `src/main/java`; the individual Maven artifacts compile selected packages from this shared tree.

The review covered 67 Java files and 2,358 methods and constructors, including nested classes, private helpers, anonymous implementations, and the empty placeholder classes. Three agents reviewed Cassandra, MongoDB, and HBase; the main review covered AWS, Azure, BigQuery, Neo4j, and the placeholders. A second agent pass checked the main review's changes.

## Changes

- Corrected direct and delegated exception contracts, including translated I/O and parsing failures, duplicate results, callback failures, constructor limits, and executor submission failures.
- Distinguished exceptions thrown by the method call from failures delivered through futures, publishers, and lazy stream traversal. Deferred validation remains deferred where moving it would change the asynchronous contract.
- Moved state and argument validation before operations and early returns where applicable, using signature order and preserving dependencies between checks.
- Corrected exception ordering, imported exception types used by Javadoc, removed qualified exception tag names, and reviewed the affected descriptions and formatting.
- Checked parameter-name literals against the supplied `abacus-common` 8.0.1 `cs` constants. Remaining names have no matching constant and remain strings as requested. No dependency changes or replacement constants class were introduced; descriptive messages and format strings remain intact.
- Added 21 regression tests across five test classes. Existing Cassandra tests now assert the requested precedence for simultaneously invalid parameters and retain separate coverage of null callbacks.

## Validation

- All eight Maven reactor projects compile successfully with the configured Java 17 release target.
- The final selected test run completed **3,544 tests, with zero failures, errors, or skipped tests**. It includes the new regressions and existing AWS, Azure, BigQuery, Neo4j, Cassandra, MongoDB, and HBase tests.
- `mvn -pl abacus-da-all javadoc:javadoc` passes with the project's `doclint=all` configuration.
- An AST audit of all production declarations found no missing Javadoc tags for declared exceptions, duplicate exception tags, or differences between the order of declared exceptions and their corresponding tags.
- No package-qualified exception names remain in throws clauses or `@throws` tags, and no package-qualified `cs` field references remain.
- `git diff --check` passes.

The complete service integration suite was not run. In particular, `HBaseExecutorTest` requires a live HBase service and was excluded. Service-specific exception behavior was also checked against locally installed dependency sources. Maven's existing Surefire configuration ignores test failures, so the actual test totals and failure reports were inspected rather than relying on `BUILD SUCCESS` alone.

## Compatibility and conditional cases

- No checked-exception contracts were widened. Added or corrected unchecked throws declarations remain compatible with Java overriding rules and ordinary source/binary calls, but their reflection-visible exception metadata can change.
- Invalid state now takes precedence over invalid arguments where applicable. When several arguments are invalid, the first exception can change to match signature order. Earlier checks can also prevent logging, conversion, callback invocation, or database work that previously preceded the eventual validation failure. Some null-state diagnostic messages are consequently more explicit.
- DynamoDB condition builders retain their historical `NullPointerException` after the first `build()`, now checked before mutation arguments. A repeated `build()` still returns null.
- Null and empty inputs retain their conditional acceptance where a branch genuinely bypasses conversion or remote work. The review did not impose a universal non-null contract on optional inputs, empty results, nullable HBase families, or SDK options.
- Java's required constructor delegation order is retained. Validation depending on schema inspection, property metadata, or a callback result occurs only after those prerequisites are available.
- SDK implementations, custom codecs, bean accessors, and caller-supplied callbacks can raise additional runtime subclasses. Such failures are documented at the operation that can produce them, using a common runtime type where a narrower cross-implementation contract would be inaccurate. Errors from asynchronous work and lazy traversal are described at their delivery point instead of added to the enclosing factory or submission method's throws clause.

Detailed per-package working notes and validation logs are available under the ignored `target/exception-review*` paths.
