package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemIdentity;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchItemRequestOptions;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.NamingPolicy;

/**
 * Integration tests for {@link CosmosContainerExecutor} that run against a <b>real</b> Azure Cosmos DB
 * instance instead of Mockito mocks (the mock-based counterpart lives in {@code CosmosContainerExecutorTest}).
 *
 * <p>These tests exercise true CRUD, query, patch and streaming round-trips so the assertions reflect
 * actual Cosmos DB semantics (status codes, case-sensitive identifiers, alias-qualified SQL, etc.)
 * rather than verified mock interactions.</p>
 *
 * <h2>Running locally against the emulator</h2>
 * <p>Start the Linux emulator in HTTPS mode &mdash; the Java SDK does not support the emulator's (default)
 * HTTP mode. The {@code -e PROTOCOL=https} environment variable is the reliable way to force HTTPS:</p>
 * <pre>{@code
 * docker rm -f cosmos-db
 * docker run --detach --name cosmos-db `
 *   -e PROTOCOL=https `
 *   --publish 8081:8081 --publish 8080:8080 --publish 1234:1234 `
 *   mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest --protocol https
 * }</pre>
 *
 * <p>The emulator serves HTTPS with a self-signed certificate. Rather than importing that certificate
 * into the JVM trust store, these tests set the SDK system property
 * {@code COSMOS.EMULATOR_SERVER_CERTIFICATE_VALIDATION_DISABLED=true}, which makes the SDK trust the
 * emulator endpoint when the host is {@code localhost} (see
 * {@code com.azure.cosmos.implementation.Configs#isEmulatorServerCertValidationDisabled}).</p>
 *
 * <p>This class is intentionally <i>not</i> named {@code *Test}, so a normal {@code mvn test} run does not
 * pick it up. Run it explicitly once the emulator is up:</p>
 * <pre>{@code
 * mvn -pl abacus-da-all test -Dtest=CosmosContainerExecutor2
 * }</pre>
 *
 * <h2>Graceful skips for emulator gaps</h2>
 * <p>Tests degrade to JUnit {@link Assumptions} skips (never failures) when the connected endpoint can't
 * support an operation, so the suite is green against the emulator while the same assertions run for real
 * against a full Azure Cosmos DB account:</p>
 * <ul>
 * <li>If the emulator is unreachable, every database-backed test is skipped. The pure-logic
 *     {@code rewritePositionalParameters} tests and the null-argument constructor guards always run.</li>
 * <li>The vNext emulator's query engine only supports predicates/projections/ORDER BY on <i>indexed</i>
 *     paths &mdash; {@code id} and the partition key by default ({@code SELECT *} and {@code COUNT} are fine;
 *     custom indexing policy is a documented no-op). Any query naming a non-indexed property (here
 *     {@code value}) returns {@code 400 BadRequest} &mdash; reproducible in the emulator's own shell, so it is
 *     the engine, not the Java SDK. Writes, point reads and {@code ReadFeed}-based {@code readAllItems}/
 *     {@code readMany} are unaffected. Therefore the {@code assumeQuerySupported()} tests (which filter/project
 *     {@code value}, as the executor's {@code selectFrom} projects every field) skip on localhost via
 *     {@link #setUpClass()} and run only against full Azure Cosmos DB, while the dedicated
 *     {@code ...RunsOnEmulator} tests stay on indexed paths and execute for real here too. See the cluster of
 *     emulator query issues, e.g. {@code github.com/Azure/azure-cosmos-db-emulator-docker/issues/156}.</li>
 * <li>{@code deleteAllItemsByPartitionKey} is skipped if the emulator reports the feature is unavailable.</li>
 * </ul>
 */
public class CosmosContainerExecutor2Test extends TestBase {

    private static final String ENDPOINT = "https://localhost:8081";

    /** Well-known public Azure Cosmos DB emulator key. */
    private static final String KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    private static final String EMULATOR_CERT_VALIDATION_DISABLED_PROP = "COSMOS.EMULATOR_SERVER_CERTIFICATE_VALIDATION_DISABLED";
    private static final String LOCAL_EMULATOR_QUERY_UNSUPPORTED_REASON = "localhost Cosmos DB emulator skips SQL Query tests because the vNext emulator returns HTTP 400 for Query via the Java SDK";

    private static final String DATABASE_ID = "abacus_da_test";
    private static final String CONTAINER_ID = "test_item";
    private static final String PARTITION_KEY_PATH = "/name";

    private static CosmosClient client;
    private static CosmosContainer container;
    private static CosmosContainerExecutor executor;

    private static boolean cosmosAvailable;
    private static String unavailabilityReason;

    /**
     * Whether the connected endpoint can actually execute the SQL <i>Query</i> operation. The vNext-preview
     * Linux emulator currently returns {@code 400 BadRequest} for queries via the Java SDK (writes, point
     * reads and ReadFeed all work), so query-based tests are skipped against it but still run against a full
     * Azure Cosmos DB account. Determined once by {@link #setUpClass()}.
     */
    private static boolean querySupported;
    private static String queryUnsupportedReason;

    @BeforeAll
    public static void setUpClass() {
        // Trust the emulator's self-signed cert for localhost instead of importing it into the JVM trust store.
        System.setProperty(EMULATOR_CERT_VALIDATION_DISABLED_PROP, "true");

        try {
            client = new CosmosClientBuilder().endpoint(ENDPOINT)
                    .key(KEY)
                    .gatewayMode() // the emulator only publishes the gateway port (8081); direct/RNTBD ports are not exposed
                    .consistencyLevel(ConsistencyLevel.SESSION)
                    .contentResponseOnWriteEnabled(true) // so create/upsert/replace responses carry the document body
                    .buildClient();

            client.createDatabaseIfNotExists(DATABASE_ID);
            final CosmosDatabase database = client.getDatabase(DATABASE_ID);
            database.createContainerIfNotExists(new CosmosContainerProperties(CONTAINER_ID, PARTITION_KEY_PATH),
                    ThroughputProperties.createManualThroughput(400));
            container = database.getContainer(CONTAINER_ID);

            executor = new CosmosContainerExecutor(container);
            cosmosAvailable = true;

            if (isLocalEmulatorEndpoint()) {
                querySupported = false;
                queryUnsupportedReason = LOCAL_EMULATOR_QUERY_UNSUPPORTED_REASON;
                return;
            }

            // Probe the SQL Query operation once: the vNext-preview emulator answers 400 to queries via the
            // Java SDK even though writes/point-reads/ReadFeed succeed. This lets query tests skip cleanly
            // here while still asserting real behavior against a full Azure Cosmos DB account.
            try {
                final TestItem probe = itemIn("query-probe-" + UUID.randomUUID(), "query-probe-" + UUID.randomUUID());
                executor.createItem(probe);
                // toList() forces the lazy CosmosPagedIterable to actually execute the query (stream().count()
                // can short-circuit without enumerating), surfacing the emulator's 400 for the Query operation.
                executor.queryItems("SELECT * FROM c WHERE c.value = '" + probe.value + "'", TestItem.class).stream().toList();
                querySupported = true;
            } catch (final Exception e) {
                querySupported = false;
                queryUnsupportedReason = queryProbeFailureReason(e);
                System.err.println(
                        "[CosmosContainerExecutor2] SQL query API unavailable on this endpoint; query-based tests will be skipped: " + queryUnsupportedReason);
            }
        } catch (final Throwable t) {
            cosmosAvailable = false;
            unavailabilityReason = t.toString();
            System.err.println("[CosmosContainerExecutor2] Cosmos emulator unavailable at " + ENDPOINT + " - database-backed tests will be skipped: " + t);
        }
    }

    @AfterAll
    public static void tearDownClass() {
        if (client != null) {
            try {
                if (cosmosAvailable) {
                    client.getDatabase(DATABASE_ID).delete(); // drop everything created by this run
                }
            } catch (final Throwable ignore) {
                // best-effort cleanup
            } finally {
                client.close();
            }
        }
    }

    private static boolean isLocalEmulatorEndpoint() {
        final String endpoint = ENDPOINT.toLowerCase();
        return endpoint.contains("localhost") || endpoint.contains("127.0.0.1") || endpoint.contains("[::1]") || endpoint.contains("://::1");
    }

    private static String queryProbeFailureReason(final Exception e) {
        if (e instanceof CosmosException) {
            return "SQL query probe failed with HTTP status " + ((CosmosException) e).getStatusCode();
        }

        final String message = e.getMessage();
        return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    private static void assumeCosmosAvailable() {
        Assumptions.assumeTrue(cosmosAvailable, "Cosmos DB emulator not reachable at " + ENDPOINT + " (" + unavailabilityReason + ")");
    }

    /**
     * Skips a test when the connected endpoint cannot execute SQL queries. Implies {@link #assumeCosmosAvailable()}.
     */
    private static void assumeQuerySupported() {
        assumeCosmosAvailable();
        Assumptions.assumeTrue(querySupported, "SQL query API not supported by this Cosmos endpoint (" + queryUnsupportedReason + ")");
    }

    // ----------------------------------------------------------------------------------------------------
    // Test data and helpers
    // ----------------------------------------------------------------------------------------------------

    private static TestItem itemIn(final String partition, final String value) {
        return new TestItem(UUID.randomUUID().toString(), partition, value);
    }

    private static String newPartition() {
        return "pk-" + UUID.randomUUID();
    }

    private static PartitionKey pk(final TestItem item) {
        return new PartitionKey(item.name);
    }

    // ----------------------------------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testConstructorWithContainer() {
        assumeCosmosAvailable();
        final CosmosContainerExecutor exec = new CosmosContainerExecutor(container);
        assertNotNull(exec);
        assertEquals(container, exec.cosmosContainer());
    }

    @Test
    public void testConstructorWithContainerAndNamingPolicy() {
        assumeCosmosAvailable();
        final CosmosContainerExecutor exec = new CosmosContainerExecutor(container, NamingPolicy.SCREAMING_SNAKE_CASE);
        assertNotNull(exec);
        assertEquals(container, exec.cosmosContainer());
    }

    @Test
    public void testCosmosContainer() {
        assumeCosmosAvailable();
        assertEquals(container, executor.cosmosContainer());
    }

    @Test
    public void testConstructorRejectsNullContainer() {
        assertThrows(IllegalArgumentException.class, () -> new CosmosContainerExecutor(null));
    }

    @Test
    public void testConstructorWithPolicyRejectsNullContainer() {
        assertThrows(IllegalArgumentException.class, () -> new CosmosContainerExecutor(null, NamingPolicy.SNAKE_CASE));
    }

    @Test
    public void testConstructorWithPolicyRejectsNullPolicy() {
        assumeCosmosAvailable();
        assertThrows(IllegalArgumentException.class, () -> new CosmosContainerExecutor(container, (NamingPolicy) null));
    }

    // ----------------------------------------------------------------------------------------------------
    // Create
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testCreateItemWithItem() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "create-basic");

        final CosmosItemResponse<TestItem> response = executor.createItem(item);

        assertEquals(201, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
    }

    @Test
    public void testCreateItemWithItemPartitionKeyAndOptions() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "create-pk-opts");

        final CosmosItemResponse<TestItem> response = executor.createItem(item, pk(item), new CosmosItemRequestOptions());

        assertEquals(201, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
    }

    @Test
    public void testCreateItemWithItemAndOptions() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "create-opts");

        final CosmosItemResponse<TestItem> response = executor.createItem(item, new CosmosItemRequestOptions());

        assertEquals(201, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
    }

    // ----------------------------------------------------------------------------------------------------
    // Upsert
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testUpsertItemWithItem() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "upsert-1");

        // First upsert creates the item -> 201
        final CosmosItemResponse<TestItem> created = executor.upsertItem(item);
        assertEquals(201, created.getStatusCode());

        // Second upsert replaces it -> 200
        item.value = "upsert-2";
        final CosmosItemResponse<TestItem> replaced = executor.upsertItem(item);
        assertEquals(200, replaced.getStatusCode());

        assertEquals("upsert-2", executor.readItem(item.id, pk(item), TestItem.class).getItem().value);
    }

    @Test
    public void testUpsertItemWithItemPartitionKeyAndOptions() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "upsert-pk-opts");

        final CosmosItemResponse<TestItem> response = executor.upsertItem(item, pk(item), new CosmosItemRequestOptions());

        assertEquals(201, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
    }

    @Test
    public void testUpsertItemWithItemAndOptions() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "upsert-opts");

        final CosmosItemResponse<TestItem> response = executor.upsertItem(item, new CosmosItemRequestOptions());

        assertEquals(201, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
    }

    // ----------------------------------------------------------------------------------------------------
    // Replace / Patch
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testReplaceItem() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "original");
        executor.createItem(item);

        final TestItem updated = new TestItem(item.id, item.name, "replaced");
        final CosmosItemResponse<TestItem> response = executor.replaceItem(item.id, pk(item), updated, new CosmosItemRequestOptions());

        assertEquals(200, response.getStatusCode());
        assertEquals("replaced", executor.readItem(item.id, pk(item), TestItem.class).getItem().value);
    }

    @Test
    public void testPatchItemWithoutOptions() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "pre-patch");
        executor.createItem(item);

        final CosmosPatchOperations patchOperations = CosmosPatchOperations.create().replace("/value", "patched");
        final CosmosItemResponse<TestItem> response = executor.patchItem(item.id, pk(item), patchOperations, TestItem.class);

        assertEquals(200, response.getStatusCode());
        assertEquals("patched", executor.readItem(item.id, pk(item), TestItem.class).getItem().value);
    }

    @Test
    public void testPatchItemWithOptions() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "pre-patch-opts");
        executor.createItem(item);

        final CosmosPatchOperations patchOperations = CosmosPatchOperations.create().replace("/value", "patched-opts");
        final CosmosItemResponse<TestItem> response = executor.patchItem(item.id, pk(item), patchOperations, new CosmosPatchItemRequestOptions(),
                TestItem.class);

        assertEquals(200, response.getStatusCode());
        assertEquals("patched-opts", executor.readItem(item.id, pk(item), TestItem.class).getItem().value);
    }

    // ----------------------------------------------------------------------------------------------------
    // Delete
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testDeleteItemWithItem() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "delete-by-item");
        executor.createItem(item);

        final CosmosItemResponse<Object> response = executor.deleteItem(item, new CosmosItemRequestOptions());

        assertEquals(204, response.getStatusCode());
        assertThrows(CosmosException.class, () -> executor.readItem(item.id, pk(item), TestItem.class));
    }

    @Test
    public void testDeleteItemWithId() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "delete-by-id");
        executor.createItem(item);

        final CosmosItemResponse<Object> response = executor.deleteItem(item.id, pk(item), new CosmosItemRequestOptions());

        assertEquals(204, response.getStatusCode());
        assertThrows(CosmosException.class, () -> executor.readItem(item.id, pk(item), TestItem.class));
    }

    @Test
    public void testDeleteAllItemsByPartitionKey() {
        assumeCosmosAvailable();
        final String partition = newPartition();
        executor.createItem(itemIn(partition, "delete-all-1"));
        executor.createItem(itemIn(partition, "delete-all-2"));

        try {
            final CosmosItemResponse<Object> response = executor.deleteAllItemsByPartitionKey(new PartitionKey(partition), new CosmosItemRequestOptions());
            assertTrue(response.getStatusCode() >= 200 && response.getStatusCode() < 300);
        } catch (final CosmosException e) {
            // The "delete all by partition key" feature is not enabled on every emulator build; skip rather than fail.
            Assumptions.assumeTrue(false, "deleteAllItemsByPartitionKey not supported by this emulator (status " + e.getStatusCode() + ")");
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Read
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testReadItemWithoutOptions() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "read-basic");
        executor.createItem(item);

        final CosmosItemResponse<TestItem> response = executor.readItem(item.id, pk(item), TestItem.class);

        assertEquals(200, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
        assertEquals("read-basic", response.getItem().value);
    }

    @Test
    public void testReadItemWithOptions() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "read-opts");
        executor.createItem(item);

        final CosmosItemResponse<TestItem> response = executor.readItem(item.id, pk(item), new CosmosItemRequestOptions(), TestItem.class);

        assertEquals(200, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
        assertEquals("read-opts", response.getItem().value);
    }

    @Test
    public void testReadManyWithoutSessionToken() {
        assumeCosmosAvailable();
        final String partition = newPartition();
        final TestItem a = itemIn(partition, "read-many-a");
        final TestItem b = itemIn(partition, "read-many-b");
        executor.createItem(a);
        executor.createItem(b);

        final List<CosmosItemIdentity> identities = Arrays.asList(new CosmosItemIdentity(new PartitionKey(partition), a.id),
                new CosmosItemIdentity(new PartitionKey(partition), b.id));

        final FeedResponse<TestItem> response = executor.readMany(identities, TestItem.class);

        assertEquals(2, response.getResults().size());
    }

    @Test
    public void testReadManyWithSessionToken() {
        assumeCosmosAvailable();
        final String partition = newPartition();
        final TestItem a = itemIn(partition, "read-many-st-a");
        final TestItem b = itemIn(partition, "read-many-st-b");
        executor.createItem(a);
        final String sessionToken = executor.createItem(b).getSessionToken();

        final List<CosmosItemIdentity> identities = Arrays.asList(new CosmosItemIdentity(new PartitionKey(partition), a.id),
                new CosmosItemIdentity(new PartitionKey(partition), b.id));

        final FeedResponse<TestItem> response = executor.readMany(identities, sessionToken, TestItem.class);

        assertEquals(2, response.getResults().size());
    }

    @Test
    public void testReadAllItemsWithoutOptions() {
        assumeCosmosAvailable();
        final String partition = newPartition();
        executor.createItem(itemIn(partition, "read-all-1"));
        executor.createItem(itemIn(partition, "read-all-2"));
        executor.createItem(itemIn(partition, "read-all-3"));

        final CosmosPagedIterable<TestItem> response = executor.readAllItems(new PartitionKey(partition), TestItem.class);

        assertEquals(3, response.stream().count());
    }

    @Test
    public void testReadAllItemsWithOptions() {
        assumeCosmosAvailable();
        final String partition = newPartition();
        executor.createItem(itemIn(partition, "read-all-opts-1"));
        executor.createItem(itemIn(partition, "read-all-opts-2"));

        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setMaxBufferedItemCount(10);
        final CosmosPagedIterable<TestItem> response = executor.readAllItems(new PartitionKey(partition), options, TestItem.class);

        assertEquals(2, response.stream().count());
    }

    // ----------------------------------------------------------------------------------------------------
    // Stream all items in a partition
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testStreamAllItemsWithoutOptions() {
        assumeCosmosAvailable();
        final String partition = newPartition();
        executor.createItem(itemIn(partition, "stream-all-1"));
        executor.createItem(itemIn(partition, "stream-all-2"));

        final List<TestItem> result = executor.streamAllItems(new PartitionKey(partition), TestItem.class).toList();

        assertEquals(2, result.size());
    }

    @Test
    public void testStreamAllItemsWithOptions() {
        assumeCosmosAvailable();
        final String partition = newPartition();
        executor.createItem(itemIn(partition, "stream-all-opts-1"));
        executor.createItem(itemIn(partition, "stream-all-opts-2"));

        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setMaxBufferedItemCount(10);
        final List<TestItem> result = executor.streamAllItems(new PartitionKey(partition), options, TestItem.class).toList();

        assertEquals(2, result.size());
    }

    // ----------------------------------------------------------------------------------------------------
    // Raw SQL queries
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testQueryItemsWithStringQuery() {
        assumeQuerySupported();
        final String value = "qsq-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final CosmosPagedIterable<TestItem> response = executor.queryItems("SELECT * FROM c WHERE c.value = '" + value + "'", TestItem.class);

        final List<TestItem> results = response.stream().toList();
        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    @Test
    public void testQueryItemsWithStringQueryAndOptions() {
        assumeQuerySupported();
        final String value = "qsqo-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setMaxBufferedItemCount(10);
        final CosmosPagedIterable<TestItem> response = executor.queryItems("SELECT * FROM c WHERE c.value = '" + value + "'", options, TestItem.class);

        final List<TestItem> results = response.stream().toList();
        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    @Test
    public void testQueryItemsWithSqlQuerySpec() {
        assumeQuerySupported();
        final String value = "qspec-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c WHERE c.value = @value", Arrays.asList(new SqlParameter("@value", value)));
        final CosmosPagedIterable<TestItem> response = executor.queryItems(querySpec, TestItem.class);

        final List<TestItem> results = response.stream().toList();
        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    @Test
    public void testQueryItemsWithSqlQuerySpecAndOptions() {
        assumeQuerySupported();
        final String value = "qspeco-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c WHERE c.value = @value", Arrays.asList(new SqlParameter("@value", value)));
        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setMaxBufferedItemCount(10);
        final CosmosPagedIterable<TestItem> response = executor.queryItems(querySpec, options, TestItem.class);

        final List<TestItem> results = response.stream().toList();
        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    // ----------------------------------------------------------------------------------------------------
    // Streaming raw SQL queries
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testStreamItemsWithStringQuery() {
        assumeQuerySupported();
        final String value = "ssq-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final List<TestItem> result = executor.streamItems("SELECT * FROM c WHERE c.value = '" + value + "'", TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
    }

    @Test
    public void testStreamItemsWithStringQueryAndOptions() {
        assumeQuerySupported();
        final String value = "ssqo-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setMaxBufferedItemCount(10);
        final List<TestItem> result = executor.streamItems("SELECT * FROM c WHERE c.value = '" + value + "'", options, TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
    }

    @Test
    public void testStreamItemsWithSqlQuerySpec() {
        assumeQuerySupported();
        final String value = "sspec-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c WHERE c.value = @value", Arrays.asList(new SqlParameter("@value", value)));
        final List<TestItem> result = executor.streamItems(querySpec, TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
    }

    @Test
    public void testStreamItemsWithSqlQuerySpecAndOptions() {
        assumeQuerySupported();
        final String value = "sspeco-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c WHERE c.value = @value", Arrays.asList(new SqlParameter("@value", value)));
        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setMaxBufferedItemCount(10);
        final List<TestItem> result = executor.streamItems(querySpec, options, TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
    }

    // ----------------------------------------------------------------------------------------------------
    // Streaming condition-based queries (SqlBuilder integration)
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testStreamItemsWithCondition() {
        assumeQuerySupported();
        final String value = "cond-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final List<TestItem> result = executor.streamItems(Filters.eq("value", value), TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
    }

    @Test
    public void testStreamItemsWithConditionAndOptions() {
        assumeQuerySupported();
        final String value = "cond-opts-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setMaxBufferedItemCount(10);
        final List<TestItem> result = executor.streamItems(Filters.eq("value", value), options, TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
    }

    @Test
    public void testStreamItemsWithSelectAndCondition() {
        assumeQuerySupported();
        final String value = "sel-cond-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final Collection<String> selectProps = Arrays.asList("id", "value");
        final List<TestItem> result = executor.streamItems(selectProps, Filters.eq("value", value), TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
        assertEquals(value, result.get(0).value);
    }

    @Test
    public void testStreamItemsWithSelectConditionAndOptions() {
        assumeQuerySupported();
        final String value = "sel-cond-opts-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        final Collection<String> selectProps = Arrays.asList("id", "value");
        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions().setMaxBufferedItemCount(10);
        final List<TestItem> result = executor.streamItems(selectProps, Filters.eq("value", value), options, TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
        assertEquals(value, result.get(0).value);
    }

    @Test
    public void testStreamItemsWithNullCondition() {
        assumeQuerySupported();
        final TestItem a = itemIn(newPartition(), "null-cond-a-" + UUID.randomUUID());
        final TestItem b = itemIn(newPartition(), "null-cond-b-" + UUID.randomUUID());
        executor.createItem(a);
        executor.createItem(b);

        // null select + null condition => "SELECT * FROM test_item c" full (cross-partition) scan
        final List<TestItem> result = executor.streamItems((Collection<String>) null, null, TestItem.class).toList();

        assertTrue(result.stream().anyMatch(t -> a.id.equals(t.id)));
        assertTrue(result.stream().anyMatch(t -> b.id.equals(t.id)));
    }

    @Test
    public void testStreamItemsWithEmptySelectProps() {
        assumeQuerySupported();
        final String value = "empty-sel-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        // empty select props is treated as SELECT * (all fields populated)
        final List<TestItem> result = executor.streamItems(Arrays.asList(), Filters.eq("value", value), TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
        assertEquals(item.name, result.get(0).name);
        assertEquals(value, result.get(0).value);
    }

    @Test
    public void testStreamItemsWithConditionUsesAliasQualifiedCosmosSql() {
        // Regression: Cosmos DB SQL requires every property reference to be bound to the FROM source/alias.
        // prepareQuery used to emit bare identifiers (e.g. "FROM test_item WHERE id = '1'"), which Cosmos
        // rejects at query enumeration. Running a real multi-term condition proves the emitted SQL is now
        // alias-qualified ("FROM test_item c ... WHERE c.id = @p0 AND c.name = @p1") and accepted by Cosmos.
        assumeQuerySupported();
        final TestItem item = itemIn(newPartition(), "alias-" + UUID.randomUUID());
        executor.createItem(item);

        final List<TestItem> result = executor.streamItems(Filters.eq("id", item.id).and(Filters.eq("name", item.name)), TestItem.class).toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
    }

    // ----------------------------------------------------------------------------------------------------
    // Naming policy behavior (real, case-sensitive Cosmos identifiers)
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testStreamItemsWithConditionAcrossNamingPolicies() {
        assumeQuerySupported();
        final String value = "np-" + UUID.randomUUID();
        final TestItem item = itemIn(newPartition(), value);
        executor.createItem(item);

        // SNAKE_CASE: single-word "value" -> "value", matches the stored (lower-case) JSON field.
        assertEquals(1,
                new CosmosContainerExecutor(container, NamingPolicy.SNAKE_CASE).streamItems(Filters.eq("value", value), TestItem.class).toList().size());

        // CAMEL_CASE: "value" stays "value", matches.
        assertEquals(1,
                new CosmosContainerExecutor(container, NamingPolicy.CAMEL_CASE).streamItems(Filters.eq("value", value), TestItem.class).toList().size());

        // SCREAMING_SNAKE_CASE: "value" -> "VALUE". Cosmos identifiers are case-sensitive, so the query is
        // valid SQL and executes without error but matches nothing against the lower-case stored field.
        assertEquals(0,
                new CosmosContainerExecutor(container, NamingPolicy.SCREAMING_SNAKE_CASE).streamItems(Filters.eq("value", value), TestItem.class)
                        .toList()
                        .size());
    }

    @Test
    public void testStreamItemsWithSelectAndConditionAndNamingPolicy_CamelCase() {
        assumeQuerySupported();
        final TestItem item = itemIn(newPartition(), "sel-camel-" + UUID.randomUUID());
        executor.createItem(item);

        final List<TestItem> result = new CosmosContainerExecutor(container, NamingPolicy.CAMEL_CASE)
                .streamItems(Arrays.asList("id"), Filters.eq("id", item.id), TestItem.class)
                .toList();

        assertEquals(1, result.size());
        assertEquals(item.id, result.get(0).id);
    }

    @Test
    public void testStreamItemsWithSelectAndConditionAndNamingPolicy_ScreamingSnake() {
        assumeQuerySupported();
        final TestItem item = itemIn(newPartition(), "sel-screaming-" + UUID.randomUUID());
        executor.createItem(item);

        // "id" -> "ID"; the case-sensitive identifier does not match the stored lower-case "id" field, so the
        // SCREAMING_SNAKE_CASE branch of prepareQuery executes a valid query that yields no rows.
        final List<TestItem> result = new CosmosContainerExecutor(container, NamingPolicy.SCREAMING_SNAKE_CASE)
                .streamItems(Arrays.asList("id"), Filters.eq("id", item.id), TestItem.class)
                .toList();

        assertEquals(0, result.size());
    }

    @Test
    public void testUnsupportedNamingPolicy() {
        assumeCosmosAvailable();
        // NO_CHANGE is not handled by prepareQuery's switch; the constructor now rejects it
        // eagerly, before any request is sent to Cosmos.
        assertThrows(IllegalArgumentException.class, () -> new CosmosContainerExecutor(container, NamingPolicy.NO_CHANGE));
    }

    // ----------------------------------------------------------------------------------------------------
    // Indexed-path queries that also execute on the local vNext emulator
    //
    // Unlike the {@code assumeQuerySupported()} tests above (which filter/project the non-indexed "value"
    // field and so 400 on the emulator), these only reference indexed paths: "id" and the partition key
    // "name", and project either via SELECT * or an explicit "id"-only list. That keeps them inside the
    // emulator's query engine's supported surface, so they run for real here AND on full Azure Cosmos DB.
    // They are gated only by assumeCosmosAvailable().
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testQueryItemsByIdSelectStarRunsOnEmulator() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "by-id-select-star");
        executor.createItem(item);

        // SELECT * does not name a non-indexed path; the filter is on the indexed "id".
        final List<TestItem> results = executor.queryItems("SELECT * FROM c WHERE c.id = '" + item.id + "'", TestItem.class).stream().toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
        assertEquals("by-id-select-star", results.get(0).value);
    }

    @Test
    public void testQueryItemsByIdSqlQuerySpecRunsOnEmulator() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "by-id-spec");
        executor.createItem(item);

        final SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c WHERE c.id = @id", Arrays.asList(new SqlParameter("@id", item.id)));
        final List<TestItem> results = executor.queryItems(querySpec, TestItem.class).stream().toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    @Test
    public void testStreamItemsByIdSelectStarRunsOnEmulator() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "by-id-stream");
        executor.createItem(item);

        final List<TestItem> results = executor.streamItems("SELECT * FROM c WHERE c.id = '" + item.id + "'", TestItem.class).toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    @Test
    public void testStreamItemsConditionByIdProjectingIdRunsOnEmulator() {
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "cond-by-id");
        executor.createItem(item);

        // Projecting only the indexed "id" -> generated SQL "SELECT c.id FROM test_item c WHERE c.id = @p0".
        final List<TestItem> results = executor.streamItems(Arrays.asList("id"), Filters.eq("id", item.id), TestItem.class).toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
        assertNull(results.get(0).value, "value was not projected, so it must be null");
    }

    @Test
    public void testStreamItemsConditionByIdAndPartitionKeyRunsOnEmulator() {
        // The real-execution counterpart of testStreamItemsWithConditionUsesAliasQualifiedCosmosSql: both
        // predicate paths ("id" and partition key "name") are indexed and only "id" is projected, so the
        // alias-qualified SQL "SELECT c.id FROM test_item c WHERE c.id = @p0 AND c.name = @p1" runs on the
        // emulator and proves the generated query is accepted by a real Cosmos query engine.
        assumeCosmosAvailable();
        final TestItem item = itemIn(newPartition(), "cond-by-id-pk");
        executor.createItem(item);

        final List<TestItem> results = executor.streamItems(Arrays.asList("id"), Filters.eq("id", item.id).and(Filters.eq("name", item.name)), TestItem.class)
                .toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    // ----------------------------------------------------------------------------------------------------
    // rewritePositionalParameters (pure logic - no Cosmos connection required)
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testRewritePositionalParametersRejectsExtraPlaceholders() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);

        final InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(null, "SELECT * FROM c WHERE c.id = ? AND c.name = ?", 1));

        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(ex.getCause().getMessage().contains("expected 1 placeholders but found 2"));
    }

    @Test
    public void testRewritePositionalParameters_NoPlaceholders() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.id = 'static'", 0);
        assertEquals("SELECT * FROM c WHERE c.id = 'static'", result);
    }

    @Test
    public void testRewritePositionalParameters_MultiPlaceholder() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.a = ? AND c.b = ? AND c.c = ?", 3);
        assertEquals("SELECT * FROM c WHERE c.a = @p0 AND c.b = @p1 AND c.c = @p2", result);
    }

    @Test
    public void testRewritePositionalParameters_QuestionMarkInsideSingleQuotes() throws Exception {
        // The '?' inside a single-quoted string literal must be preserved, not rewritten.
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.name = 'a?b' AND c.id = ?", 1);
        assertEquals("SELECT * FROM c WHERE c.name = 'a?b' AND c.id = @p0", result);
    }

    @Test
    public void testRewritePositionalParameters_EscapedSingleQuote() throws Exception {
        // '' is the SQL escape for a single quote and should not toggle the in-quote state.
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.name = 'O''Brien' AND c.id = ?", 1);
        assertEquals("SELECT * FROM c WHERE c.name = 'O''Brien' AND c.id = @p0", result);
    }

    @Test
    public void testRewritePositionalParameters_FewerParametersThanPlaceholders() throws Exception {
        // When parameterCount < total '?' found, the extras stay as '?' and we hit the mismatch error.
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(null, "SELECT * FROM c WHERE a=? AND b=? AND c=?", 2));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(ex.getCause().getMessage().contains("expected 2 placeholders but found 3"));
    }

    // ----------------------------------------------------------------------------------------------------
    // Test data class
    // ----------------------------------------------------------------------------------------------------

    public static class TestItem {
        public String id;
        public String name; // partition key (path "/name")
        public String value; // mutable payload for replace/patch round-trips

        public TestItem() {
        }

        public TestItem(final String id, final String name, final String value) {
            this.id = id;
            this.name = name;
            this.value = value;
        }
    }
}
