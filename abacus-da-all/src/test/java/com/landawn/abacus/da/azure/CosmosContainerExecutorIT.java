package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Cosmos emulator round trips at https://localhost:8081. Each run owns a separate database. Query features unsupported by the emulator are reported as skipped; SQL rewriting is covered by CosmosQueryParameterTest.
 *
 * <p>Run with {@code mvn -pl abacus-da-all -Pintegration-tests -Dit.test=CosmosContainerExecutorIT verify -Dgpg.skip=true}.</p>
 */
public class CosmosContainerExecutorIT extends TestBase {

    private static final String ENDPOINT = "https://localhost:8081";

    /** Well-known public Azure Cosmos DB emulator key. */
    private static final String KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    private static final String EMULATOR_CERT_VALIDATION_DISABLED_PROP = "COSMOS.EMULATOR_SERVER_CERTIFICATE_VALIDATION_DISABLED";
    private static final String LOCAL_EMULATOR_QUERY_UNSUPPORTED_REASON = "localhost Cosmos DB emulator skips SQL Query tests because the vNext emulator returns HTTP 400 for Query via the Java SDK";

    private static final String DATABASE_ID = "abacus_da_test_" + UUID.randomUUID().toString().replace("-", "");
    private static final String CONTAINER_ID = "test_item";
    private static final String PARTITION_KEY_PATH = "/name";

    private static CosmosClient client;
    private static CosmosContainer container;
    private static CosmosContainerExecutor executor;

    private static String previousCertificateValidationSetting;
    private static boolean databaseCreated;

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
        previousCertificateValidationSetting = System.getProperty(EMULATOR_CERT_VALIDATION_DISABLED_PROP);
        System.setProperty(EMULATOR_CERT_VALIDATION_DISABLED_PROP, "true");

        try {
            client = new CosmosClientBuilder().endpoint(ENDPOINT)
                    .key(KEY)
                    .gatewayMode() // the emulator only publishes the gateway port (8081); direct/RNTBD ports are not exposed
                    .consistencyLevel(ConsistencyLevel.SESSION)
                    .contentResponseOnWriteEnabled(true) // so create/upsert/replace responses carry the document body
                    .buildClient();

            client.createDatabase(DATABASE_ID);
            databaseCreated = true;
            final CosmosDatabase database = client.getDatabase(DATABASE_ID);
            database.createContainerIfNotExists(new CosmosContainerProperties(CONTAINER_ID, PARTITION_KEY_PATH),
                    ThroughputProperties.createManualThroughput(400));
            container = database.getContainer(CONTAINER_ID);

            executor = new CosmosContainerExecutor(container);

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
                        "[CosmosContainerExecutorIT] SQL query API unavailable on this endpoint; query-based tests will be skipped: " + queryUnsupportedReason);
            }
        } catch (final Exception e) {
            throw new IllegalStateException("Cosmos integration setup failed at " + ENDPOINT, e);
        }
    }

    @AfterAll
    public static void tearDownClass() {
        try {
            if (client != null) {
                try {
                    if (databaseCreated) {
                        client.getDatabase(DATABASE_ID).delete();
                    }
                } finally {
                    client.close();
                }
            }
        } finally {
            if (previousCertificateValidationSetting == null) {
                System.clearProperty(EMULATOR_CERT_VALIDATION_DISABLED_PROP);
            } else {
                System.setProperty(EMULATOR_CERT_VALIDATION_DISABLED_PROP, previousCertificateValidationSetting);
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

    /**
     * Skips a test when the connected endpoint cannot execute SQL queries.
     */
    private static void assumeQuerySupported() {
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
    // Create
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testCreateItemWithItem() {
        final TestItem item = itemIn(newPartition(), "create-basic");

        final CosmosItemResponse<TestItem> response = executor.createItem(item);

        assertEquals(201, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
    }

    @Test
    public void testCreateItemWithItemPartitionKeyAndOptions() {
        final TestItem item = itemIn(newPartition(), "create-pk-opts");

        final CosmosItemResponse<TestItem> response = executor.createItem(item, pk(item), new CosmosItemRequestOptions());

        assertEquals(201, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
    }

    @Test
    public void testCreateItemWithItemAndOptions() {
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
        final TestItem item = itemIn(newPartition(), "upsert-pk-opts");

        final CosmosItemResponse<TestItem> response = executor.upsertItem(item, pk(item), new CosmosItemRequestOptions());

        assertEquals(201, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
    }

    @Test
    public void testUpsertItemWithItemAndOptions() {
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
        final TestItem item = itemIn(newPartition(), "original");
        executor.createItem(item);

        final TestItem updated = new TestItem(item.id, item.name, "replaced");
        final CosmosItemResponse<TestItem> response = executor.replaceItem(item.id, pk(item), updated, new CosmosItemRequestOptions());

        assertEquals(200, response.getStatusCode());
        assertEquals("replaced", executor.readItem(item.id, pk(item), TestItem.class).getItem().value);
    }

    @Test
    public void testPatchItemWithoutOptions() {
        final TestItem item = itemIn(newPartition(), "pre-patch");
        executor.createItem(item);

        final CosmosPatchOperations patchOperations = CosmosPatchOperations.create().replace("/value", "patched");
        final CosmosItemResponse<TestItem> response = executor.patchItem(item.id, pk(item), patchOperations, TestItem.class);

        assertEquals(200, response.getStatusCode());
        assertEquals("patched", executor.readItem(item.id, pk(item), TestItem.class).getItem().value);
    }

    @Test
    public void testPatchItemWithOptions() {
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
        final TestItem item = itemIn(newPartition(), "delete-by-item");
        executor.createItem(item);

        final CosmosItemResponse<Object> response = executor.deleteItem(item, new CosmosItemRequestOptions());

        assertEquals(204, response.getStatusCode());
        assertThrows(CosmosException.class, () -> executor.readItem(item.id, pk(item), TestItem.class));
    }

    @Test
    public void testDeleteItemWithId() {
        final TestItem item = itemIn(newPartition(), "delete-by-id");
        executor.createItem(item);

        final CosmosItemResponse<Object> response = executor.deleteItem(item.id, pk(item), new CosmosItemRequestOptions());

        assertEquals(204, response.getStatusCode());
        assertThrows(CosmosException.class, () -> executor.readItem(item.id, pk(item), TestItem.class));
    }

    @Test
    public void testDeleteAllItemsByPartitionKey() {
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
        final TestItem item = itemIn(newPartition(), "read-basic");
        executor.createItem(item);

        final CosmosItemResponse<TestItem> response = executor.readItem(item.id, pk(item), TestItem.class);

        assertEquals(200, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
        assertEquals("read-basic", response.getItem().value);
    }

    @Test
    public void testReadItemWithOptions() {
        final TestItem item = itemIn(newPartition(), "read-opts");
        executor.createItem(item);

        final CosmosItemResponse<TestItem> response = executor.readItem(item.id, pk(item), new CosmosItemRequestOptions(), TestItem.class);

        assertEquals(200, response.getStatusCode());
        assertEquals(item.id, response.getItem().id);
        assertEquals("read-opts", response.getItem().value);
    }

    @Test
    public void testReadManyWithoutSessionToken() {
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
        final String partition = newPartition();
        executor.createItem(itemIn(partition, "read-all-1"));
        executor.createItem(itemIn(partition, "read-all-2"));
        executor.createItem(itemIn(partition, "read-all-3"));

        final CosmosPagedIterable<TestItem> response = executor.readAllItems(new PartitionKey(partition), TestItem.class);

        assertEquals(3, response.stream().count());
    }

    @Test
    public void testReadAllItemsWithOptions() {
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
        final String partition = newPartition();
        executor.createItem(itemIn(partition, "stream-all-1"));
        executor.createItem(itemIn(partition, "stream-all-2"));

        final List<TestItem> result = executor.streamAllItems(new PartitionKey(partition), TestItem.class).toList();

        assertEquals(2, result.size());
    }

    @Test
    public void testStreamAllItemsWithOptions() {
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

        final List<TestItem> result = executor.streamItems(Filters.and(Filters.eq("id", item.id), Filters.eq("name", item.name)), TestItem.class).toList();

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
        final TestItem item = itemIn(newPartition(), "by-id-spec");
        executor.createItem(item);

        final SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c WHERE c.id = @id", Arrays.asList(new SqlParameter("@id", item.id)));
        final List<TestItem> results = executor.queryItems(querySpec, TestItem.class).stream().toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    @Test
    public void testStreamItemsByIdSelectStarRunsOnEmulator() {
        final TestItem item = itemIn(newPartition(), "by-id-stream");
        executor.createItem(item);

        final List<TestItem> results = executor.streamItems("SELECT * FROM c WHERE c.id = '" + item.id + "'", TestItem.class).toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    @Test
    public void testStreamItemsConditionByIdProjectingIdRunsOnEmulator() {
        final TestItem item = itemIn(newPartition(), "cond-by-id");
        executor.createItem(item);

        // Projecting only the indexed "id" -> generated SQL
        // SELECT VALUE { "id": c.id } FROM test_item c WHERE c.id = @p0.
        final List<TestItem> results = executor.streamItems(Arrays.asList("id"), Filters.eq("id", item.id), TestItem.class).toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
        assertNull(results.get(0).value, "value was not projected, so it must be null");
    }

    @Test
    public void testStreamItemsConditionByIdAndPartitionKeyRunsOnEmulator() {
        // The real-execution counterpart of testStreamItemsWithConditionUsesAliasQualifiedCosmosSql: both
        // predicate paths ("id" and partition key "name") are indexed and only "id" is projected, so the
        // alias-qualified SQL SELECT VALUE { "id": c.id } FROM test_item c WHERE c.id = @p0 AND c.name = @p1 runs on the
        // emulator and proves the generated query is accepted by a real Cosmos query engine.
        final TestItem item = itemIn(newPartition(), "cond-by-id-pk");
        executor.createItem(item);

        final List<TestItem> results = executor
                .streamItems(Arrays.asList("id"), Filters.and(Filters.eq("id", item.id), Filters.eq("name", item.name)), TestItem.class)
                .toList();

        assertEquals(1, results.size());
        assertEquals(item.id, results.get(0).id);
    }

    // ----------------------------------------------------------------------------------------------------
    // rewritePositionalParameters (pure logic - no Cosmos connection required)
    // ----------------------------------------------------------------------------------------------------

    // ---- 2026-10-04 coverageDC ----

    /**
     * Live end-to-end check of the coalesce fix: a raw {@code ??} expression combined with a bound condition executes on the real
     * query engine and filters correctly (HEAD rejected both queries client-side with "Query parameter count mismatch"). Only
     * indexed paths ({@code id}, partition key {@code name}) are filtered/projected, so this runs on the vNext emulator too; a
     * coalesce over a property the items lack ({@code missing}) is accepted by the emulator and yields the fallback value.
     */
    @Test
    public void testCoalesceWithBoundConditionRunsOnEmulator_coverageDC() {
        final String partition = newPartition();
        final TestItem first = itemIn(partition, "coalesce-1");
        final TestItem second = itemIn(partition, "coalesce-2");
        executor.createItem(first);
        executor.createItem(second);

        try {
            // SELECT VALUE { "id": c.id } FROM test_item c WHERE (c.name = @p0) AND ((c.id ?? 'none') = '<first.id>')
            final List<TestItem> byId = executor
                    .streamItems(Arrays.asList("id"), Filters.and(Filters.eq("name", partition), Filters.expr("(id ?? 'none') = '" + first.id + "'")),
                            TestItem.class)
                    .toList();
            assertEquals(List.of(first.id), byId.stream().map(it -> it.id).toList());

            final List<String> fallback = executor
                    .streamItems(Arrays.asList("id"), Filters.and(Filters.eq("name", partition), Filters.expr("(missing ?? 'none') = 'none'")), TestItem.class)
                    .map(it -> it.id)
                    .sorted()
                    .toList();
            assertEquals(java.util.stream.Stream.of(first.id, second.id).sorted().toList(), fallback);
        } finally {
            executor.deleteItem(first.id, pk(first), new CosmosItemRequestOptions());
            executor.deleteItem(second.id, pk(second), new CosmosItemRequestOptions());
        }
    }

    /**
     * Live end-to-end check of the ESCAPE fix: {@code id LIKE 'esc!_%' ESCAPE '!'} next to a bound partition-key condition executes on
     * the real query engine and matches only the id with a literal underscore, while the same pattern without ESCAPE also matches the
     * id where {@code _} is a wildcard. HEAD rendered {@code c.id LIKE 'esc!_%' c.escape '!'}, which the engine rejects with 400.
     */
    @Test
    public void testLikeEscapeWithBoundConditionRunsOnEmulator_coverageDC() {
        final String partition = newPartition();
        final String suffix = UUID.randomUUID().toString().substring(0, 8);
        final TestItem literalUnderscore = new TestItem("esc_" + suffix, partition, "escape-1");
        final TestItem otherChar = new TestItem("escX" + suffix, partition, "escape-2");
        executor.createItem(literalUnderscore);
        executor.createItem(otherChar);

        try {
            final List<String> escaped = executor
                    .streamItems(Arrays.asList("id"), Filters.and(Filters.eq("name", partition), Filters.expr("id LIKE 'esc!_%' ESCAPE '!'")), TestItem.class)
                    .map(it -> it.id)
                    .toList();
            assertEquals(List.of(literalUnderscore.id), escaped);

            final List<String> wildcard = executor
                    .streamItems(Arrays.asList("id"), Filters.and(Filters.eq("name", partition), Filters.expr("id LIKE 'esc_%'")), TestItem.class)
                    .map(it -> it.id)
                    .sorted()
                    .toList();
            assertEquals(java.util.stream.Stream.of(literalUnderscore.id, otherChar.id).sorted().toList(), wildcard);
        } finally {
            executor.deleteItem(literalUnderscore.id, pk(literalUnderscore), new CosmosItemRequestOptions());
            executor.deleteItem(otherChar.id, pk(otherChar), new CosmosItemRequestOptions());
        }
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
