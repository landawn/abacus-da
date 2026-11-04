package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.ModelBridgeInternal;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.azure.cosmos.util.UtilBridgeInternal;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

import reactor.core.publisher.Flux;

class CosmosContainerExecutorReview20261102Test {

    private CosmosContainer container;
    private CosmosPagedIterable<Item> rows;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        container = mock(CosmosContainer.class);
        rows = mock(CosmosPagedIterable.class);
        when(rows.streamByPage()).thenAnswer(CosmosPagedTestSupport.noPages());
    }

    private String query(final NamingPolicy namingPolicy, final Condition condition) {
        final org.mockito.ArgumentCaptor<SqlQuerySpec> spec = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(container.queryItems(spec.capture(), any(), eq(Item.class))).thenReturn(rows);

        new CosmosContainerExecutor(container, namingPolicy).streamItems(condition, Item.class).count();

        return spec.getValue().getQueryText();
    }

    /**
     * A top-level property whose name starts with {@code $} (the common JSON {@code $type}) was left unqualified ({@code $type = @p0}),
     * which Cosmos DB rejects as a syntax error because its grammar has no {@code $} token. It is now qualified and written with the
     * bracket accessor, like a {@code $} name after a path separator already was.
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "SNAKE_CASE", "CAMEL_CASE", "SCREAMING_SNAKE_CASE" }, mode = Mode.INCLUDE)
    void dollarPrefixedTopLevelPropertyIsQualifiedWithTheBracketAccessor(final NamingPolicy namingPolicy) {
        final String from = namingPolicy == NamingPolicy.SCREAMING_SNAKE_CASE ? "ITEM" : "item";

        assertEquals("SELECT * FROM " + from + " c WHERE c[\"$type\"] = @p0", query(namingPolicy, Filters.eq("$type", "order")));
        assertEquals("SELECT * FROM " + from + " c WHERE IS_NULL(c[\"$type\"])", query(namingPolicy, Filters.isNull("$type")));
        assertEquals("SELECT * FROM " + from + " c WHERE c[\"$type\"] IN (@p0, @p1)", query(namingPolicy, Filters.in("$type", List.of("a", "b"))));
        assertEquals("SELECT * FROM " + from + " c WHERE c[\"$\"] = 1 OR c[\"$1\"] = 2", query(namingPolicy, Filters.expr("$ = 1 OR $1 = 2")));
    }

    /**
     * {@code NOT} before a {@code $} name is the unary keyword: on HEAD the name was not an operand, so {@code NOT} was read as a
     * property and two operands were juxtaposed ({@code c.active = c["NOT"] $type}).
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "SNAKE_CASE", "CAMEL_CASE" }, mode = Mode.INCLUDE)
    void notBeforeDollarPrefixedPropertyIsTheUnaryKeyword(final NamingPolicy namingPolicy) {
        assertEquals("SELECT * FROM item c WHERE c.active = NOT c[\"$type\"] AND NOT IS_NULL(c[\"$type\"])",
                query(namingPolicy, Filters.expr("active = NOT $type AND $type IS NOT NULL")));
        assertEquals("SELECT * FROM item c WHERE EXISTS(SELECT value t FROM t IN c.tags WHERE t[\"$type\"] = 1) AND c[\"$x\"] = 2",
                query(namingPolicy, Filters.expr("EXISTS(SELECT VALUE t FROM t IN tags WHERE t.$type = 1) AND $x = 2")));
    }

    // ---- page-by-page streaming ----

    private final AtomicInteger fetchedPages = new AtomicInteger();
    private final AtomicInteger cancellations = new AtomicInteger();

    /**
     * A real SDK paged iterable over 1000 pages of two items each. Pages are produced on demand, like the SDK's query paginator, so
     * {@link #fetchedPages} counts the pages the stream requests.
     */
    private CosmosPagedIterable<String> pagedIterable() {
        return UtilBridgeInternal.createCosmosPagedIterable(
                UtilBridgeInternal.createCosmosPagedFlux(options -> Flux.<FeedResponse<String>, Integer> generate(() -> 0, (page, sink) -> {
                    if (page >= 1000) {
                        sink.complete();
                    } else {
                        fetchedPages.incrementAndGet();
                        sink.next(ModelBridgeInternal.createFeedResponse(new ArrayList<>(List.of("a" + page, "b" + page)), new HashMap<>()));
                    }

                    return page + 1;
                }).doOnCancel(cancellations::incrementAndGet)));
    }

    private List<Function<CosmosContainerExecutor, Stream<String>>> streamMethods() {
        final CosmosPagedIterable<String> pages = pagedIterable();
        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        final SqlQuerySpec spec = new SqlQuerySpec("SELECT * FROM c");
        final PartitionKey key = new PartitionKey("p");
        when(container.readAllItems(key, String.class)).thenReturn(pages);
        when(container.readAllItems(key, options, String.class)).thenReturn(pages);
        when(container.queryItems("SELECT * FROM c", null, String.class)).thenReturn(pages);
        when(container.queryItems(spec, null, String.class)).thenReturn(pages);

        return List.of(executor -> executor.streamAllItems(key, String.class), executor -> executor.streamAllItems(key, options, String.class),
                executor -> executor.streamItems("SELECT * FROM c", String.class), executor -> executor.streamItems(spec, String.class));
    }

    /**
     * HEAD streamed through {@code CosmosPagedIterable.stream()}, which subscribes at once to the SDK's item flux; its flatMap
     * requests 256 pages, so 256 pages were read when the stream was created and closing the stream did not cancel the reads.
     * Now no page is read before consumption, {@code limit(3)} over two-item pages reads the two pages it needs plus one page
     * ahead, and the close done by the terminal operation cancels the subscription. The page source here is purely demand-driven,
     * so this measures the executor's own read-ahead; the real SDK's query pipeline reads much further ahead on its own (more than
     * 256 pages for a query, measured against a fake gateway with azure-cosmos 4.83; see the class documentation of the executor).
     */
    @Test
    void streamsReadNoPageBeforeConsumptionAndOnlyTheNeededPagesForLimit() {
        final CosmosContainerExecutor executor = new CosmosContainerExecutor(container);

        for (final Function<CosmosContainerExecutor, Stream<String>> method : streamMethods()) {
            fetchedPages.set(0);
            cancellations.set(0);

            final Stream<String> stream = method.apply(executor);
            assertEquals(0, fetchedPages.get(), "pages read when the stream is created");

            assertEquals(List.of("a0", "b0", "a1"), stream.limit(3).toList());
            assertEquals(3, fetchedPages.get(), "pages read for limit(3)");
            assertEquals(1, cancellations.get(), "subscription cancelled by the terminal operation");

            fetchedPages.set(0);
            cancellations.set(0);
            assertEquals("a0", method.apply(executor).first().orElseThrow());
            assertEquals(2, fetchedPages.get(), "pages read for first()");
            assertEquals(1, cancellations.get());
        }
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void conditionStreamReadsNoPageBeforeConsumptionAndAllPagesWhenCounted() {
        when(container.queryItems(any(SqlQuerySpec.class), any(), eq(Item.class))).thenReturn((CosmosPagedIterable) pagedIterable());

        // Only counted, so the String elements are never cast to Item.
        final Stream<Item> stream = new CosmosContainerExecutor(container).streamItems(Filters.eq("id", "x"), Item.class);
        assertEquals(0, fetchedPages.get());
        assertEquals(2000, stream.count());
        assertEquals(1000, fetchedPages.get());
    }

    /** Over this demand-driven source, a stream abandoned after iterator() reads one page ahead and stops when it is closed. */
    @Test
    void closingAnIteratedStreamCancelsTheRemainingReads() {
        final PartitionKey key = new PartitionKey("p");
        when(container.readAllItems(key, String.class)).thenReturn(pagedIterable());

        final Stream<String> stream = new CosmosContainerExecutor(container).streamAllItems(key, String.class);
        final Iterator<String> iterator = stream.iterator();
        assertEquals("a0", iterator.next());
        assertEquals(2, fetchedPages.get());
        assertEquals(0, cancellations.get());

        stream.close();
        assertEquals(1, cancellations.get());
        assertEquals(2, fetchedPages.get());
    }

    /**
     * Closing a stream that was never consumed (e.g. try-with-resources left early) reads nothing: with Stream.defer, the close ran
     * the deferred supplier, which subscribed and fetched the first page only to cancel it.
     */
    @Test
    void closingAnUnconsumedStreamReadsNoPage() {
        final CosmosContainerExecutor executor = new CosmosContainerExecutor(container);

        for (final Function<CosmosContainerExecutor, Stream<String>> method : streamMethods()) {
            fetchedPages.set(0);

            method.apply(executor).filter(item -> true).close();

            assertEquals(0, fetchedPages.get());
        }
    }

    /** As on HEAD, a failing page surfaces as the SDK's exception when the stream is consumed, not when it is created. */
    @Test
    void pageFailureIsThrownOnConsumption() {
        final CosmosException failure = mock(CosmosException.class);
        final PartitionKey key = new PartitionKey("p");
        when(container.readAllItems(key, String.class))
                .thenReturn(UtilBridgeInternal.createCosmosPagedIterable(UtilBridgeInternal.<String> createCosmosPagedFlux(options -> Flux.error(failure))));

        final Stream<String> stream = new CosmosContainerExecutor(container).streamAllItems(key, String.class);

        assertSame(failure, assertThrows(CosmosException.class, stream::count));
    }

    /** Errors the SDK raises when the query is created (a partition key together with a feed range) are still thrown at once. */
    @Test
    void queryCreationFailureIsStillThrownByTheStreamMethod() {
        final IllegalArgumentException failure = new IllegalArgumentException("Setting partitionKey and feedRange at the same time is not allowed");
        final CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        when(container.queryItems("SELECT * FROM c", options, String.class)).thenThrow(failure);

        assertSame(failure, assertThrows(IllegalArgumentException.class,
                () -> new CosmosContainerExecutor(container).streamItems("SELECT * FROM c", options, String.class)));
    }

    /** Empty pages and pages without a result list contribute no items. */
    @Test
    @SuppressWarnings("unchecked")
    void emptyAndNullPageResultsAreSkipped() {
        final CosmosPagedIterable<String> pages = mock(CosmosPagedIterable.class);
        final FeedResponse<String> nullResults = mock(FeedResponse.class);
        when(nullResults.getResults()).thenReturn(null);
        when(pages.streamByPage()).thenAnswer(invocation -> java.util.stream.Stream.of(CosmosPagedTestSupport.page(List.of()), nullResults,
                CosmosPagedTestSupport.page(List.of("x")), CosmosPagedTestSupport.page(List.of())));
        final PartitionKey key = new PartitionKey("p");
        when(container.readAllItems(key, String.class)).thenReturn(pages);

        assertEquals(List.of("x"), new CosmosContainerExecutor(container).streamAllItems(key, String.class).toList());
    }

    public static class Item {
        private String id;
        private boolean active;
        private List<String> tags;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(final boolean active) {
            this.active = active;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(final List<String> tags) {
            this.tags = tags;
        }
    }
}
