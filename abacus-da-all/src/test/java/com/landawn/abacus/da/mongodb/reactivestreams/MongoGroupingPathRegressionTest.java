package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import com.landawn.abacus.da.TestBase;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;

public class MongoGroupingPathRegressionTest extends TestBase {

    @Test
    public void compositeGroupBuildsNestedKeyDocuments() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        executor.groupBy(List.of("address.city", "address.zip", "status", "address.city", "status"));

        // Each leaf is wrapped in $ifNull (2026-10-19 sliceT): a missing field then joins the null-key group.
        final Document key = new Document("address", new Document("city", ifNull("$address.city")).append("zip", ifNull("$address.zip"))).append("status",
                ifNull("$status"));
        assertEquals(List.of(new Document("$group", new Document("_id", key))), pipeline.get());

        executor.groupByAndCount(List.of("address.city", "address.zip", "status"), Map.class);

        assertEquals(List.of(new Document("$group", new Document("_id", key).append("count", new Document("$sum", 1))),
                new Document("$project",
                        new Document("_id", 0).append("address.city", "$_id.address.city")
                                .append("address.zip", "$_id.address.zip")
                                .append("status", "$_id.status")
                                .append("count", 1))),
                pipeline.get());
    }

    @Test
    public void compositeGroupRejectsMalformedOrOverlappingPaths() {
        final MongoCollectionExecutor executor = executor(new AtomicReference<>());

        for (final List<String> names : List.of(List.of("address", "address.city"), List.of("address.city", "address"), List.of("address..city"),
                List.of(".city"), List.of("address."), List.of(""), Arrays.asList("address", null))) {
            assertThrows(IllegalArgumentException.class, () -> executor.groupBy(names), names.toString());
            assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(names, Map.class), names.toString());
        }
    }

    @Test
    public void singleFieldGroupAndDistinctRejectMalformedPaths() {
        final MongoCollectionExecutor executor = executor(new AtomicReference<>());
        final MongoCollectionMapper<Map> mapper = new MongoCollectionMapper<>(executor, Map.class);

        // The multi-field overloads always rejected these paths; the single-field grouping and distinct
        // pipelines validate them too (previously they reached the server, or built an empty _id projection key).
        // Validation is eager at publisher construction, so no subscription is needed.
        for (final String name : List.of("address..city", ".city", "address.", "_id.")) {
            assertThrows(IllegalArgumentException.class, () -> executor.groupBy(name), name);
            assertThrows(IllegalArgumentException.class, () -> executor.groupBy(name, Map.class), name);
            assertThrows(IllegalArgumentException.class, () -> executor.groupBy(name, String.class), name);
            assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(name), name);
            assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(name, Map.class), name);
            assertThrows(IllegalArgumentException.class, () -> mapper.distinct(name), name);
            assertThrows(IllegalArgumentException.class, () -> mapper.distinct(name, new Document("active", true)), name);
            // The mapper's single-field grouping delegates to the executor overloads above, so it is rejected eagerly too.
            assertThrows(IllegalArgumentException.class, () -> mapper.groupBy(name), name);
            assertThrows(IllegalArgumentException.class, () -> mapper.groupByAndCount(name), name);
        }
    }

    @Test
    public void countColumnNameHasSingleSource() {
        // The reactive executor's count accumulator and the shared collision check must use the same column name.
        assertEquals(com.landawn.abacus.da.mongodb.MongoDBBase.GROUP_COUNT_FIELD, MongoCollectionExecutor._COUNT);
    }

    @Test
    public void countColumnRejectsNestedOutputPathCollision() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount("count.value", Map.class));
        assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(List.of("status", "count.value"), Map.class));

        // Document rows retain the group key under _id, so the separate count column cannot overwrite it.
        executor.groupByAndCount("count.value", Document.class);
        assertEquals(List.of(new Document("$group", new Document("_id", "$count.value").append("count", new Document("$sum", 1)))), pipeline.get());
        // The multi-field overload is exempt with Document rows too: the composite key stays under _id (no $project stage).
        executor.groupByAndCount(List.of("count"), Document.class);
        assertEquals(List.of(new Document("$group", new Document("_id", new Document("count", ifNull("$count"))).append("count", new Document("$sum", 1)))),
                pipeline.get());
        executor.groupByAndCount("counter.value", Map.class);
        assertEquals("$_id", ((Document) pipeline.get().get(1)).get("$project", Document.class).get("counter.value"));
    }

    @Test
    public void embeddedIdPathGroupingRetainsProjectedValue() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final Document row = new Document("_id", new Document("part", "x"));
        final MongoCollectionExecutor executor = executor(pipeline, List.of(row));

        assertEquals(List.of(row), executor.groupBy("_id.part", Map.class).collectList().block());
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id")))),
                ((Document) pipeline.get().get(1)).get("$project"));

        executor.groupByAndCount("_id.part", Map.class);
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id")))).append("count", 1),
                ((Document) pipeline.get().get(1)).get("$project"));
    }

    @Test
    public void embeddedIdPathCompositeGroupingAvoidsParentExclusion() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        executor.groupBy(List.of("_id.part", "status"), Map.class);
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id._id.part")))).append("status", "$_id.status"),
                ((Document) pipeline.get().get(1)).get("$project"));
    }

    @Test
    public void mapperDistinctEmbeddedIdPathRetainsProjectedValue() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final Document row = new Document("_id", new Document("part", "x"));
        final MongoCollectionMapper<Map> mapper = new MongoCollectionMapper<>(executor(pipeline, List.of(row)), Map.class);

        assertEquals(List.of(row), mapper.distinct("_id.part").collectList().block());
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id")))),
                ((Document) pipeline.get().get(1)).get("$project"));

        mapper.distinct("_id.part", new Document("active", true));
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id")))),
                ((Document) pipeline.get().get(2)).get("$project"));
    }

    @Test
    public void embeddedIdProjectionRebuildsNestedAndSiblingFields() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        executor.groupByAndCount(List.of("_id.nested.part", "_id.nested.other", "status"), Map.class);

        final Document id = new Document("nested", new Document("part", "$_id._id.nested.part").append("other", "$_id._id.nested.other"));
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(id))).append("status", "$_id.status").append("count", 1),
                ((Document) pipeline.get().get(1)).get("$project"));
    }

    private static Document ifNull(final String fieldPath) {
        return new Document("$ifNull", Arrays.asList(fieldPath, null));
    }

    private static MongoCollectionExecutor executor(final AtomicReference<List<? extends Bson>> pipeline) {
        return executor(pipeline, List.of());
    }

    @SuppressWarnings("unchecked")
    private static MongoCollectionExecutor executor(final AtomicReference<List<? extends Bson>> pipeline, final List<Document> rows) {
        final AggregatePublisher<Document> publisher = (AggregatePublisher<Document>) Proxy.newProxyInstance(AggregatePublisher.class.getClassLoader(),
                new Class<?>[] { AggregatePublisher.class }, (proxy, method, args) -> {
                    if (method.getName().equals("subscribe")) {
                        Flux.fromIterable(rows).subscribe((Subscriber<Document>) args[0]);
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        final MongoCollection<Document> collection = (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[] { MongoCollection.class }, (proxy, method, args) -> {
                    if (method.getName().equals("aggregate")) {
                        pipeline.set((List<? extends Bson>) args[0]);
                        return publisher;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return new MongoCollectionExecutor(collection);
    }
}
