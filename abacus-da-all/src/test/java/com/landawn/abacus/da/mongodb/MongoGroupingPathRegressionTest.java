package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.da.TestBase;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class MongoGroupingPathRegressionTest extends TestBase {

    @Test
    public void compositeGroupBuildsNestedKeyDocuments() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        executor.groupBy(List.of("address.city", "address.zip", "status", "address.city", "status")).close();

        final Document key = new Document("address", new Document("city", "$address.city").append("zip", "$address.zip")).append("status", "$status");
        assertEquals(List.of(new Document("$group", new Document("_id", key))), pipeline.get());

        executor.groupByAndCount(List.of("address.city", "address.zip", "status"), Map.class).close();

        assertEquals(List.of(new Document("$group", new Document("_id", key).append("count", new Document("$sum", 1))),
                new Document("$project", new Document("_id", 0).append("address.city", "$_id.address.city").append("address.zip", "$_id.address.zip")
                        .append("status", "$_id.status").append("count", 1))), pipeline.get());
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
    public void countColumnRejectsNestedOutputPathCollision() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount("count.value", Map.class));
        assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(List.of("status", "count.value"), Map.class));

        // Document rows retain the group key under _id, so the separate count column cannot overwrite it.
        executor.groupByAndCount("count.value", Document.class).close();
        assertEquals(List.of(new Document("$group", new Document("_id", "$count.value").append("count", new Document("$sum", 1)))), pipeline.get());
        executor.groupByAndCount("counter.value", Map.class).close();
        assertEquals("$_id", ((Document) pipeline.get().get(1)).get("$project", Document.class).get("counter.value"));
    }

    @Test
    public void embeddedIdPathGroupingRetainsProjectedValue() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final Document row = new Document("_id", new Document("part", "x"));
        final MongoCollectionExecutor executor = executor(pipeline, List.of(row));

        assertEquals(List.of(row), executor.groupBy("_id.part", Map.class).toList());
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id")))),
                ((Document) pipeline.get().get(1)).get("$project"));

        executor.groupByAndCount("_id.part", Map.class).close();
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id")))).append("count", 1),
                ((Document) pipeline.get().get(1)).get("$project"));
    }

    @Test
    public void embeddedIdPathCompositeGroupingAvoidsParentExclusion() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        executor.groupBy(List.of("_id.part", "status"), Map.class).close();
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id._id.part")))).append("status", "$_id.status"),
                ((Document) pipeline.get().get(1)).get("$project"));
    }

    @Test
    public void mapperDistinctEmbeddedIdPathRetainsProjectedValue() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final Document row = new Document("_id", new Document("part", "x"));
        final MongoCollectionMapper<Map> mapper = new MongoCollectionMapper<>(executor(pipeline, List.of(row)), Map.class);

        assertEquals(List.of(row), mapper.distinct("_id.part").toList());
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id")))),
                ((Document) pipeline.get().get(1)).get("$project"));

        mapper.distinct("_id.part", new Document("active", true)).close();
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(new Document("part", "$_id")))),
                ((Document) pipeline.get().get(2)).get("$project"));
    }

    @Test
    public void embeddedIdProjectionRebuildsNestedAndSiblingFields() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        executor.groupByAndCount(List.of("_id.nested.part", "_id.nested.other", "status"), Map.class).close();

        final Document id = new Document("nested", new Document("part", "$_id._id.nested.part").append("other", "$_id._id.nested.other"));
        assertEquals(new Document("_id", new Document("$mergeObjects", List.of(id))).append("status", "$_id.status").append("count", 1),
                ((Document) pipeline.get().get(1)).get("$project"));
    }

    @Test
    public void embeddedIdArrayGroupingAndDistinctPreserveServerResultShape() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);
        final MongoCollectionMapper<Map> mapper = new MongoCollectionMapper<>(executor, Map.class);

        // Execute the captured pipeline: canned rows cannot detect MongoDB traversing an array-valued _id.
        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            final List<Executable> checks = new ArrayList<>();
            for (final Object key : Arrays.asList(List.of(), List.of("x", "y"), List.of(List.of("x"), List.of()),
                    List.of(new Document("part", "x")), new Document("part", "x"), "x", null)) {
                for (final String fieldName : List.of("_id.part", "_id.nested.part")) {
                    final Document id = fieldName.equals("_id.part") ? new Document("part", key) : new Document("nested", new Document("part", key));
                    final Document row = new Document("_id", id);
                    checks.add(() -> {
                        executor.groupBy(fieldName, Map.class).close();
                        assertServerRows(client, pipeline.get(), row, row);
                    });
                    checks.add(() -> {
                        executor.groupByAndCount(fieldName, Map.class).close();
                        assertServerRows(client, pipeline.get(), row, new Document(row).append("count", 2));
                    });
                    checks.add(() -> {
                        mapper.distinct(fieldName).close();
                        assertServerRows(client, pipeline.get(), row, row);
                    });
                    checks.add(() -> {
                        mapper.distinct(fieldName, new Document("_id", new Document("$exists", true))).close();
                        assertServerRows(client, pipeline.get(), row, row);
                    });
                }
            }
            assertAll(checks);
        }
    }

    @Test
    public void embeddedIdArrayCompositeGroupingPreservesServerResultShape() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);

        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            for (final List<?> key : List.of(List.of(), List.of("x", "y"), List.of(List.of("x"), List.of()))) {
                // The ordinary part key also occupies _id.part in the intermediate composite group document.
                final Document row = new Document("_id", new Document("part", key).append("other", List.of("sibling")))
                        .append("part", List.of("original")).append("status", "active");
                final List<String> fields = List.of("_id.part", "_id.other", "part", "status");
                executor.groupBy(fields, Map.class).close();
                assertServerRows(client, pipeline.get(), row, row);

                executor.groupByAndCount(fields, Map.class).close();
                assertServerRows(client, pipeline.get(), row, new Document(row).append("count", 2));
            }
        }
    }

    private static void assertServerRows(final MongoClient client, final List<? extends Bson> pipeline, final Document input, final Document expected) {
        final List<Bson> stages = new ArrayList<>();
        // Database-level $documents provides deterministic input without creating or modifying a collection.
        stages.add(new Document("$documents", List.of(input, input)));
        stages.addAll(pipeline);
        assertEquals(List.of(expected), client.getDatabase("test").aggregate(stages).into(new ArrayList<>()), stages.toString());
    }

    private static MongoCollectionExecutor executor(final AtomicReference<List<? extends Bson>> pipeline) {
        return executor(pipeline, List.of());
    }

    @SuppressWarnings("unchecked")
    private static MongoCollectionExecutor executor(final AtomicReference<List<? extends Bson>> pipeline, final List<Document> rows) {
        final AggregateIterable<Document> iterable = (AggregateIterable<Document>) Proxy.newProxyInstance(AggregateIterable.class.getClassLoader(),
                new Class<?>[] { AggregateIterable.class }, (proxy, method, args) -> {
                    if (method.getName().equals("iterator")) {
                        final Iterator<Document> iterator = rows.iterator();
                        return Proxy.newProxyInstance(MongoCursor.class.getClassLoader(), new Class<?>[] { MongoCursor.class }, (cursor, cursorMethod, cursorArgs) -> {
                            if (cursorMethod.getName().equals("hasNext")) {
                                return iterator.hasNext();
                            } else if (cursorMethod.getName().equals("next")) {
                                return iterator.next();
                            } else if (cursorMethod.getName().equals("close")) {
                                return null;
                            }
                            throw new UnsupportedOperationException(cursorMethod.getName());
                        });
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        final MongoCollection<Document> collection = (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[] { MongoCollection.class }, (proxy, method, args) -> {
                    if (method.getName().equals("aggregate")) {
                        pipeline.set((List<? extends Bson>) args[0]);
                        return iterable;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return new MongoCollectionExecutor(collection, MongoDBBase.DEFAULT_ASYNC_EXECUTOR);
    }
}
