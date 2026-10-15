package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

public class MongoGroupingPathIT extends TestBase {

    @Test
    public void embeddedIdArrayGroupingAndDistinctPreserveServerResultShape() {
        final AtomicReference<List<? extends Bson>> pipeline = new AtomicReference<>();
        final MongoCollectionExecutor executor = executor(pipeline);
        final MongoCollectionMapper<Map> mapper = new MongoCollectionMapper<>(executor, Map.class);

        // Execute the captured pipeline: canned rows cannot detect MongoDB traversing an array-valued _id.
        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            final List<Executable> checks = new ArrayList<>();
            for (final Object key : Arrays.asList(List.of(), List.of("x", "y"), List.of(List.of("x"), List.of()), List.of(new Document("part", "x")),
                    new Document("part", "x"), "x", null)) {
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
                final Document row = new Document("_id", new Document("part", key).append("other", List.of("sibling"))).append("part", List.of("original"))
                        .append("status", "active");
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
                        return Proxy.newProxyInstance(MongoCursor.class.getClassLoader(), new Class<?>[] { MongoCursor.class },
                                (cursor, cursorMethod, cursorArgs) -> {
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
