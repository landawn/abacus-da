package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.da.mongodb.MongoDBBase;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;

/**
 * Review 2026-11-02 of the reactive {@link MongoCollectionMapper}: its private copy of the BSON-value row-type rule, the
 * single-value routing it shares with the executors and the blocking mapper, and the {@code distinct} pipeline for the BSON
 * document and container row types.
 */
public class MongoCollectionMapperReview20261102Test extends MockitoTestBase {

    public static class Item {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    private static final List<Class<?>> TYPES = Arrays.asList(String.class, int.class, Integer.class, boolean.class, Date.class, byte[].class, ObjectId.class,
            Decimal128.class, Binary.class, Code.class, CodeWithScope.class, Symbol.class, BsonString.class, BsonInt32.class, BsonBinary.class,
            BsonRegularExpression.class, BsonNull.class, BsonValue.class, BsonArray.class, BsonDocument.class, RawBsonDocument.class, Document.class, Map.class,
            Object.class, Bson.class, Object[].class, String[].class, List.class, Item.class);

    private static Method staticMethod(final Class<?> owner, final String name) throws NoSuchMethodException {
        final Method method = owner.getDeclaredMethod(name, Class.class);
        method.setAccessible(true);
        return method;
    }

    /** The copy exists only because MongoDBBase's rule is package-private; it must not drift from it. */
    @Test
    public void testBsonValueClassCopyMatchesMongoDBBase() throws Exception {
        final Method copy = staticMethod(MongoCollectionMapper.class, "isBsonValueClass");
        final Method base = staticMethod(MongoDBBase.class, "isBsonValueClass");

        for (final Class<?> type : TYPES) {
            assertEquals(base.invoke(null, type), copy.invoke(null, type), type::getName);
        }
    }

    /** The reactive mapper, the reactive executor and the blocking mapper must route the same row types as single values. */
    @Test
    public void testSingleValueRowTypeRuleMatchesExecutorAndBlockingMapper() throws Exception {
        final Method mapperRule = staticMethod(MongoCollectionMapper.class, "isSingleValueRowType");
        final Method executorRule = staticMethod(MongoCollectionExecutor.class, "isSingleValueType");
        final Method blockingMapperRule = staticMethod(com.landawn.abacus.da.mongodb.MongoCollectionMapper.class, "isSingleValueRowType");

        for (final Class<?> type : TYPES) {
            final Object actual = mapperRule.invoke(null, type);
            assertEquals(!type.isAssignableFrom(Document.class) && (Boolean) executorRule.invoke(null, type), actual, type::getName);
            assertEquals(blockingMapperRule.invoke(null, type), actual, type::getName);
        }
    }

    /** Executor over a mocked collection whose every aggregate emits {@code rows}; the pipelines sent are recorded. */
    private static MongoCollectionExecutor executor(final List<List<? extends Bson>> pipelines, final List<Document> rows) {
        final MongoCollection<Document> collection = mock(MongoCollection.class);

        when(collection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            pipelines.add(invocation.getArgument(0));
            final AggregatePublisher<Document> publisher = mock(AggregatePublisher.class);
            doAnswer(subscribe -> {
                Flux.fromIterable(rows).subscribe(subscribe.<Subscriber<Document>> getArgument(0));
                return null;
            }).when(publisher).subscribe(any());
            return publisher;
        });

        return new MongoCollectionExecutor(collection);
    }

    private static Document projectStage(final String fieldName) {
        return new Document("$project", new Document("_id", 0).append(fieldName, "$_id"));
    }

    /**
     * A {@code BsonDocument} or {@code RawBsonDocument} mapper is a Map row type: its {@code distinct} rows are re-projected
     * under the field name and arrive holding BSON values (on HEAD every row failed with {@code ClassCastException}, or could
     * not be created for {@code RawBsonDocument}).
     */
    @Test
    public void testDistinctIntoBsonDocumentRowTypes() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        final List<Document> rows = List.of(new Document("a", new Document("b", "x")), new Document("a", new Document("b", null)));
        final MongoCollectionExecutor executor = executor(pipelines, rows);

        final List<BsonDocument> bson = new MongoCollectionMapper<>(executor, BsonDocument.class).distinct("a.b").collectList().block();
        final List<RawBsonDocument> raw = new MongoCollectionMapper<>(executor, RawBsonDocument.class).distinct("a.b", new Document("k", 1))
                .collectList()
                .block();

        final BsonDocument x = new BsonDocument("a", new BsonDocument("b", new BsonString("x")));
        final BsonDocument nullB = new BsonDocument("a", new BsonDocument("b", BsonNull.VALUE));
        assertEquals(List.of(x, nullB), bson);
        assertEquals(2, raw.size());
        assertInstanceOf(RawBsonDocument.class, raw.get(0));
        assertEquals(x, raw.get(0));
        assertEquals(nullB, raw.get(1));

        final Document group = new Document("$group", new Document("_id", "$a.b"));
        assertEquals(List.of(group, projectStage("a.b")), pipelines.get(0));
        assertEquals(3, pipelines.get(1).size());
        assertEquals(group, pipelines.get(1).get(1));
        assertEquals(projectStage("a.b"), pipelines.get(1).get(2));
    }

    /** Pins the documented shape for an object-array or Collection row type: the row's one top-level (here nested) value. */
    @Test
    public void testDistinctIntoContainerRowTypeHoldsTopLevelValue() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        final MongoCollectionExecutor executor = executor(pipelines, List.of(new Document("a", new Document("b", "x"))));

        final List<Object[]> arrays = new MongoCollectionMapper<>(executor, Object[].class).distinct("a.b").collectList().block();
        final List<List> lists = new MongoCollectionMapper<>(executor, List.class).distinct("a.b").collectList().block();

        assertEquals(1, arrays.size());
        assertArrayEquals(new Object[] { new Document("b", "x") }, arrays.get(0));
        assertEquals(List.of(List.of(new Document("b", "x"))), lists);
        assertEquals(projectStage("a.b"), pipelines.get(0).get(1));
        assertEquals(projectStage("a.b"), pipelines.get(1).get(1));
    }
}
