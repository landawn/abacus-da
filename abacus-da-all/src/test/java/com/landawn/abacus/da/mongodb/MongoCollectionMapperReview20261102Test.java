package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.AsyncExecutor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

/**
 * Review 2026-11-02 of the blocking {@link MongoCollectionMapper}: the {@code distinct} pipeline for the BSON document and
 * container row types, and the single-value routing rule it shares with the executor.
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

    /** Executor over a mocked collection whose every aggregate returns {@code rows}; the pipelines sent are recorded. */
    private static MongoCollectionExecutor executor(final List<List<? extends Bson>> pipelines, final List<Document> rows) {
        final MongoCollection<Document> collection = mock(MongoCollection.class);

        when(collection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            pipelines.add(invocation.getArgument(0));
            final Iterator<Document> iterator = rows.iterator();
            final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
            final MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(aggregateIterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenAnswer(ignored -> iterator.hasNext());
            when(cursor.next()).thenAnswer(ignored -> iterator.next());
            return aggregateIterable;
        });

        return new MongoCollectionExecutor(collection, mock(AsyncExecutor.class));
    }

    private static Document projectStage(final String fieldName) {
        return new Document("$project", new Document("_id", 0).append(fieldName, "$_id"));
    }

    /**
     * A {@code BsonDocument} or {@code RawBsonDocument} mapper is a Map row type: its {@code distinct} rows are re-projected
     * under the field name and arrive holding BSON values. On HEAD the rows failed with {@code ClassCastException}
     * ("Document cannot be cast to BsonValue") and {@code RawBsonDocument} could not be created at all.
     */
    @Test
    public void testDistinctIntoBsonDocumentRowTypes() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        final List<Document> rows = List.of(new Document("a", new Document("b", "x")), new Document("a", new Document("b", null)));
        final MongoCollectionExecutor executor = executor(pipelines, rows);

        final List<BsonDocument> bson = new MongoCollectionMapper<>(executor, BsonDocument.class).distinct("a.b").toList();
        final List<RawBsonDocument> raw = new MongoCollectionMapper<>(executor, RawBsonDocument.class).distinct("a.b", new Document("k", 1)).toList();

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

    /**
     * Pins the documented shape for an object-array or Collection row type: the row's one top-level value, which for a
     * dotted field is the nested document re-projected by the {@code $project} stage (as {@code groupBy} returns it).
     */
    @Test
    public void testDistinctIntoContainerRowTypeHoldsTopLevelValue() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        final List<Document> rows = List.of(new Document("a", new Document("b", "x")));
        final MongoCollectionExecutor executor = executor(pipelines, rows);

        final List<Object[]> arrays = new MongoCollectionMapper<>(executor, Object[].class).distinct("a.b").toList();
        final List<List> lists = new MongoCollectionMapper<>(executor, List.class).distinct("a.b").toList();

        assertEquals(1, arrays.size());
        assertArrayEquals(new Object[] { new Document("b", "x") }, arrays.get(0));
        assertEquals(List.of(List.of(new Document("b", "x"))), lists);
        assertEquals(projectStage("a.b"), pipelines.get(0).get(1));
        assertEquals(projectStage("a.b"), pipelines.get(1).get(1));
    }

    /**
     * The mapper's {@code distinct} routing must classify row types as the executor's own scalar rule does (the executor's
     * callers exclude types that can hold a Document separately), or the two disagree on which rows have a field to read.
     */
    @Test
    public void testSingleValueRowTypeRuleMatchesExecutor() throws Exception {
        final Method mapperRule = MongoCollectionMapper.class.getDeclaredMethod("isSingleValueRowType", Class.class);
        final Method executorRule = MongoCollectionExecutor.class.getDeclaredMethod("isSingleValueType", Class.class);
        mapperRule.setAccessible(true);
        executorRule.setAccessible(true);

        final List<Class<?>> types = Arrays.asList(String.class, int.class, Integer.class, boolean.class, Date.class, byte[].class, ObjectId.class,
                Decimal128.class, Binary.class, Code.class, Symbol.class, BsonString.class, BsonInt32.class, BsonValue.class, BsonArray.class,
                BsonDocument.class, RawBsonDocument.class, Document.class, Map.class, Object.class, Bson.class, Object[].class, String[].class, List.class,
                Item.class);

        for (final Class<?> type : types) {
            final boolean expected = !type.isAssignableFrom(Document.class) && (Boolean) executorRule.invoke(null, type);
            assertEquals(expected, mapperRule.invoke(null, type), type::getName);
        }

        assertEquals(Boolean.TRUE, mapperRule.invoke(null, Code.class));
        assertEquals(Boolean.TRUE, mapperRule.invoke(null, BsonString.class));
        assertEquals(Boolean.FALSE, mapperRule.invoke(null, BsonDocument.class));
        assertEquals(Boolean.FALSE, mapperRule.invoke(null, BsonArray.class));
        assertEquals(Boolean.FALSE, mapperRule.invoke(null, Item.class));
    }
}
