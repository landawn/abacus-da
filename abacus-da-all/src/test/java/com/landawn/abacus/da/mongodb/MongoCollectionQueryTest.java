package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.u.Optional;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;

public class MongoCollectionQueryTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testCount() {
        when(mockCollection.countDocuments()).thenReturn(100L);

        long result = executor.count();
        Assertions.assertEquals(100L, result);
    }

    @Test
    public void testCountWithFilter() {
        Document filter = new Document("status", "active");
        when(mockCollection.countDocuments(filter)).thenReturn(50L);

        long result = executor.count(filter);
        Assertions.assertEquals(50L, result);
    }

    @Test
    public void testCountWithFilterAndNullOptions() {
        Document filter = new Document("a", 1);
        when(mockCollection.countDocuments(any(Bson.class))).thenReturn(5L);
        long result = executor.count(filter, null);
        Assertions.assertEquals(5L, result);
    }

    @Test
    public void testCountWithFilterAndOptions() {
        Document filter = new Document("status", "active");
        CountOptions options = new CountOptions().limit(10);
        when(mockCollection.countDocuments(filter, options)).thenReturn(10L);

        long result = executor.count(filter, options);
        Assertions.assertEquals(10L, result);
    }

    @Test
    public void testDottedSelectNameCheckToleratesNullNames() {
        // Pin: the dotted-name check of query(Collection, ...) -> Dataset is null-safe, so a null select name still fails
        // with the projection's IllegalArgumentException (as before the fix), whichever position it has.
        stubFindRows(Arrays.asList(new Document("_id", 1).append("address", new Document("city", "Paris"))));

        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query(Arrays.asList(null, "address.city"), new Document(), java.util.Map.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query(Arrays.asList("address.city", null), new Document(), java.util.Map.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query(Arrays.asList("name", null), new Document(), java.util.Map.class));
    }

    @Test
    public void testEstimatedDocumentCount() {
        when(mockCollection.estimatedDocumentCount()).thenReturn(1000L);

        long result = executor.estimatedDocumentCount();
        Assertions.assertEquals(1000L, result);
    }

    @Test
    public void testEstimatedDocumentCountWithNullOptionsUsesDefaults() {
        // Null options fall back to the no-options driver call (family convention),
        // instead of the driver's notNull IAE.
        when(mockCollection.estimatedDocumentCount()).thenReturn(42L);

        long count = executor.estimatedDocumentCount((EstimatedDocumentCountOptions) null);

        Assertions.assertEquals(42L, count);
        verify(mockCollection).estimatedDocumentCount();
    }

    @Test
    public void testEstimatedDocumentCountWithOptions() {
        EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions();
        when(mockCollection.estimatedDocumentCount(options)).thenReturn(1000L);

        long result = executor.estimatedDocumentCount(options);
        Assertions.assertEquals(1000L, result);
    }

    @Test
    public void testExistsWithBsonFilter() {
        Document filter = new Document("name", "test");
        when(mockCollection.countDocuments(any(), any(CountOptions.class))).thenReturn(1L);

        boolean result = executor.exists(filter);
        Assertions.assertTrue(result);
    }

    @Test
    public void testExistsWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockCollection.countDocuments(any(), any(CountOptions.class))).thenReturn(0L);

        boolean result = executor.exists(objectId);
        Assertions.assertFalse(result);
    }

    @Test
    public void testExistsWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        when(mockCollection.countDocuments(any(), any(CountOptions.class))).thenReturn(1L);

        boolean result = executor.exists(objectId);
        Assertions.assertTrue(result);
    }

    @Test
    public void testFindFirst() {
        Document filter = new Document("name", "test");
        Document doc = new Document("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Document> result = executor.findFirst(filter);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testFindFirstBsonProjectionObjectRowTypeReturnsRawDocument() {
        Document filter = new Document("active", true);
        Document doc = new Document("_id", new ObjectId()).append("email", "a@b.c").append("name", "John");
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Object> result = executor.findFirst(new Document("email", 1), filter, null, Object.class);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertSame(doc, result.get(), "Document-assignable rowType receives the raw document instead of readRow's scalar fallback");
    }

    @Test
    public void testFindFirstSingleDottedScalarProjectionReadsNestedValue() {
        Document filter = new Document("active", true);
        when(mockFindIterable.first()).thenReturn(new Document("_id", new ObjectId()).append("address", new Document("city", "Paris")));

        Optional<String> result = executor.findFirst(Arrays.asList("address.city"), filter, String.class);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("Paris", result.get());
    }

    @Test
    public void testFindFirstSingleProjectionObjectRowTypeReturnsRawDocument() {
        Document filter = new Document("active", true);
        Document doc = new Document("_id", new ObjectId()).append("email", "a@b.c");
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Object> result = executor.findFirst(Arrays.asList("email"), filter, Object.class);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertSame(doc, result.get(), "Document-assignable rowType receives the raw projected document, matching list/stream");
    }

    @Test
    public void testFindFirstSingleScalarProjectionDoesNotFallBackToObjectId() {
        Document filter = new Document("active", true);
        when(mockFindIterable.first()).thenReturn(new Document("_id", new ObjectId()));

        Optional<String> result = executor.findFirst(Arrays.asList("name"), filter, String.class);

        Assertions.assertFalse(result.isPresent(), "A missing projected value is not the document's _id");
    }

    @Test
    public void testFindFirstWithEmptyDocumentRowTypeReturnsDocument() {
        Document filter = new Document("active", true);
        Document doc = new Document();
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Document> result = executor.findFirst(filter, Document.class);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get().isEmpty());
    }

    @Test
    public void testFindFirstWithProjectionFilterSortClassEmpty() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("id", 1);
        Document sort = new Document("name", 1);
        when(mockFindIterable.first()).thenReturn(null);
        Optional<Document> result = executor.findFirst(projection, filter, sort, Document.class);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testFindFirstWithProjectionFilterSortClassFound() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("id", 1);
        Document sort = new Document("name", 1);
        Document doc = new Document("name", "x");
        when(mockFindIterable.first()).thenReturn(doc);
        Optional<Document> result = executor.findFirst(projection, filter, sort, Document.class);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testFindFirstWithRowType() {
        Document filter = new Document("name", "test");
        Document doc = new Document("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Document> result = executor.findFirst(filter, Document.class);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testFindFirstWithScalarRowTypeSkipsEmptyDocument() {
        Document filter = new Document("active", true);
        when(mockFindIterable.first()).thenReturn(new Document());

        Optional<Integer> result = executor.findFirst(filter, Integer.class);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testFindFirstWithSelectFilterSortAndClassEmpty() {
        Document filter = new Document();
        Document sort = new Document();
        when(mockFindIterable.first()).thenReturn(null);
        Optional<Document> r = executor.findFirst(Arrays.asList("a"), filter, sort, Document.class);
        Assertions.assertFalse(r.isPresent());
    }

    @Test
    public void testGetWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockFindIterable.first()).thenReturn(null);

        Optional<Document> result = executor.get(objectId);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testGetWithSelectPropNamesAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<TestEntity> result = executor.get(objectId, Arrays.asList("name"), TestEntity.class);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testGetWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId));
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Document> result = executor.get(objectId);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testGetWithStringObjectIdAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);

        Optional<TestEntity> result = executor.get(objectId, TestEntity.class);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testGettWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockFindIterable.first()).thenReturn(null);

        Document result = executor.gett(objectId);
        Assertions.assertNull(result);
    }

    @Test
    public void testGettWithSelectPropNamesAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);

        TestEntity result = executor.gett(objectId, Arrays.asList("name"), TestEntity.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGettWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId));
        when(mockFindIterable.first()).thenReturn(doc);

        Document result = executor.gett(objectId);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testList() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(filter);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testListRejectsNegativeCount() {
        Document filter = new Document("status", "active");
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(filter, 0, -1, Document.class));
    }

    @Test
    public void testListRejectsNegativeOffset() {
        Document filter = new Document("status", "active");
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(filter, -1, 10, Document.class));
    }

    @Test
    public void testListSingleDottedProjectionReadsNestedScalarValue() {
        // A projection of "address.city" is returned as {address: {city: ...}}. Treating the
        // top-level address document as the scalar produced a JSON-like String instead of "Berlin".
        Document filter = new Document("active", true);
        Document row = new Document("_id", new ObjectId()).append("address", new Document("city", "Berlin"));
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList(row));

        List<String> result = executor.list(Arrays.asList("address.city"), filter, String.class);

        Assertions.assertEquals(Arrays.asList("Berlin"), result);
    }

    @Test
    public void testListSingleMissingProjectionDoesNotReturnImplicitId() {
        Document filter = new Document("active", true);
        Document row = new Document("_id", new ObjectId());
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList(row));

        List<String> result = executor.list(Arrays.asList("nickname"), filter, String.class);

        Assertions.assertEquals(Arrays.asList((String) null), result);
    }

    @Test
    public void testListSingleProjectionPreservesRawDocumentForDocumentSupertypes() {
        Document filter = new Document("active", true);
        Document row = new Document("name", "alice");
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList(row));

        List<Object> objects = executor.list(Arrays.asList("name"), filter, Object.class);
        List<Bson> bsons = executor.list(Arrays.asList("name"), filter, Bson.class);

        Assertions.assertSame(row, objects.get(0));
        Assertions.assertSame(row, bsons.get(0));
    }

    @Test
    public void testListWithBsonProjectionFilterAndSort() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(projection, filter, sort, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testListWithBsonProjectionFilterSortOffsetCount() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(projection, filter, sort, 0, 10, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testListWithMaxCountDoesNotCallLimitWithMaxValue() {
        Document filter = new Document("status", "active");
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList());
        executor.list(filter, 0, Integer.MAX_VALUE, Document.class);
        verify(mockFindIterable, org.mockito.Mockito.never()).limit(Integer.MAX_VALUE);
    }

    @Test
    public void testListWithNullFilterThrowsIAE() {
        // A null filter is now rejected with IllegalArgumentException (it is no longer silently treated
        // as match-all), and the driver must not be touched.
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(null, 0, 10, Document.class));
        verify(mockCollection, org.mockito.Mockito.never()).find();
    }

    @Test
    public void testListWithPositiveOffsetCallsSkip() {
        Document filter = new Document("status", "active");
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList());
        executor.list(filter, 5, 10, Document.class);
        verify(mockFindIterable).skip(5);
        verify(mockFindIterable).limit(10);
    }

    @Test
    public void testListWithRowType() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(filter, Document.class);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testListWithSelectPropNamesAndFilter() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("name", "a"), new Document("name", "b"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(Arrays.asList("name"), filter, Document.class);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testListWithSelectPropNamesFilterAndSort() {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(Arrays.asList("name"), filter, sort, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testListWithSelectPropNamesFilterOffsetCount() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(Arrays.asList("name"), filter, 0, 10, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testListWithSelectPropNamesFilterSortOffsetCount() {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(Arrays.asList("name"), filter, sort, 0, 10, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    // Test entity class
    @Test
    public void testListWithZeroCountReturnsEmptyAndDoesNotUseLimitZero() {
        // Regression: limit(0) is treated by the MongoDB driver as "no limit" (return ALL docs).
        // A user-supplied count of 0 must yield zero results, not the whole collection.
        Document filter = new Document("status", "active");
        org.mockito.ArgumentCaptor<Bson> findCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList());

        List<Document> result = executor.list(filter, 0, 0, Document.class);

        Assertions.assertTrue(result.isEmpty());
        verify(mockCollection).find(findCaptor.capture());
        Assertions.assertEquals(new Document("$expr", false), findCaptor.getValue());
        // limit(0) must never be applied (it would mean "no limit").
        verify(mockFindIterable, org.mockito.Mockito.never()).limit(0);
    }

    @Test
    public void testQuery() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(filter);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testQueryDottedSelectNameThroughArrayFailsOnEveryOverload() {
        stubFindRows(Arrays.asList(new Document("_id", 1).append("name", "n1").append("address", new Document("city", "Paris")),
                new Document("_id", 2).append("name", "n2").append("address", Arrays.asList(new Document("city", "Rome")))));
        final List<String> names = Arrays.asList("name", "address.city");
        final Bson sort = new Document("_id", 1);

        Assertions.assertThrows(ClassCastException.class, () -> executor.query(names, new Document(), java.util.Map.class));
        Assertions.assertThrows(ClassCastException.class, () -> executor.query(names, new Document(), 0, 5, Document.class));
        Assertions.assertThrows(ClassCastException.class, () -> executor.query(names, new Document(), sort, DottedRowWithoutAddress.class));
        Assertions.assertThrows(ClassCastException.class, () -> executor.query(names, new Document(), sort, 0, 5, java.util.Map.class));

        // Without a dotted name the same rows load as before: the array is just a column value.
        final Dataset plain = executor.query(Arrays.asList("name", "address"), new Document(), java.util.Map.class);
        Assertions.assertEquals(Arrays.asList("n1", "n2"), plain.getColumn("name"));
        Assertions.assertEquals(Arrays.asList(new Document("city", "Rome")), plain.getColumn("address").get(1));
    }

    @Test
    public void testQueryDottedSelectNamesOnEveryOverloadKeepOrderAndRawNestedValues() {
        // Every Collection-projection Dataset overload resolves dotted names from the nested documents (they were all-null
        // columns): requested column order is kept, a missing/null parent or leaf gives null, and the dotted value is the
        // raw nested value (Integer zip although DottedAddress declares no zip), while plain columns are converted as before.
        stubFindRows(Arrays.asList(new Document("_id", 1).append("name", "n1").append("address", new Document("city", "Paris").append("zip", 75001)),
                new Document("_id", 2).append("name", "n2"), new Document("_id", 3).append("name", "n3").append("address", new Document("city", null)),
                new Document("_id", 4).append("name", "n4").append("address", null)));
        final List<String> names = Arrays.asList("address.zip", "name", "address.city");
        final Bson sort = new Document("_id", 1);

        for (final Class<?> rowType : Arrays.asList(java.util.Map.class, java.util.LinkedHashMap.class, Document.class, DottedRow.class)) {
            for (final Dataset ds : Arrays.asList(executor.query(names, new Document(), rowType), executor.query(names, new Document(), 1, 3, rowType),
                    executor.query(names, new Document(), sort, rowType), executor.query(names, new Document(), sort, 1, 3, rowType))) {
                Assertions.assertEquals(names, ds.columnNames(), rowType.getName());
                Assertions.assertEquals(Arrays.asList("n1", "n2", "n3", "n4"), ds.getColumn("name"), rowType.getName());
                Assertions.assertEquals(Arrays.asList(75001, null, null, null), ds.getColumn("address.zip"), rowType.getName());
                Assertions.assertEquals(Arrays.asList("Paris", null, null, null), ds.getColumn("address.city"), rowType.getName());
                Assertions.assertFalse(ds.isFrozen());
            }
        }

        final Dataset withId = executor.query(new java.util.LinkedHashSet<>(Arrays.asList("_id", "address.city")), new Document(), java.util.Map.class);
        Assertions.assertEquals(Arrays.asList("_id", "address.city"), withId.columnNames());
        Assertions.assertEquals(Arrays.asList(1, 2, 3, 4), withId.getColumn("_id"));
        Assertions.assertEquals(Arrays.asList("Paris", null, null, null), withId.getColumn("address.city"));
    }

    @Test
    public void testQueryWithBsonProjectionFilterSort() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(projection, filter, sort, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testQueryWithDottedSelectPropNameEmptyAndArrayResults() {
        stubFindRows(java.util.Collections.emptyList());
        Assertions.assertEquals(0, executor.query(Arrays.asList("name", "address.city"), new Document(), java.util.Map.class).size());

        // Same dotted-path rule as list/findFirst/queryForSingleValue: a path through an array is not a Document path.
        stubFindRows(Arrays.asList(new Document("_id", 1).append("items", Arrays.asList(new Document("sku", "a")))));
        Assertions.assertThrows(ClassCastException.class, () -> executor.query(Arrays.asList("items.sku"), new Document(), java.util.Map.class));
    }

    @Test
    public void testQueryWithDottedSelectPropNameFillsNestedColumn() {
        // Regression: the server returns a dotted projection such as "address.city" nested ({address: {city: ...}}),
        // but the Dataset column was read by the flat key/property "address.city", so the column was all null.
        stubFindRows(Arrays.asList(new Document("_id", 1).append("name", "n1").append("address", new Document("city", "Paris")),
                new Document("_id", 2).append("name", "n2")));

        final Dataset mapRows = executor.query(Arrays.asList("name", "address.city"), new Document(), java.util.Map.class);
        Assertions.assertEquals(Arrays.asList("name", "address.city"), mapRows.columnNames());
        Assertions.assertEquals(Arrays.asList("n1", "n2"), mapRows.getColumn("name"));
        Assertions.assertEquals(Arrays.asList("Paris", null), mapRows.getColumn("address.city"));

        final Dataset beanRows = executor.query(Arrays.asList("name", "address.city"), new Document(), null, 0, 10, DottedRow.class);
        Assertions.assertEquals(Arrays.asList("name", "address.city"), beanRows.columnNames());
        Assertions.assertEquals(Arrays.asList("n1", "n2"), beanRows.getColumn("name"));
        Assertions.assertEquals(Arrays.asList("Paris", null), beanRows.getColumn("address.city"));

        final Dataset documentRows = executor.query(Arrays.asList("address.city"), new Document(), 0, 10, Document.class);
        Assertions.assertEquals(Arrays.asList("Paris", null), documentRows.getColumn("address.city"));
    }

    @Test
    public void testQueryWithFilterAndRowType() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(filter, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testQueryWithFilterOffsetCountAndRowType() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(filter, 0, 10, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testQueryWithSelectPropNamesAndFilter() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(Arrays.asList("id"), filter, Document.class);
        Assertions.assertNotNull(result);
    }
}
