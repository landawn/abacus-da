package com.landawn.abacus.da.mongodb;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.u.OptionalBoolean;
import com.landawn.abacus.util.u.OptionalByte;
import com.landawn.abacus.util.u.OptionalChar;
import com.landawn.abacus.util.u.OptionalDouble;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.u.OptionalLong;
import com.landawn.abacus.util.u.OptionalShort;
import com.mongodb.client.model.CountOptions;

public class AsyncMongoCollectionQueryTest extends AsyncMongoCollectionExecutorTestSupport {

    @Test
    public void testCountDelegatesArgumentsAndResult() throws Exception {

        final var expected = 42L;
        when(mockCollExecutor.count()).thenReturn(expected);

        Assertions.assertEquals(expected, asyncExecutor.count().get());

        verify(mockCollExecutor).count();
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testCountWithFilterAndOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("x", 1);
        CountOptions options = new CountOptions();
        final var expected = 3L;
        when(mockCollExecutor.count(filter, options)).thenReturn(expected);

        Assertions.assertEquals(expected, asyncExecutor.count(filter, options).get());

        verify(mockCollExecutor).count(filter, options);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testCountWithFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("x", 1);
        final var expected = 7L;
        when(mockCollExecutor.count(filter)).thenReturn(expected);

        Assertions.assertEquals(expected, asyncExecutor.count(filter).get());

        verify(mockCollExecutor).count(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testEstimatedDocumentCountDelegatesArgumentsAndResult() throws Exception {

        final var expected = 1000L;
        when(mockCollExecutor.estimatedDocumentCount()).thenReturn(expected);

        Assertions.assertEquals(expected, asyncExecutor.estimatedDocumentCount().get());

        verify(mockCollExecutor).estimatedDocumentCount();
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testEstimatedDocumentCountWithOptionsDelegatesArgumentsAndResult() throws Exception {
        com.mongodb.client.model.EstimatedDocumentCountOptions options = new com.mongodb.client.model.EstimatedDocumentCountOptions();
        final var expected = 2000L;
        when(mockCollExecutor.estimatedDocumentCount(options)).thenReturn(expected);

        Assertions.assertEquals(expected, asyncExecutor.estimatedDocumentCount(options).get());

        verify(mockCollExecutor).estimatedDocumentCount(options);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testExistsWithFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("name", "test");
        final var expected = true;
        when(mockCollExecutor.exists(filter)).thenReturn(expected);

        Assertions.assertEquals(expected, asyncExecutor.exists(filter).get());

        verify(mockCollExecutor).exists(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testExistsWithObjectIdDelegatesArgumentsAndResult() throws Exception {
        ObjectId objectId = new ObjectId();
        final var expected = false;
        when(mockCollExecutor.exists(objectId)).thenReturn(expected);

        Assertions.assertEquals(expected, asyncExecutor.exists(objectId).get());

        verify(mockCollExecutor).exists(objectId);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testExistsWithStringObjectIdDelegatesArgumentsAndResult() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        final var expected = true;
        when(mockCollExecutor.exists(objectId)).thenReturn(expected);

        Assertions.assertEquals(expected, asyncExecutor.exists(objectId).get());

        verify(mockCollExecutor).exists(objectId);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindFirstFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        final var expected = Optional.of(new Document());
        when(mockCollExecutor.findFirst(filter)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findFirst(filter).get());

        verify(mockCollExecutor).findFirst(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindFirstFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        TestEntity te = new TestEntity();
        final var expected = Optional.of(te);
        when(mockCollExecutor.findFirst(filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findFirst(filter, TestEntity.class).get());

        verify(mockCollExecutor).findFirst(filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindFirstProjFilterSortRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document proj = new Document("a", 1);
        Document filter = new Document("status", "active");
        Document sort = new Document("name", -1);
        TestEntity te = new TestEntity();
        final var expected = Optional.of(te);
        when(mockCollExecutor.findFirst(proj, filter, sort, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findFirst(proj, filter, sort, TestEntity.class).get());

        verify(mockCollExecutor).findFirst(proj, filter, sort, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindFirstSelectFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        List<String> sel = Arrays.asList("a");
        TestEntity te = new TestEntity();
        final var expected = Optional.of(te);
        when(mockCollExecutor.findFirst(sel, filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findFirst(sel, filter, TestEntity.class).get());

        verify(mockCollExecutor).findFirst(sel, filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindFirstSelectFilterSortRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", -1);
        List<String> sel = Arrays.asList("a");
        TestEntity te = new TestEntity();
        final var expected = Optional.of(te);
        when(mockCollExecutor.findFirst(sel, filter, sort, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findFirst(sel, filter, sort, TestEntity.class).get());

        verify(mockCollExecutor).findFirst(sel, filter, sort, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @ParameterizedTest(name = "present={0}")
    @ValueSource(booleans = { true, false })
    public void testGetObjectIdDelegatesArgumentsAndResult(boolean present) throws Exception {
        ObjectId objectId = new ObjectId();
        final var expected = present ? Optional.of(new Document("value", 7)) : Optional.<Document> empty();
        when(mockCollExecutor.get(objectId)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.get(objectId).get());

        verify(mockCollExecutor).get(objectId);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @ParameterizedTest(name = "present={0}")
    @ValueSource(booleans = { true, false })
    public void testGetObjectIdRowTypeDelegatesArgumentsAndResult(boolean present) throws Exception {
        ObjectId objectId = new ObjectId();
        final var expected = present ? Optional.of(new TestEntity()) : Optional.<TestEntity> empty();
        when(mockCollExecutor.get(objectId, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.get(objectId, TestEntity.class).get());

        verify(mockCollExecutor).get(objectId, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @ParameterizedTest(name = "present={0}")
    @ValueSource(booleans = { true, false })
    public void testGetObjectIdSelectPropsRowTypeDelegatesArgumentsAndResult(boolean present) throws Exception {
        ObjectId objectId = new ObjectId();
        List<String> selectProps = Arrays.asList("name");
        final var expected = present ? Optional.of(new TestEntity()) : Optional.<TestEntity> empty();
        when(mockCollExecutor.get(objectId, selectProps, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.get(objectId, selectProps, TestEntity.class).get());

        verify(mockCollExecutor).get(objectId, selectProps, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @ParameterizedTest(name = "present={0}")
    @ValueSource(booleans = { true, false })
    public void testGetStringObjectIdDelegatesArgumentsAndResult(boolean present) throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        final var expected = present ? Optional.of(new Document("value", 7)) : Optional.<Document> empty();
        when(mockCollExecutor.get(objectId)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.get(objectId).get());

        verify(mockCollExecutor).get(objectId);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @ParameterizedTest(name = "present={0}")
    @ValueSource(booleans = { true, false })
    public void testGetStringObjectIdRowTypeDelegatesArgumentsAndResult(boolean present) throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        final var expected = present ? Optional.of(new TestEntity()) : Optional.<TestEntity> empty();
        when(mockCollExecutor.get(objectId, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.get(objectId, TestEntity.class).get());

        verify(mockCollExecutor).get(objectId, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @ParameterizedTest(name = "present={0}")
    @ValueSource(booleans = { true, false })
    public void testGetStringObjectIdSelectPropsRowTypeDelegatesArgumentsAndResult(boolean present) throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        List<String> selectProps = Arrays.asList("name");
        final var expected = present ? Optional.of(new TestEntity()) : Optional.<TestEntity> empty();
        when(mockCollExecutor.get(objectId, selectProps, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.get(objectId, selectProps, TestEntity.class).get());

        verify(mockCollExecutor).get(objectId, selectProps, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @ParameterizedTest(name = "present={0}")
    @ValueSource(booleans = { true, false })
    public void testGettObjectIdDelegatesArgumentsAndResult(boolean present) throws Exception {
        ObjectId objectId = new ObjectId();
        final Document expected = present ? new Document("value", 7) : null;
        when(mockCollExecutor.gett(objectId)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.gett(objectId).get());

        verify(mockCollExecutor).gett(objectId);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testGettObjectIdRowTypeDelegatesArgumentsAndResult() throws Exception {
        ObjectId objectId = new ObjectId();
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.gett(objectId, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.gett(objectId, TestEntity.class).get());

        verify(mockCollExecutor).gett(objectId, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testGettObjectIdSelectPropsRowTypeDelegatesArgumentsAndResult() throws Exception {
        ObjectId objectId = new ObjectId();
        List<String> selectProps = Arrays.asList("name");
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.gett(objectId, selectProps, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.gett(objectId, selectProps, TestEntity.class).get());

        verify(mockCollExecutor).gett(objectId, selectProps, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @ParameterizedTest(name = "present={0}")
    @ValueSource(booleans = { true, false })
    public void testGettStringObjectIdDelegatesArgumentsAndResult(boolean present) throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        final Document expected = present ? new Document("value", 7) : null;
        when(mockCollExecutor.gett(objectId)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.gett(objectId).get());

        verify(mockCollExecutor).gett(objectId);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testGettStringObjectIdRowTypeDelegatesArgumentsAndResult() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.gett(objectId, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.gett(objectId, TestEntity.class).get());

        verify(mockCollExecutor).gett(objectId, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testGettStringObjectIdSelectPropsRowTypeDelegatesArgumentsAndResult() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        List<String> selectProps = Arrays.asList("name");
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.gett(objectId, selectProps, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.gett(objectId, selectProps, TestEntity.class).get());

        verify(mockCollExecutor).gett(objectId, selectProps, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("a", 1));
        final var expected = docs;
        when(mockCollExecutor.list(filter)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(filter).get());

        verify(mockCollExecutor).list(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListFilterOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        List<TestEntity> es = Arrays.asList(new TestEntity());
        final var expected = es;
        when(mockCollExecutor.list(filter, 0, 10, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(filter, 0, 10, TestEntity.class).get());

        verify(mockCollExecutor).list(filter, 0, 10, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        List<TestEntity> es = Arrays.asList(new TestEntity());
        final var expected = es;
        when(mockCollExecutor.list(filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(filter, TestEntity.class).get());

        verify(mockCollExecutor).list(filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListProjFilterSortOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document proj = new Document("a", 1);
        Document filter = new Document("status", "active");
        Document sort = new Document("name", -1);
        List<TestEntity> es = Arrays.asList(new TestEntity());
        final var expected = es;
        when(mockCollExecutor.list(proj, filter, sort, 0, 10, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(proj, filter, sort, 0, 10, TestEntity.class).get());

        verify(mockCollExecutor).list(proj, filter, sort, 0, 10, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListProjFilterSortRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document proj = new Document("a", 1);
        Document filter = new Document("status", "active");
        Document sort = new Document("name", -1);
        List<TestEntity> es = Arrays.asList(new TestEntity());
        final var expected = es;
        when(mockCollExecutor.list(proj, filter, sort, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(proj, filter, sort, TestEntity.class).get());

        verify(mockCollExecutor).list(proj, filter, sort, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListSelectFilterOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        List<String> sel = Arrays.asList("a");
        List<TestEntity> es = Arrays.asList(new TestEntity());
        final var expected = es;
        when(mockCollExecutor.list(sel, filter, 0, 10, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(sel, filter, 0, 10, TestEntity.class).get());

        verify(mockCollExecutor).list(sel, filter, 0, 10, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListSelectFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        List<String> sel = Arrays.asList("a");
        List<TestEntity> es = Arrays.asList(new TestEntity());
        final var expected = es;
        when(mockCollExecutor.list(sel, filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(sel, filter, TestEntity.class).get());

        verify(mockCollExecutor).list(sel, filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListSelectFilterSortOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", -1);
        List<String> sel = Arrays.asList("a");
        List<TestEntity> es = Arrays.asList(new TestEntity());
        final var expected = es;
        when(mockCollExecutor.list(sel, filter, sort, 0, 10, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(sel, filter, sort, 0, 10, TestEntity.class).get());

        verify(mockCollExecutor).list(sel, filter, sort, 0, 10, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testListSelectFilterSortRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", -1);
        List<String> sel = Arrays.asList("a");
        List<TestEntity> es = Arrays.asList(new TestEntity());
        final var expected = es;
        when(mockCollExecutor.list(sel, filter, sort, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.list(sel, filter, sort, TestEntity.class).get());

        verify(mockCollExecutor).list(sel, filter, sort, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Dataset ds = N.newEmptyDataset(Arrays.asList("a"));
        final var expected = ds;
        when(mockCollExecutor.query(filter)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(filter).get());

        verify(mockCollExecutor).query(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryFilterOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Dataset ds = N.newEmptyDataset(Arrays.asList("a"));
        final var expected = ds;
        when(mockCollExecutor.query(filter, 0, 10, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(filter, 0, 10, TestEntity.class).get());

        verify(mockCollExecutor).query(filter, 0, 10, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Dataset ds = N.newEmptyDataset(Arrays.asList("a"));
        final var expected = ds;
        when(mockCollExecutor.query(filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(filter, TestEntity.class).get());

        verify(mockCollExecutor).query(filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryWithProjFilterSortOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document proj = new Document("name", 1);
        Document filter = new Document("k", "v");
        Document sort = new Document("name", 1);
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        final var expected = dataset;
        when(mockCollExecutor.query(proj, filter, sort, 0, 5, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(proj, filter, sort, 0, 5, TestEntity.class).get());

        verify(mockCollExecutor).query(proj, filter, sort, 0, 5, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryWithProjFilterSortRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document proj = new Document("name", 1);
        Document filter = new Document("k", "v");
        Document sort = new Document("name", 1);
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        final var expected = dataset;
        when(mockCollExecutor.query(proj, filter, sort, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(proj, filter, sort, TestEntity.class).get());

        verify(mockCollExecutor).query(proj, filter, sort, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryWithSelectFilterOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        final var expected = dataset;
        when(mockCollExecutor.query(select, filter, 0, 5, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(select, filter, 0, 5, TestEntity.class).get());

        verify(mockCollExecutor).query(select, filter, 0, 5, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryWithSelectFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        final var expected = dataset;
        when(mockCollExecutor.query(select, filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(select, filter, TestEntity.class).get());

        verify(mockCollExecutor).query(select, filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryWithSelectFilterSortOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        final var expected = dataset;
        when(mockCollExecutor.query(select, filter, sort, 0, 5, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(select, filter, sort, 0, 5, TestEntity.class).get());

        verify(mockCollExecutor).query(select, filter, sort, 0, 5, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryWithSelectFilterSortRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        final var expected = dataset;
        when(mockCollExecutor.query(select, filter, sort, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.query(select, filter, sort, TestEntity.class).get());

        verify(mockCollExecutor).query(select, filter, sort, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryForPrimitivesDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        when(mockCollExecutor.queryForBoolean("p", filter)).thenReturn(OptionalBoolean.of(true));
        when(mockCollExecutor.queryForChar("p", filter)).thenReturn(OptionalChar.of('a'));
        when(mockCollExecutor.queryForByte("p", filter)).thenReturn(OptionalByte.of((byte) 1));
        when(mockCollExecutor.queryForShort("p", filter)).thenReturn(OptionalShort.of((short) 1));
        when(mockCollExecutor.queryForInt("p", filter)).thenReturn(OptionalInt.of(1));
        when(mockCollExecutor.queryForLong("p", filter)).thenReturn(OptionalLong.of(1L));
        when(mockCollExecutor.queryForFloat("p", filter)).thenReturn(OptionalFloat.of(1f));
        when(mockCollExecutor.queryForDouble("p", filter)).thenReturn(OptionalDouble.of(1d));
        when(mockCollExecutor.queryForString("p", filter)).thenReturn(Nullable.of("s"));

        Assertions.assertTrue(asyncExecutor.queryForBoolean("p", filter).get().orElse(false));
        Assertions.assertEquals('a', asyncExecutor.queryForChar("p", filter).get().orElse('z'));
        Assertions.assertEquals((byte) 1, asyncExecutor.queryForByte("p", filter).get().orElse((byte) 0));
        Assertions.assertEquals((short) 1, asyncExecutor.queryForShort("p", filter).get().orElse((short) 0));
        Assertions.assertEquals(1, asyncExecutor.queryForInt("p", filter).get().orElse(0));
        Assertions.assertEquals(1L, asyncExecutor.queryForLong("p", filter).get().orElse(0L));
        Assertions.assertEquals(1f, asyncExecutor.queryForFloat("p", filter).get().orElse(0f));
        Assertions.assertEquals(1d, asyncExecutor.queryForDouble("p", filter).get().orElse(0d));
        Assertions.assertEquals("s", asyncExecutor.queryForString("p", filter).get().orElse(null));

        verify(mockCollExecutor).queryForBoolean("p", filter);
        verify(mockCollExecutor).queryForChar("p", filter);
        verify(mockCollExecutor).queryForByte("p", filter);
        verify(mockCollExecutor).queryForShort("p", filter);
        verify(mockCollExecutor).queryForInt("p", filter);
        verify(mockCollExecutor).queryForLong("p", filter);
        verify(mockCollExecutor).queryForFloat("p", filter);
        verify(mockCollExecutor).queryForDouble("p", filter);
        verify(mockCollExecutor).queryForString("p", filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryForDateDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        Date d = new Date();
        final var expected = Nullable.of(d);
        when(mockCollExecutor.queryForDate("p", filter)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.queryForDate("p", filter).get());

        verify(mockCollExecutor).queryForDate("p", filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryForDateWithValueTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        java.sql.Timestamp ts = java.sql.Timestamp.from(java.time.Instant.parse("2026-01-02T03:04:05Z"));
        final var expected = Nullable.of(ts);
        when(mockCollExecutor.queryForDate("p", filter, java.sql.Timestamp.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.queryForDate("p", filter, java.sql.Timestamp.class).get());

        verify(mockCollExecutor).queryForDate("p", filter, java.sql.Timestamp.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryForSingleNonNullDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        final var expected = Optional.of("hello");
        when(mockCollExecutor.queryForSingleNonNull("p", filter, String.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.queryForSingleNonNull("p", filter, String.class).get());

        verify(mockCollExecutor).queryForSingleNonNull("p", filter, String.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testQueryForSingleValueDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        final var expected = Nullable.of("hello");
        when(mockCollExecutor.queryForSingleValue("p", filter, String.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.queryForSingleValue("p", filter, String.class).get());

        verify(mockCollExecutor).queryForSingleValue("p", filter, String.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }
}
