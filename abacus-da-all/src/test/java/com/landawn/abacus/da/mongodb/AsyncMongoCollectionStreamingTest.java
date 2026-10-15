package com.landawn.abacus.da.mongodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.stream.Stream;
import com.mongodb.client.ChangeStreamIterable;

public class AsyncMongoCollectionStreamingTest extends AsyncMongoCollectionExecutorTestSupport {

    @Test
    public void testAggregateDelegatesArgumentsAndResult() throws Exception {
        List<Document> pipeline = Arrays.asList(new Document());
        final var expected = Stream.of(List.of(new Document("value", 7)));
        when(mockCollExecutor.aggregate(pipeline)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.aggregate(pipeline).get());

        verify(mockCollExecutor).aggregate(pipeline);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testAggregateRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<Document> pipeline = Arrays.asList(new Document());
        final var expected = Stream.of(new TestEntity());
        when(mockCollExecutor.aggregate(pipeline, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.aggregate(pipeline, TestEntity.class).get());

        verify(mockCollExecutor).aggregate(pipeline, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testDistinctDelegatesArgumentsAndResult() throws Exception {
        Stream<String> s = Stream.of("a", "b");
        final var expected = s;
        when(mockCollExecutor.distinct("f", String.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.distinct("f", String.class).get());

        verify(mockCollExecutor).distinct("f", String.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testDistinctWithFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        final var expected = Stream.of("north");
        when(mockCollExecutor.distinct("f", filter, String.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.distinct("f", filter, String.class).get());

        verify(mockCollExecutor).distinct("f", filter, String.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testGroupByDelegatesArgumentsAndResult() throws Exception {

        final var expected = Stream.of(List.of(new Document("value", 7)));
        when(mockCollExecutor.groupBy("f")).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.groupBy("f").get());

        verify(mockCollExecutor).groupBy("f");
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testGroupByFieldsDelegatesArgumentsAndResult() throws Exception {
        List<String> fs = Arrays.asList("a", "b");
        final var expected = Stream.of(List.of(new Document("value", 7)));
        when(mockCollExecutor.groupBy(fs)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.groupBy(fs).get());

        verify(mockCollExecutor).groupBy(fs);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testGroupByAndCountDelegatesArgumentsAndResult() throws Exception {

        final var expected = Stream.of(List.of(new Document("value", 7)));
        when(mockCollExecutor.groupByAndCount("f")).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.groupByAndCount("f").get());

        verify(mockCollExecutor).groupByAndCount("f");
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testGroupByAndCountFieldsDelegatesArgumentsAndResult() throws Exception {
        List<String> fs = Arrays.asList("a", "b");
        final var expected = Stream.of(List.of(new Document("value", 7)));
        when(mockCollExecutor.groupByAndCount(fs)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.groupByAndCount(fs).get());

        verify(mockCollExecutor).groupByAndCount(fs);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testMapReduceDelegatesArgumentsAndResult() throws Exception {

        final var expected = Stream.of(List.of(new Document("value", 7)));
        when(mockCollExecutor.mapReduce("m", "r")).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.mapReduce("m", "r").get());

        verify(mockCollExecutor).mapReduce("m", "r");
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testMapReduceRowTypeDelegatesArgumentsAndResult() throws Exception {

        final var expected = Stream.of(new TestEntity());
        when(mockCollExecutor.mapReduce("m", "r", TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.mapReduce("m", "r", TestEntity.class).get());

        verify(mockCollExecutor).mapReduce("m", "r", TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        final var expected = Stream.of(List.of(new Document("value", 7)));
        when(mockCollExecutor.stream(filter)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(filter).get());

        verify(mockCollExecutor).stream(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamFilterOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        final var expected = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(filter, 0, 10, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(filter, 0, 10, TestEntity.class).get());

        verify(mockCollExecutor).stream(filter, 0, 10, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        final var expected = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(filter, TestEntity.class).get());

        verify(mockCollExecutor).stream(filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamWithProjFilterSortOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document proj = new Document("k", 1);
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        final var expected = stream;
        when(mockCollExecutor.stream(proj, filter, sort, 0, 5, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(proj, filter, sort, 0, 5, TestEntity.class).get());

        verify(mockCollExecutor).stream(proj, filter, sort, 0, 5, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamWithProjFilterSortRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document proj = new Document("k", 1);
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        final var expected = stream;
        when(mockCollExecutor.stream(proj, filter, sort, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(proj, filter, sort, TestEntity.class).get());

        verify(mockCollExecutor).stream(proj, filter, sort, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamWithSelectFilterOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        final var expected = stream;
        when(mockCollExecutor.stream(select, filter, 0, 5, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(select, filter, 0, 5, TestEntity.class).get());

        verify(mockCollExecutor).stream(select, filter, 0, 5, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamWithSelectFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        final var expected = stream;
        when(mockCollExecutor.stream(select, filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(select, filter, TestEntity.class).get());

        verify(mockCollExecutor).stream(select, filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamWithSelectFilterSortOffsetCountRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        final var expected = stream;
        when(mockCollExecutor.stream(select, filter, sort, 0, 5, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(select, filter, sort, 0, 5, TestEntity.class).get());

        verify(mockCollExecutor).stream(select, filter, sort, 0, 5, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testStreamWithSelectFilterSortRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        final var expected = stream;
        when(mockCollExecutor.stream(select, filter, sort, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.stream(select, filter, sort, TestEntity.class).get());

        verify(mockCollExecutor).stream(select, filter, sort, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testWatchDelegatesArgumentsAndResult() throws Exception {
        ChangeStreamIterable<Document> csi = mock(ChangeStreamIterable.class);
        final var expected = csi;
        when(mockCollExecutor.watch()).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.watch().get());

        verify(mockCollExecutor).watch();
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testWatchPipelineDelegatesArgumentsAndResult() throws Exception {
        List<Document> pipeline = Arrays.asList(new Document());
        ChangeStreamIterable<Document> csi = mock(ChangeStreamIterable.class);
        final var expected = csi;
        when(mockCollExecutor.watch(pipeline)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.watch(pipeline).get());

        verify(mockCollExecutor).watch(pipeline);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testWatchPipelineRowTypeDelegatesArgumentsAndResult() throws Exception {
        List<Document> pipeline = Arrays.asList(new Document());
        ChangeStreamIterable<TestEntity> csi = mock(ChangeStreamIterable.class);
        final var expected = csi;
        when(mockCollExecutor.watch(pipeline, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.watch(pipeline, TestEntity.class).get());

        verify(mockCollExecutor).watch(pipeline, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testWatchRowTypeDelegatesArgumentsAndResult() throws Exception {
        ChangeStreamIterable<TestEntity> csi = mock(ChangeStreamIterable.class);
        final var expected = csi;
        when(mockCollExecutor.watch(TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.watch(TestEntity.class).get());

        verify(mockCollExecutor).watch(TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }
}
