package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.quality.Strictness;
import org.reactivestreams.Subscriber;

import com.landawn.abacus.da.TestBase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Pins the single-value row type contract documented on the reactive mapper's projection reads (2026-10-24 sliceV):
 * with exactly one selected field the field's value is emitted and a missing or {@code null} value is not emitted
 * (the {@code Mono} completes empty), while a {@link Bson} projection converts the sole non-{@code _id} field and falls
 * back to the {@code _id} of a document that lacks the projected field. The mocked collection returns the documents the
 * server would send for each projection.
 */
public class ReactiveMongoMapperDeepReview2026SingleValueProjectionTest extends TestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private static final ObjectId ID1 = new ObjectId("507f1f77bcf86cd799439011");
    private static final ObjectId ID2 = new ObjectId("507f1f77bcf86cd799439012");
    private static final ObjectId ID3 = new ObjectId("507f1f77bcf86cd799439013");

    @SuppressWarnings("unchecked")
    private static MongoCollectionMapper<String> stringMapperReturning(final Document... rows) {
        final org.mockito.MockSettings lenient = Mockito.withSettings().strictness(Strictness.LENIENT);
        final MongoCollection<Document> collection = mock(MongoCollection.class, lenient);
        final FindPublisher<Document> findPublisher = mock(FindPublisher.class, lenient);

        when(collection.find(any(Bson.class))).thenReturn(findPublisher);
        when(findPublisher.projection(any())).thenReturn(findPublisher);
        when(findPublisher.sort(any())).thenReturn(findPublisher);
        when(findPublisher.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        when(findPublisher.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        doAnswer(invocation -> {
            Flux.fromArray(rows).subscribe((Subscriber<? super Document>) invocation.getArgument(0));
            return null;
        }).when(findPublisher).subscribe(any());

        return new MongoCollectionMapper<>(new MongoCollectionExecutor(collection), String.class);
    }

    @Test
    public void testSelectedFieldReadsSkipMissingAndNullValues() {
        final Document present = new Document("_id", ID1).append("name", "a");
        final Document nullValue = new Document("_id", ID2).append("name", null);
        final Document missing = new Document("_id", ID3);
        final List<String> name = List.of("name");

        assertAll(() -> assertEquals(List.of("a"), stringMapperReturning(present, nullValue, missing).list(name, Filters.empty()).collectList().block(TIMEOUT)),
                () -> assertEquals(List.of("X"),
                        stringMapperReturning(new Document("_id", ID1).append("address", new Document("city", "X")))
                                .list(List.of("address.city"), Filters.empty())
                                .collectList()
                                .block(TIMEOUT)),
                () -> assertEquals(List.of("a"),
                        stringMapperReturning(present, nullValue, missing).list(name, Filters.empty(), null, 0, 10).collectList().block(TIMEOUT)),
                () -> assertFalse(stringMapperReturning(missing).findFirst(name, Filters.empty()).blockOptional(TIMEOUT).isPresent(),
                        "a matched document without the selected value completes empty"),
                () -> assertFalse(stringMapperReturning(nullValue).findFirst(name, Filters.empty(), null).blockOptional(TIMEOUT).isPresent()),
                () -> assertFalse(stringMapperReturning(nullValue).get(ID2, name).blockOptional(TIMEOUT).isPresent()),
                () -> assertFalse(stringMapperReturning(missing).get(ID3.toHexString(), name).blockOptional(TIMEOUT).isPresent()));
    }

    @Test
    public void testBsonProjectionReadsFallBackToIdForDocumentsLackingTheField() {
        final Bson projection = Projections.include("name");
        final Document present = new Document("_id", ID1).append("name", "a");
        final Document nullValue = new Document("_id", ID2).append("name", null);
        final Document idOnly = new Document("_id", ID3);

        assertAll(
                () -> assertEquals(Arrays.asList("a", ID3.toHexString()),
                        stringMapperReturning(present, nullValue, idOnly).list(projection, Filters.empty(), null).collectList().block(TIMEOUT)),
                () -> assertEquals(Arrays.asList("a", ID3.toHexString()),
                        stringMapperReturning(present, nullValue, idOnly).list(projection, Filters.empty(), null, 0, 10).collectList().block(TIMEOUT)),
                () -> assertEquals(Optional.of(ID3.toHexString()),
                        stringMapperReturning(idOnly).findFirst(projection, Filters.empty(), null).blockOptional(TIMEOUT)),
                () -> assertFalse(stringMapperReturning(nullValue).findFirst(projection, Filters.empty(), null).blockOptional(TIMEOUT).isPresent(),
                        "a null projected value completes empty"),
                // With _id excluded, a document lacking the field arrives empty and is skipped.
                () -> assertEquals(List.of("a"),
                        stringMapperReturning(new Document("name", "a"), new Document("name", null), new Document())
                                .list(Projections.fields(projection, Projections.excludeId()), Filters.empty(), null)
                                .collectList()
                                .block(TIMEOUT)));
    }

    @Test
    public void testQueryForSingleValueCompletesEmptyForTextThatConvertsToNull() {
        final Document emptyText = new Document("_id", ID1).append("n", "");

        assertAll(
                () -> assertFalse(stringMapperReturning(emptyText).queryForSingleValue("n", Filters.empty(), Integer.class).blockOptional(TIMEOUT).isPresent(),
                        "empty text read as Integer converts to null, so the Mono completes empty"),
                () -> assertFalse(stringMapperReturning(emptyText).queryForInt("n", Filters.empty()).blockOptional(TIMEOUT).isPresent()),
                () -> assertEquals(Optional.of(0), stringMapperReturning(emptyText).queryForSingleValue("n", Filters.empty(), int.class).blockOptional(TIMEOUT),
                        "a primitive value type emits its default instead"),
                () -> assertEquals(Optional.of(""), stringMapperReturning(emptyText).queryForString("n", Filters.empty()).blockOptional(TIMEOUT)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiFieldGroupByIntoSingleValueTypeSignalsIllegalArgumentException() {
        final org.mockito.MockSettings lenient = Mockito.withSettings().strictness(Strictness.LENIENT);
        final MongoCollection<Document> collection = mock(MongoCollection.class, lenient);
        final AggregatePublisher<Document> aggregatePublisher = mock(AggregatePublisher.class, lenient);
        when(collection.aggregate(anyList(), eq(Document.class))).thenReturn(aggregatePublisher);
        // The $project stage of a two-field group yields one value per field.
        doAnswer(invocation -> {
            Flux.just(new Document("f", "a").append("g", "x")).subscribe((Subscriber<? super Document>) invocation.getArgument(0));
            return null;
        }).when(aggregatePublisher).subscribe(any());

        final MongoCollectionMapper<String> mapper = new MongoCollectionMapper<>(new MongoCollectionExecutor(collection), String.class);
        // Building the publisher does not fail; the conversion failure is signalled on consumption.
        final Flux<String> groups = mapper.groupBy(List.of("f", "g"));

        StepVerifier.create(groups)
                .expectErrorSatisfies(e -> assertAll(() -> assertEquals(IllegalArgumentException.class, e.getClass()),
                        () -> assertTrue(e.getMessage().startsWith("Cannot convert document with multiple non-_id fields to single-value type"),
                                e.getMessage())))
                .verify(TIMEOUT);
    }

    // ---- 2026-10-24 verify5 ----

    @SuppressWarnings("unchecked")
    private static <T> MongoCollectionMapper<T> mapperReturning(final Class<T> rowType, final Document... rows) {
        final org.mockito.MockSettings lenient = Mockito.withSettings().strictness(Strictness.LENIENT);
        final MongoCollection<Document> collection = mock(MongoCollection.class, lenient);
        final FindPublisher<Document> findPublisher = mock(FindPublisher.class, lenient);

        when(collection.find(any(Bson.class))).thenReturn(findPublisher);
        when(findPublisher.projection(any())).thenReturn(findPublisher);
        when(findPublisher.sort(any())).thenReturn(findPublisher);
        when(findPublisher.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        when(findPublisher.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        doAnswer(invocation -> {
            Flux.fromArray(rows).subscribe((Subscriber<? super Document>) invocation.getArgument(0));
            return null;
        }).when(findPublisher).subscribe(any());

        return new MongoCollectionMapper<>(new MongoCollectionExecutor(collection), rowType);
    }

    @Test
    public void testBsonProjectionNullValueEmitsPrimitiveDefaultForPrimitiveMapper() {
        // A Bson-projection read converts the sole non-_id field through readRow, which turns a null value into the
        // primitive default: a primitive mapper type emits 0 where a wrapper type skips the document. The selected-field
        // (Collection) reads skip the null value for both.
        final Bson projection = Projections.include("n");
        final Document one = new Document("_id", ID1).append("n", 1);
        final Document nullValue = new Document("_id", ID2).append("n", null);

        assertAll(
                () -> assertEquals(List.of(1, 0),
                        mapperReturning(int.class, one, nullValue).list(projection, Filters.empty(), null).collectList().block(TIMEOUT)),
                () -> assertEquals(List.of(1, 0),
                        mapperReturning(int.class, one, nullValue).list(projection, Filters.empty(), null, 0, 10).collectList().block(TIMEOUT)),
                () -> assertEquals(Optional.of(0), mapperReturning(int.class, nullValue).findFirst(projection, Filters.empty(), null).blockOptional(TIMEOUT)),
                () -> assertEquals(List.of(1),
                        mapperReturning(Integer.class, one, nullValue).list(projection, Filters.empty(), null).collectList().block(TIMEOUT)),
                () -> assertFalse(mapperReturning(Integer.class, nullValue).findFirst(projection, Filters.empty(), null).blockOptional(TIMEOUT).isPresent()),
                () -> assertEquals(List.of(1), mapperReturning(int.class, one, nullValue).list(List.of("n"), Filters.empty()).collectList().block(TIMEOUT)),
                () -> assertFalse(mapperReturning(int.class, nullValue).findFirst(List.of("n"), Filters.empty()).blockOptional(TIMEOUT).isPresent()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSiblingDottedGroupFieldsIntoSingleValueTypeConvertTheNestedDocument() {
        final org.mockito.MockSettings lenient = Mockito.withSettings().strictness(Strictness.LENIENT);
        final MongoCollection<Document> collection = mock(MongoCollection.class, lenient);
        final AggregatePublisher<Document> aggregatePublisher = mock(AggregatePublisher.class, lenient);
        when(collection.aggregate(anyList(), eq(Document.class))).thenReturn(aggregatePublisher);
        // The $project stage of "a.b" + "a.c" nests both keys under the one top-level field "a".
        doAnswer(invocation -> {
            Flux.just(new Document("a", new Document("b", "x").append("c", "y"))).subscribe((Subscriber<? super Document>) invocation.getArgument(0));
            return null;
        }).when(aggregatePublisher).subscribe(any());

        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection);
        final List<String> mapperRows = new MongoCollectionMapper<>(executor, String.class).groupBy(List.of("a.b", "a.c")).collectList().block(TIMEOUT);
        final List<String> executorRows = executor.groupBy(List.of("a.b", "a.c"), String.class).collectList().block(TIMEOUT);

        assertAll(() -> assertEquals(1, mapperRows.size()),
                () -> assertTrue(mapperRows.get(0).contains("\"x\"") && mapperRows.get(0).contains("\"y\""), mapperRows.get(0)),
                () -> assertEquals(mapperRows, executorRows));
    }
}
