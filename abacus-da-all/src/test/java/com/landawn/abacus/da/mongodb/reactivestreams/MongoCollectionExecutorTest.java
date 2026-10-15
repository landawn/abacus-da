package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.CountOptions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MongoCollectionExecutorTest extends MongoCollectionExecutorTestSupport {

    // Confirms executor created via constructor with a non-null collection is usable.
    @Test
    public void testConstructorAcceptsCollection() {
        MongoCollectionExecutor exec = new MongoCollectionExecutor(mockCollection);
        assertSame(mockCollection, exec.coll());
    }

    @Test
    public void testReactiveDriverFactoriesRejectNullArgumentsSynchronously() {
        assertThrows(IllegalArgumentException.class, () -> executor.watch((Class<Document>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.watch((List<? extends Bson>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.watch(List.of(), null));
        assertThrows(IllegalArgumentException.class, () -> executor.distinct("category", (Class<String>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.aggregate(null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.mapReduce("", "function(k, v) { return 1; }", Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.mapReduce("function() { emit(1, 1); }", "", Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query(Arrays.asList("name"), new Document(), null, 0, 1, null));

        verify(mockCollection, org.mockito.Mockito.never()).aggregate(anyList(), eq(Document.class));
        verify(mockCollection, org.mockito.Mockito.never()).find(any(Bson.class));
    }

    @Test
    public void testReadMethodsRejectNullFilterWithIAE() {
        // Regression: findFirst/list/query taking a Bson filter must reject a null filter eagerly with
        // IllegalArgumentException at the call site (before a Mono/Flux is built). Previously a null filter
        // was silently treated as "match all". The driver must NOT be invoked.
        final Collection<String> selectPropNames = Arrays.asList("name");
        final Bson sort = new Document("name", 1);
        final Bson projection = new Document("name", 1);

        // findFirst family
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst((Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst(selectPropNames, (Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst(selectPropNames, (Bson) null, sort, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst(projection, (Bson) null, sort, Document.class));

        // list family
        assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null, 0, 10, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list(selectPropNames, (Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list(selectPropNames, (Bson) null, sort, 0, 10, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list(projection, (Bson) null, sort, 0, 10, Document.class));

        // query family
        assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null, 0, 10, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query(selectPropNames, (Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query(selectPropNames, (Bson) null, sort, 0, 10, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query(projection, (Bson) null, sort, 0, 10, Document.class));

        // count / exists / queryForSingleValue / distinct take a Bson filter too and must reject null consistently.
        assertThrows(IllegalArgumentException.class, () -> executor.count((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.count((Bson) null, new CountOptions()));
        assertThrows(IllegalArgumentException.class, () -> executor.exists((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue("name", (Bson) null, String.class));
        assertThrows(IllegalArgumentException.class, () -> executor.distinct("name", (Bson) null, String.class));

        // The eager guard short-circuits before the driver is touched.
        verify(mockCollection, org.mockito.Mockito.never()).find(any(Bson.class));
        verify(mockCollection, org.mockito.Mockito.never()).countDocuments(any(Bson.class));
    }

}
