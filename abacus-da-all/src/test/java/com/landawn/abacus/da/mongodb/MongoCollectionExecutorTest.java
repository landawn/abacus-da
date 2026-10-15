package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.Dataset;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;

public class MongoCollectionExecutorTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testAsync() {
        AsyncMongoCollectionExecutor asyncExecutor = executor.async();
        Assertions.assertSame(asyncExecutor, executor.async());
        Assertions.assertSame(executor, asyncExecutor.sync());
    }

    @Test
    public void testColl() {
        MongoCollection<Document> result = executor.coll();
        Assertions.assertSame(mockCollection, result);
    }

    @Test
    public void testDriverFactoriesRejectNullArgumentsBeforeOpeningPublishersOrCursors() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.watch((Class<Document>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.watch((List<? extends Bson>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.watch(List.of(), null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.distinct("category", (Class<String>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.aggregate(null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.mapReduce("", "function(k, v) { return 1; }", Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.mapReduce("function() { emit(1, 1); }", "", Document.class));

        verify(mockCollection, never()).watch((Class<Document>) any());
        verify(mockCollection, never()).aggregate(anyList(), eq(Document.class));
    }

    @Test
    public void testReadMethodsRejectNullFilterWithIAE() {
        // Regression: findFirst/list/query taking a Bson filter must reject a null filter with
        // IllegalArgumentException (the documented contract). Previously a null filter was silently
        // treated as "match all" (coll.find() with no filter). The driver must NOT be invoked.
        final Collection<String> selectPropNames = Arrays.asList("name");
        final Bson sort = new Document("name", 1);
        final Bson projection = new Document("name", 1);

        // findFirst family
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findFirst((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findFirst((Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findFirst(selectPropNames, (Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findFirst(selectPropNames, (Bson) null, sort, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findFirst(projection, (Bson) null, sort, Document.class));

        // list family
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null, 0, 10, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(selectPropNames, (Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(selectPropNames, (Bson) null, sort, 0, 10, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(projection, (Bson) null, sort, 0, 10, Document.class));

        // query family
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null, 0, 10, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query(selectPropNames, (Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query(selectPropNames, (Bson) null, sort, 0, 10, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query(projection, (Bson) null, sort, 0, 10, Document.class));

        // stream / queryFor* / count / exists / distinct take a Bson filter too and must reject null consistently.
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.stream((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.stream((Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.stream(selectPropNames, (Bson) null, sort, 0, 10, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue("name", (Bson) null, String.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleNonNull("name", (Bson) null, String.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.count((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.count((Bson) null, new CountOptions()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.exists((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.distinct("name", (Bson) null, String.class));

        // The guard short-circuits before the driver is touched (no find / countDocuments / distinct).
        verify(mockCollection, never()).find();
        verify(mockCollection, never()).find(any(Bson.class));
        verify(mockCollection, never()).countDocuments(any(Bson.class));
        verify(mockCollection, never()).countDocuments(any(Bson.class), any(CountOptions.class));
    }

    @Test
    public void testTypedReadAndModifyMethodsRejectNullRowTypeWithIAE() {
        // Regression: findFirst/get/gett, the Dataset query(..., Class) overloads, the typed
        // findOneAndUpdate/findOneAndReplace/findOneAndDelete overloads, and mapReduce(map, reduce,
        // Class) all document "@throws IllegalArgumentException ... rowType is null" but had no
        // guard — a null rowType silently produced Object[] rows via MongoDBBase.readRow (or an NPE
        // via checkResultClass for the Dataset overloads). The guards now throw eagerly.
        final Collection<String> selectPropNames = Arrays.asList("name");
        final Bson filter = new Document("id", 1);
        final Bson sort = new Document("name", 1);
        final Bson projection = new Document("name", 1);
        final Document update = new Document("$set", new Document("a", 1));

        // findFirst terminals (the get/gett/findFirst typed family funnels through these two).
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findFirst(selectPropNames, filter, sort, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findFirst(projection, filter, sort, null));

        // Dataset query terminals.
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query(selectPropNames, filter, sort, 0, 10, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.query(projection, filter, sort, 0, 10, null));

        // Typed find-and-modify chokepoints (the no-options typed variants delegate into them).
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, update, (FindOneAndUpdateOptions) null, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> executor.findOneAndUpdate(filter, Arrays.asList(update), (FindOneAndUpdateOptions) null, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> executor.findOneAndReplace(filter, new Document("a", 1), (FindOneAndReplaceOptions) null, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete(filter, (FindOneAndDeleteOptions) null, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete(filter, (Class<Document>) null));

        // Typed mapReduce.
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.mapReduce("function(){}", "function(k,v){}", null));
    }
}
