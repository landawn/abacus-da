package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Collection;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.AsyncExecutor;
import com.mongodb.client.MongoCollection;

class MongoValidationOrderTest {

    @Test
    void validatesReadArgumentsBeforeAccessingCollection() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection, mock(AsyncExecutor.class));

        assertArgument("filter", () -> executor.list((Bson) null, -1, -1, null));
        assertArgument("offset", () -> executor.list(new Document(), -1, -1, null));
        assertArgument("count", () -> executor.list(new Document(), 0, -1, null));
        assertArgument("rowType", () -> executor.list(new Document(), 0, 0, null));
        verifyNoInteractions(collection);
    }

    @Test
    void validatesWritePayloadBeforeResultType() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection, mock(AsyncExecutor.class));

        assertArgument("update", () -> executor.findOneAndUpdate(new Document(), (Object) null, (Class<?>) null));
        assertArgument("objList", () -> executor.findOneAndUpdate(new Document(), (Collection<?>) null, (Class<?>) null));
        assertArgument("replacement", () -> executor.findOneAndReplace(new Document(), null, (Class<?>) null));
        verifyNoInteractions(collection);
    }

    @Test
    void validatesAsyncArgumentsBeforeSubmittingTask() {
        final AsyncExecutor backingExecutor = mock(AsyncExecutor.class);
        final AsyncMongoCollectionExecutor executor = new AsyncMongoCollectionExecutor(mock(MongoCollectionExecutor.class), backingExecutor);

        assertArgument("filter", () -> executor.findFirst((Bson) null, null));
        assertArgument("filter", () -> executor.queryForSingleValue("name", null, null));
        assertArgument("update", () -> executor.findOneAndUpdate(new Document(), (Object) null, (Class<?>) null));
        verifyNoInteractions(backingExecutor);
    }

    private static void assertArgument(final String parameter, final org.junit.jupiter.api.function.Executable action) {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action);
        assertTrue(exception.getMessage().contains(parameter), exception.getMessage());
    }
}
