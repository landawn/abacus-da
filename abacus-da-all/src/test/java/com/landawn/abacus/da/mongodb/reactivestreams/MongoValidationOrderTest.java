package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Arrays;
import java.util.Collection;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

class MongoValidationOrderTest {

    @Test
    void validatesEmptyCollectionNameBeforeResultType() {
        final MongoDB database = new MongoDB(mock(MongoDatabase.class));

        assertArgument("collectionName", () -> database.collection("", null));
        assertArgument("collectionName", () -> database.collectionMapper("", null));
    }

    @Test
    void validatesAllBatchElementsBeforeConvertingBeansOrCreatingPublisher() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection);
        final ValidationBean bean = new ValidationBean();

        assertThrows(IllegalArgumentException.class, () -> executor.insertMany(Arrays.asList(bean, null)));
        assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(Arrays.asList(bean, null)));
        assertThrows(IllegalArgumentException.class, () -> executor.updateMany(new Document(), Arrays.asList(bean, null)));
        assertThrows(IllegalArgumentException.class,
                () -> executor.bulkWrite(Arrays.asList(new InsertOneModel<>(new Document()), null)));

        assertEquals(0, bean.readCount);
        verifyNoInteractions(collection);
    }

    @Test
    void validatesReadArgumentsBeforeCreatingPublisher() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection);

        assertArgument("filter", () -> executor.list((Bson) null, -1, -1, null));
        assertArgument("offset", () -> executor.list(new Document(), -1, -1, null));
        assertArgument("count", () -> executor.list(new Document(), 0, -1, null));
        assertArgument("rowType", () -> executor.list(new Document(), 0, 0, null));
        verifyNoInteractions(collection);
    }

    @Test
    void validatesWritePayloadBeforeResultType() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection);

        assertArgument("filter", () -> executor.findOneAndDelete(null, (Class<?>) null));
        assertArgument("update", () -> executor.findOneAndUpdate(new Document(), (Object) null, (Class<?>) null));
        assertArgument("objList", () -> executor.findOneAndUpdate(new Document(), (Collection<?>) null, (Class<?>) null));
        assertArgument("replacement", () -> executor.findOneAndReplace(new Document(), null, (Class<?>) null));
        verifyNoInteractions(collection);
    }

    private static void assertArgument(final String parameter, final org.junit.jupiter.api.function.Executable action) {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action);
        assertTrue(exception.getMessage().contains(parameter), exception.getMessage());
    }

    public static class ValidationBean {
        private int readCount;

        public String getName() {
            readCount++;
            return "name";
        }

        public void setName(final String name) {
            // Bean setter required for the document conversion path.
        }
    }
}
