package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.util.AsyncExecutor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

class MongoNullValidationTest {

    @Test
    void codecRegistryRejectsNullClassAndRetainsIgnoredNullRegistry() {
        final MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();

        assertArgument("clazz", () -> registry.get(null));
        assertArgument("clazz", () -> registry.get(null, (CodecRegistry) null));
        assertSame(registry.get(String.class), registry.get(String.class, (CodecRegistry) null));
    }

    @Test
    void beanCodecValidatesArgumentsBeforeReadingBeanOrWritingBson() {
        final MongoDBBase.GeneralCodec<ValidationBean> codec = new MongoDBBase.GeneralCodec<>(ValidationBean.class);
        final BsonWriter writer = mock(BsonWriter.class);
        final ValidationBean bean = new ValidationBean();

        assertArgument("writer", () -> codec.encode(null, null, null));
        assertArgument("value", () -> codec.encode(writer, null, null));
        assertArgument("encoderContext", () -> codec.encode(writer, bean, null));
        assertArgument("reader", () -> codec.decode(null, null));

        assertEquals(0, bean.readCount);
        verifyNoInteractions(writer);
    }

    @Test
    void scalarCodecRetainsUnusedNullContexts() {
        final MongoDBBase.GeneralCodec<String> codec = new MongoDBBase.GeneralCodec<>(String.class);
        final BsonDocument document = new BsonDocument();
        final BsonDocumentWriter writer = new BsonDocumentWriter(document);
        writer.writeStartDocument();
        writer.writeName("value");
        codec.encode(writer, "test", null);
        writer.writeEndDocument();

        final BsonDocumentReader reader = new BsonDocumentReader(document);
        reader.readStartDocument();
        reader.readName();
        assertEquals("test", codec.decode(reader, null));
        reader.readEndDocument();
    }

    @Test
    void scalarCodecRetainsWriterDependentNullHandling() {
        final MongoDBBase.GeneralCodec<String> codec = new MongoDBBase.GeneralCodec<>(String.class);
        final BsonWriter writer = mock(BsonWriter.class);
        codec.encode(writer, null, null);
        verify(writer).writeString(null);

        final BsonDocumentWriter strictWriter = new BsonDocumentWriter(new BsonDocument());
        strictWriter.writeStartDocument();
        strictWriter.writeName("value");
        assertThrows(IllegalArgumentException.class, () -> codec.encode(strictWriter, null, null));
    }

    @Test
    void emptyBeanDocumentRetainsUnusedNullDecoderContext() {
        final MongoDBBase.GeneralCodec<ValidationBean> codec = new MongoDBBase.GeneralCodec<>(ValidationBean.class);

        assertNotNull(codec.decode(new BsonDocumentReader(new BsonDocument()), null));
    }

    @Test
    void inferredMapColumnsRejectNullRowsBeforeReadingAnyMap() {
        final Map<String, Object> row = mock(Map.class);

        assertArgument("row", () -> MongoDBBase.extractData(Arrays.asList(row, null)));
        verifyNoInteractions(row);
        assertEquals(0, MongoDBBase.extractData(Arrays.asList(null, null)).size());
        assertEquals(0, MongoDBBase.extractData((java.util.List<?>) null).size());
    }

    @Test
    void selectedNullPropertyIsRejectedOnlyWhenExtractingScalarResult() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final FindIterable<Document> iterable = mock(FindIterable.class, RETURNS_SELF);
        when(collection.find(any(Bson.class))).thenReturn(iterable);
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection, mock(AsyncExecutor.class));
        final Document row = new Document("value", "test");
        when(iterable.first()).thenReturn(row);

        assertArgument("propName", () -> executor.findFirst(Collections.singletonList(null), new Document(), String.class));
        assertSame(row, executor.findFirst(Collections.singletonList(null), new Document(), Document.class).get());

        when(iterable.first()).thenReturn(null);
        assertTrue(executor.findFirst(Collections.singletonList(null), new Document(), String.class).isEmpty());
    }

    private static void assertArgument(final String parameter, final Executable action) {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action);
        assertTrue(exception.getMessage().contains(parameter), exception.getMessage());
    }

    public static class ValidationBean {
        private int readCount;
        private String value;

        public String getValue() {
            readCount++;
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }
}
