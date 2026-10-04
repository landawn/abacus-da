package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import com.landawn.abacus.da.TestBase;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoLiveTestSupportTest extends TestBase {

    @Test
    public void unavailableMongoAbortsPromptly() throws Exception {
        // Reserve a local port without answering MongoDB requests; no external service is needed.
        try (ServerSocket unavailable = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            assertTimeout(Duration.ofSeconds(5), () -> assertThrows(TestAbortedException.class, () -> {
                try (MongoClient ignored = MongoLiveTestSupport.documentsClient("mongodb://127.0.0.1:" + unavailable.getLocalPort())) {
                    // A successful connection would fail assertThrows and still close the client.
                }
            }));
        }
    }

    @Test
    public void unsupportedDocumentsStageAborts() {
        final MongoCommandException error = commandError(40324, "Unrecognized pipeline stage name: '$documents'");

        assertThrows(TestAbortedException.class, () -> MongoLiveTestSupport.requireDocumentsStage(failingClient(error)));
    }

    @Test
    public void unrelatedCommandErrorsRemainFailures() {
        for (final MongoCommandException error : new MongoCommandException[] { commandError(13, "not authorized to execute $documents"),
                commandError(40324, "Unrecognized pipeline stage name: '$unexpected'") }) {
            assertSame(error, assertThrows(MongoCommandException.class, () -> MongoLiveTestSupport.requireDocumentsStage(failingClient(error))));
        }
    }

    private static MongoClient failingClient(final MongoCommandException error) {
        final MongoClient client = mock(MongoClient.class);
        final MongoDatabase database = mock(MongoDatabase.class);
        when(client.getDatabase("test")).thenReturn(database);
        when(database.aggregate(anyList())).thenThrow(error);
        return client;
    }

    private static MongoCommandException commandError(final int code, final String message) {
        return new MongoCommandException(new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(code))
                .append("errmsg", new BsonString(message)), new ServerAddress("localhost"));
    }
}
