package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/** Prerequisite checks for read-only, database-level aggregation tests. */
public final class MongoLiveTestSupport {
    private MongoLiveTestSupport() {
    }

    public static MongoClient documentsClient() {
        return documentsClient("mongodb://localhost:27017");
    }

    static MongoClient documentsClient(final String connectionString) {
        final MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(builder -> builder.serverSelectionTimeout(1, TimeUnit.SECONDS))
                .applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.SECONDS).readTimeout(1, TimeUnit.SECONDS))
                .timeout(2, TimeUnit.SECONDS).build();
        final MongoClient client = MongoClients.create(settings);
        try {
            requireDocumentsStage(client);
            return client;
        } catch (final RuntimeException | Error e) {
            client.close();
            throw e;
        }
    }

    static void requireDocumentsStage(final MongoClient client) {
        try {
            // Probe the actual feature, since server versions and compatible implementations may differ.
            client.getDatabase("test").aggregate(List.of(new Document("$documents", List.of(new Document("probe", 1))))).first();
        } catch (final MongoTimeoutException | MongoSocketException e) {
            assumeTrue(false, "Live MongoDB is unavailable: " + e.getMessage());
        } catch (final MongoCommandException e) {
            if (e.getErrorCode() != 40324 || !e.getErrorMessage().contains("$documents")) {
                throw e;
            }
            assumeTrue(false, "Live MongoDB does not support the $documents stage: " + e.getErrorMessage());
        }
    }
}
