/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Optional;
import com.mongodb.MongoNamespace;

class MongoCollectionExecutorLineCoverageTest extends MongoCollectionExecutorTestSupport {

    @Test
    void findFirstOfANullIterableIsEmpty() {
        when(mockCollection.getNamespace()).thenReturn(new MongoNamespace("demo", "people"));
        when(mockFindIterable.limit(anyInt())).thenReturn(null);

        final Optional<String> found = executor.findFirst((Bson) null, new Document("id", "1"), (Bson) null, String.class);

        assertTrue(found.isEmpty());
    }

    @Test
    void singleFieldStreamMapsANullDocumentToNull() {
        when(mockCollection.getNamespace()).thenReturn(new MongoNamespace("demo", "people"));
        when(mockCursor.hasNext()).thenReturn(true, false);
        when(mockCursor.next()).thenReturn(null);

        final List<String> names;
        try (Stream<String> rows = executor.stream(List.of("name"), new Document("id", "1"), null, String.class)) {
            names = rows.toList();
        }

        assertEquals(Collections.singletonList(null), names);
    }

    @Test
    void executeQueryLogsWhenDebugIsEnabled() {
        final Level previous = DynamoDBExecutorLineCoverageTest.enableDebug(MongoCollectionExecutor.class);
        when(mockCollection.getNamespace()).thenReturn(new MongoNamespace("demo", "people"));
        when(mockFindIterable.first()).thenReturn(new Document("name", "Ada"));

        try {
            assertTrue(LoggerFactory.getLogger(MongoCollectionExecutor.class).isDebugEnabled());
            final Optional<String> found = executor.findFirst((Bson) null, new Document("id", "1"), (Bson) null, String.class);
            assertEquals("Ada", found.orElse(null));
        } finally {
            Logger.getLogger(MongoCollectionExecutor.class.getName()).setLevel(previous);
        }

        verify(mockCollection).find(any(org.bson.conversions.Bson.class));
        verify(mockCollection).getNamespace();
    }
}
