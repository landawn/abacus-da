/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.stream.Stream;

class AsyncMongoCollectionExecutorLineCoverageTest extends AsyncMongoCollectionExecutorTestSupport {

    @Test
    void mapReduceFutureGetWithTimeoutReturnsTheStream() throws Exception {
        final Stream<Document> rows = Stream.empty();
        when(mockCollExecutor.mapReduce("function() { emit(this._id, 1); }", "function(key, values) { return 1; }", Document.class)).thenReturn(rows);

        @SuppressWarnings("deprecation")
        final ContinuableFuture<Stream<Document>> future = asyncExecutor.mapReduce("function() { emit(this._id, 1); }", "function(key, values) { return 1; }",
                Document.class);

        assertEquals(0L, future.get(1, TimeUnit.SECONDS).count());
    }
}
