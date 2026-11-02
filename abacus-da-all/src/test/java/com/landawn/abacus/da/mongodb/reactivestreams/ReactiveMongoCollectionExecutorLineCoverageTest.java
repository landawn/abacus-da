/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.function.Function;

class ReactiveMongoCollectionExecutorLineCoverageTest extends MockitoTestBase {

    @Test
    void nullDocumentMappersReturnNull() throws Exception {
        final Method selected = MongoCollectionExecutor.class.getDeclaredMethod("toEntity", Class.class, Collection.class);
        final Method plain = MongoCollectionExecutor.class.getDeclaredMethod("toEntity", Class.class);
        selected.setAccessible(true);
        plain.setAccessible(true);

        @SuppressWarnings("unchecked")
        final Function<Document, String> selectedMapper = (Function<Document, String>) selected.invoke(null, String.class, List.of("name"));
        @SuppressWarnings("unchecked")
        final Function<Document, String> plainMapper = (Function<Document, String>) plain.invoke(null, String.class);

        assertNull(selectedMapper.apply(null));
        assertNull(plainMapper.apply(null));
        assertNull(plainMapper.apply(new Document()));
    }
}
