package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;

import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.BasicDBObject;

class MongoDBBaseDeepReview2026IdentifierOrderTest {

    @Test
    void idNormalizationPreservesKeyOrderInEveryBsonOutputType() {
        final ObjectId expectedId = new ObjectId("507f1f77bcf86cd799439011");
        final LinkedHashMap<String, Object> source = new LinkedHashMap<>();
        source.put(MongoDBBase._ID, expectedId.toHexString());
        source.put("secondary", 2);
        source.put("tertiary", 3);
        final List<String> expectedKeys = List.of(MongoDBBase._ID, "secondary", "tertiary");

        final Document document = MongoDBBase.toDocument(source);
        final BasicBSONObject bsonObject = MongoDBBase.toBSONObject(source);
        final BasicDBObject dbObject = MongoDBBase.toDBObject(source);

        assertEquals(expectedKeys, List.copyOf(document.keySet()));
        assertEquals(expectedKeys, List.copyOf(bsonObject.keySet()));
        assertEquals(expectedKeys, List.copyOf(dbObject.keySet()));
        assertEquals(expectedId, document.get(MongoDBBase._ID));
        assertEquals(expectedId, bsonObject.get(MongoDBBase._ID));
        assertEquals(expectedId, dbObject.get(MongoDBBase._ID));
    }
}
