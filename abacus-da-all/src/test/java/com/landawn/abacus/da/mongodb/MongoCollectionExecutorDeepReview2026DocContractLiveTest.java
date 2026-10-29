package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.BsonInvalidOperationException;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndDeleteOptions;

/**
 * Pins, against a live MongoDB (skipped when none is reachable), the exception contracts the 2026-10-27 review documented on
 * {@link MongoCollectionExecutor} (doc-only changes; these pass before and after them):
 * <ul>
 * <li>a null name in {@code selectPropNames} is rejected by the driver for every row type when the query is sent, but not by
 * {@code list} with a {@code count} of 0, which sends no projection; {@code query(Collection, ...)} rejects null and duplicated
 * names in every case (they are Dataset columns);</li>
 * <li>{@code distinct} decodes every value when it is called, so a value its row type cannot decode fails the call itself;</li>
 * <li>{@code aggregate} rejects a null pipeline stage at the call;</li>
 * <li>a typed {@code findOneAndDelete} that cannot convert the deleted document throws after the delete, so a retry deletes
 * another document;</li>
 * <li>field names are stored names: an entity's id property is stored as {@code _id}, so {@code "id"} matches nothing.</li>
 * </ul>
 */
public class MongoCollectionExecutorDeepReview2026DocContractLiveTest {

    public static class Named {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class Identified {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    // ---- 2026-10-27 sliceQ ----

    @Test
    public void documentedCallTimeFailuresAndPostWriteConversionFailure_sliceQ() {
        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            final MongoDatabase db = client.getDatabase("test");
            final String collectionName = "abacus_da_doc_contract_" + UUID.randomUUID().toString().replace("-", "");
            final MongoCollectionExecutor executor = new MongoDB(db).collectionExecutor(collectionName);

            try {
                executor.insertMany(Arrays.asList(new Document("name", "a").append("n", 1).append("k", "del"),
                        new Document("name", "b").append("n", "text").append("k", "del"), new Document("name", "c").append("n", 2.5)));

                final Bson all = Filters.empty();
                final List<String> nullName = Arrays.asList((String) null);
                final List<String> nameAndNull = Arrays.asList("name", null);
                final List<String> duplicated = Arrays.asList("name", "name");

                assertAll(() -> assertThrows(IllegalArgumentException.class, () -> executor.list(nullName, all, Map.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.list(nameAndNull, all, Named.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.findFirst(nullName, Filters.eq("name", "zz"), Map.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.get(new ObjectId(), nullName, Named.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.stream(nullName, all, Map.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.query(nullName, all, Map.class)),
                        () -> assertEquals(new ArrayList<>(), executor.list(nullName, all, 0, 0, Map.class)),
                        () -> assertEquals(0, executor.stream(nullName, all, 0, 0, Map.class).count()),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.query(nullName, all, 0, 0, Map.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.query(duplicated, all, Map.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.query(duplicated, Filters.eq("name", "zz"), Named.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.query(duplicated, all, 0, 0, Map.class)),
                        // distinct decodes in the call: the failure is not deferred to the consumption of the stream.
                        () -> assertThrows(BsonInvalidOperationException.class, () -> executor.distinct("n", Long.class)),
                        () -> assertThrows(BsonInvalidOperationException.class, () -> executor.distinct("n", all, Long.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.distinct("n", int.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> executor.aggregate(Arrays.asList(null, Aggregates.match(all)))),
                        () -> assertThrows(IllegalArgumentException.class,
                                () -> executor.aggregate(Arrays.asList(Aggregates.match(all), null), Document.class)));

                // The deleted document {_id, name, n, k} has several fields: Integer cannot hold it, but it is gone already.
                final Bson toDelete = Filters.eq("k", "del");

                assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete(toDelete, Integer.class));
                assertEquals(1L, executor.count(toDelete));
                assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete(toDelete, new FindOneAndDeleteOptions(), Integer.class));
                assertEquals(0L, executor.count(toDelete));
                assertTrue(executor.exists(Filters.eq("name", "c")));

                // Field names are stored names: the entity's id property is stored as _id (a 24-hex String as an ObjectId).
                final String hexId = new ObjectId().toHexString();
                final Identified entity = new Identified();
                entity.setId(hexId);
                entity.setName("e");
                executor.insertOne(entity);

                assertAll(() -> assertEquals(0L, executor.count(Filters.eq("id", hexId))),
                        () -> assertEquals(1L, executor.count(Filters.eq("_id", new ObjectId(hexId)))),
                        () -> assertTrue(executor.queryForSingleValue("id", Filters.eq("name", "e"), String.class).isPresent()),
                        () -> assertEquals(null, executor.queryForSingleValue("id", Filters.eq("name", "e"), String.class).orElseThrow()),
                        () -> assertEquals(hexId, executor.queryForSingleValue("_id", Filters.eq("name", "e"), String.class).orElseThrow()));
            } finally {
                db.getCollection(collectionName).drop();
            }
        }
    }
}
