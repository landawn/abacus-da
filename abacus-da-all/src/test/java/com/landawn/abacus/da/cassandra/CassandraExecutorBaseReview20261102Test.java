package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Criteria;
import com.landawn.abacus.util.NamingPolicy;

public class CassandraExecutorBaseReview20261102Test extends CassandraExecutorBaseTestSupport {

    // The builder resolves a property name in any case or in snake_case ("ACCOUNT_ID" and "account_id" are accountId), but the
    // primary-key guard compared names exactly: update(entity, ["account_id"]) built UPDATE people SET account_id = :account_id
    // WHERE account_id = :accountId, which the server rejects only after a round trip (failing a whole batch), and a DELETE of a
    // key column was built the same way. Every spelling of a key property is now the documented IllegalArgumentException.
    @Test
    public void test_keyPropertyInAnySpellingIsRejectedBeforeCqlRuns() {
        final ReviewAccount entity = new ReviewAccount();
        entity.setAccountId(7L);
        entity.setDisplayName("Ada");

        final RecordingExecutor recording = new RecordingExecutor();
        final Map<String, Object> keyedProps = new LinkedHashMap<>();
        keyedProps.put("accountId", 7L);
        keyedProps.put("ACCOUNT_ID", 8L);
        keyedProps.put("displayName", "Ada");

        assertAll(() -> assertKeyRejected("propNamesToUpdate", "account_id", () -> recording.update(entity, List.of("displayName", "account_id"))),
                () -> assertKeyRejected("propNamesToUpdate", "ACCOUNTID", () -> recording.update(entity, List.of("ACCOUNTID"))),
                () -> assertKeyRejected("propNamesToUpdate", "account_id", () -> recording.recordingAsync().update(entity, List.of("account_id"))),
                () -> assertKeyRejected("props", "ACCOUNT_ID",
                        () -> recording.update(ReviewAccount.class, Map.of("ACCOUNT_ID", 1L), Filters.eq("accountId", 7L))),
                () -> assertKeyRejected("propNamesToDelete", "Account_Id", () -> recording.delete(entity, List.of("Account_Id"))),
                () -> assertKeyRejected("propNamesToDelete", "account_id",
                        () -> recording.delete(ReviewAccount.class, List.of("account_id"), Filters.eq("accountId", 7L))),
                // A map batch row: the exact key spelling forms the WHERE clause, another spelling of it may not be SET.
                () -> assertKeyRejected("props", "ACCOUNT_ID", () -> executor.prepareBatchMapUpdateForTest(ReviewAccount.class, keyedProps)),
                () -> assertEquals(List.of(), recording.recorded));
    }

    // The resolution must not reject a non-key property: another spelling of one, a column name declared with @Column, a property
    // whose name merely starts like a key, a dotted UDT-field path whose last segment is named like a key, or a Map entity's entries.
    @Test
    public void test_nonKeyPropertiesInAnySpellingAreStillAccepted() {
        final ReviewAccount entity = new ReviewAccount();
        entity.setAccountId(7L);
        entity.setDisplayName("Ada");
        entity.setAccountIdNote("n");

        final ReviewMapEntity mapEntity = new ReviewMapEntity();
        mapEntity.setId(3L);
        mapEntity.put("name", "x");

        final RecordingExecutor recording = new RecordingExecutor();
        recording.update(entity, List.of("display_name"));
        recording.update(entity, List.of("accountIdNote"));
        recording.update(ReviewAccount.class, Map.of("DISPLAY_NAME", "B"), Filters.eq("accountId", 7L));
        recording.delete(ReviewAccount.class, List.of("home.accountId"), Filters.eq("accountId", 7L));
        recording.update(mapEntity, List.of("name"));
        recording.update(mapEntity);

        assertEquals(
                List.of("UPDATE people_review SET display_name = :displayName WHERE account_id = :accountId [Ada, 7]",
                        "UPDATE people_review SET account_id_note = :accountIdNote WHERE account_id = :accountId [n, 7]",
                        "UPDATE people_review SET display_name = :DISPLAY_NAME WHERE account_id = :accountId [B, 7]",
                        "DELETE home.account_id FROM people_review WHERE account_id = :accountId [7]",
                        "UPDATE review_map_rows SET name = :name WHERE id = :id [x, 3]", "UPDATE review_map_rows SET name = :name WHERE id = :id [x, 3]"),
                recording.recorded);

        assertEquals("UPDATE people_review SET DISPLAY_NAME = :displayName WHERE ACCOUNT_ID = :accountId",
                new TestCassandraExecutor(NamingPolicy.SCREAMING_SNAKE_CASE).exposedPrepareUpdate(entity, List.of("displayName")).query());
    }

    // Pins the "Row Limits" class documentation: the methods that append their own LIMIT reject a condition that carries one, before
    // any CQL runs; list/query/stream apply the condition's LIMIT as given.
    @Test
    public void test_conditionWithLimitRejectedOnlyByMethodsThatAppendTheirOwn() {
        final Condition limited = Criteria.builder().where(Filters.eq("accountId", 7L)).limit(5).build();
        final RecordingExecutor recording = new RecordingExecutor();

        assertAll(() -> assertThrows(IllegalStateException.class, () -> recording.findFirst(ReviewAccount.class, limited)),
                () -> assertThrows(IllegalStateException.class, () -> recording.exists(ReviewAccount.class, Filters.limit(1))),
                () -> assertThrows(IllegalStateException.class, () -> recording.queryForLong(ReviewAccount.class, "accountId", limited)),
                () -> assertThrows(IllegalStateException.class, () -> recording.exposedPrepareQuery(ReviewAccount.class, null, limited, 2)),
                () -> assertEquals(List.of(), recording.recorded));

        recording.list(ReviewAccount.class, List.of("displayName"), limited);
        recording.count(ReviewAccount.class, limited);

        assertEquals(List.of("SELECT display_name AS \"displayName\" FROM people_review WHERE account_id = :accountId LIMIT 5 [7]",
                "SELECT count(*) FROM people_review WHERE account_id = :accountId LIMIT 5 [7]"), recording.recorded);
    }

    private static void assertKeyRejected(final String argumentName, final String propName, final org.junit.jupiter.api.function.Executable action) {
        assertEquals("'" + argumentName + "' must not contain primary-key property: " + propName,
                assertThrows(IllegalArgumentException.class, action).getMessage());
    }

    @Table("people_review")
    public static class ReviewAccount {
        @Id
        private Long accountId;
        private String displayName;
        private String accountIdNote;
        @Column
        private ReviewHome home;

        public Long getAccountId() {
            return accountId;
        }

        public void setAccountId(final Long accountId) {
            this.accountId = accountId;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(final String displayName) {
            this.displayName = displayName;
        }

        public String getAccountIdNote() {
            return accountIdNote;
        }

        public void setAccountIdNote(final String accountIdNote) {
            this.accountIdNote = accountIdNote;
        }

        public ReviewHome getHome() {
            return home;
        }

        public void setHome(final ReviewHome home) {
            this.home = home;
        }
    }

    public static class ReviewHome {
        private Long accountId;

        public Long getAccountId() {
            return accountId;
        }

        public void setAccountId(final Long accountId) {
            this.accountId = accountId;
        }
    }

    @com.landawn.abacus.annotation.Entity
    @Table("review_map_rows")
    public static class ReviewMapEntity extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;

        public Long getId() {
            return (Long) get("id");
        }

        public void setId(final Long id) {
            put("id", id);
        }
    }
}
