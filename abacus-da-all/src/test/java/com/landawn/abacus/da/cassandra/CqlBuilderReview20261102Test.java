package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PAC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PLC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSB;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.query.Filters;

class CqlBuilderReview20261102Test {

    @Test
    void build_rejectedIncompleteJoin_doesNotLeaveAllowFilteringMidStatement() {
        final CqlBuilder builder = PSC.select("a").from("t").join("u").allowFiltering();

        // The parent rejects the pending qualified JOIN before it releases the builder.
        assertThrows(IllegalStateException.class, builder::build);

        assertEquals("SELECT a FROM t JOIN u ON t.id = u.id ALLOW FILTERING", builder.on("t.id = u.id").build().query());
    }

    @Test
    void build_rejectedIncompleteJoin_retryWithWhereRendersAllowFilteringOnceAtTheEnd() {
        final CqlBuilder builder = PSC.select("a").from("t").allowFiltering().leftJoin("u");

        assertThrows(IllegalStateException.class, builder::build);
        assertThrows(IllegalStateException.class, builder::build);

        assertEquals("SELECT a FROM t LEFT JOIN u ON x = y WHERE b = ? ALLOW FILTERING", builder.on("x = y").where(Filters.eq("b", 1)).build().query());
    }

    @Test
    void build_withoutAllowFiltering_isUnchanged() {
        assertEquals("SELECT a FROM t WHERE b = ?", PSC.select("a").from("t").where(Filters.eq("b", 1)).build().query());
        assertEquals("SELECT a FROM t WHERE b = ? ALLOW FILTERING", PSC.select("a").from("t").allowFiltering().where(Filters.eq("b", 1)).build().query());
    }

    @Test
    void selectFrom_reservedKeywordColumns_areQuotedOnEveryCall() {
        final String expected = "SELECT id AS \"id\", \"desc\" AS \"desc\", \"order\" AS \"order\", user_name AS \"userName\" FROM keyword_entity WHERE id = ?";

        // The per-class answer is memoized; the first, cached and repeated renderings must all agree.
        for (int i = 0; i < 3; i++) {
            assertEquals(expected, PSC.selectFrom(KeywordEntity.class).where(Filters.eq("id", 1)).build().query());
        }

        assertEquals(expected, PSC.select(KeywordEntity.class).from(KeywordEntity.class).where(Filters.eq("id", 1)).build().query());
    }

    @Test
    void selectFrom_reservedKeywordColumns_areQuotedForEveryNamingPolicy() {
        assertEquals("SELECT ID AS \"id\", \"desc\" AS \"desc\", \"order\" AS \"order\", USER_NAME AS \"userName\" FROM KEYWORD_ENTITY",
                PAC.selectFrom(KeywordEntity.class).build().query());
        assertEquals("SELECT id AS \"id\", \"desc\" AS \"desc\", \"order\" AS \"order\", userName AS \"userName\" FROM keywordEntity",
                PLC.selectFrom(KeywordEntity.class).build().query());
        assertEquals("SELECT id, \"desc\", \"order\", userName FROM KeywordEntity", PSB.selectFrom(KeywordEntity.class).build().query());
        assertEquals("SELECT id AS \"id\", \"desc\" AS \"desc\", \"order\" AS \"order\", user_name AS \"userName\" FROM keyword_entity WHERE id = :id",
                NSC.selectFrom(KeywordEntity.class).where(Filters.eq("id", 1)).build().query());
    }

    @Test
    void selectFrom_entityWithoutReservedKeywordColumn_keepsTheParentRendering() {
        for (int i = 0; i < 2; i++) {
            assertEquals("SELECT id AS \"id\", user_name AS \"userName\", note2 AS \"note2\" FROM plain_entity",
                    PSC.selectFrom(PlainEntity.class).build().query());
        }

        // Interleaving another class must not mix up the memoized answers.
        assertEquals("SELECT id AS \"id\", \"desc\" AS \"desc\", \"order\" AS \"order\", user_name AS \"userName\" FROM keyword_entity",
                PSC.selectFrom(KeywordEntity.class).build().query());
        assertEquals("SELECT id AS \"id\", user_name AS \"userName\", note2 AS \"note2\" FROM plain_entity", PSC.selectFrom(PlainEntity.class).build().query());
    }

    @Test
    void selectFrom_keywordOnlyUnderSomeNamingPolicies_isMemoizedPerPolicy() {
        // "inTo" is the reserved word INTO under lower camel case, but in_to under snake case is not: a memo keyed by the class
        // alone would reuse the first policy's answer for the other one. Each class starts with an empty memo, so the two
        // classes check both orders.
        final String snake = "SELECT id AS \"id\", in_to AS \"inTo\" FROM policy_keyword_entity_a";
        final String camel = "SELECT id AS \"id\", \"into\" AS \"inTo\" FROM policyKeywordEntityA";

        assertEquals(snake, PSC.selectFrom(PolicyKeywordEntityA.class).build().query());
        assertEquals(camel, PLC.selectFrom(PolicyKeywordEntityA.class).build().query());
        assertEquals(snake, PSC.selectFrom(PolicyKeywordEntityA.class).build().query());

        assertEquals(camel.replace("EntityA", "EntityB"), PLC.selectFrom(PolicyKeywordEntityB.class).build().query());
        assertEquals(snake.replace("entity_a", "entity_b"), PSC.selectFrom(PolicyKeywordEntityB.class).build().query());
        assertEquals(camel.replace("EntityA", "EntityB"), PLC.selectFrom(PolicyKeywordEntityB.class).build().query());
    }

    @Test
    void reservedKeywordDetection_isCaseInsensitiveAndIgnoresNamesWithDigitsOrUnderscores() {
        assertEquals("SELECT \"desc\", \"order\", \"limit\", desc2, order_no, \"from\" FROM t",
                PSB.select("DESC", "Order", "lImIt", "desc2", "order_no", "from").from("t").build().query());
        assertEquals("UPDATE t SET \"desc\" = ?, descx = ? WHERE \"by\" = ?",
                PSB.update("t").set(java.util.List.of("Desc", "descx")).where(Filters.eq("BY", 1)).build().query());
    }

    public static class KeywordEntity {
        @Id
        private long id;
        private String desc;
        private String order;
        private String userName;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(final String desc) {
            this.desc = desc;
        }

        public String getOrder() {
            return order;
        }

        public void setOrder(final String order) {
            this.order = order;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }
    }

    public static class PolicyKeywordEntityA {
        @Id
        private long id;
        private String inTo;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getInTo() {
            return inTo;
        }

        public void setInTo(final String inTo) {
            this.inTo = inTo;
        }
    }

    public static class PolicyKeywordEntityB extends PolicyKeywordEntityA {
    }

    public static class PlainEntity {
        @Id
        private long id;
        private String userName;
        private String note2;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }

        public String getNote2() {
            return note2;
        }

        public void setNote2(final String note2) {
            this.note2 = note2;
        }
    }
}
