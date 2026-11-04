package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PAC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSB;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.SCCB;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;

/**
 * 2026-11-03 review of {@link CqlBuilder}: a batch INSERT renders its column list once and repeats that text for every
 * later row (it was re-rendered per row); these tests pin that every row still carries the same, fully rendered column
 * list (reserved-keyword quoting, naming policy, entity column mapping) and values, and that the per-class memo of the
 * reserved-keyword select check honors the entity's column mapping. {@code select(Class, true, excluded)} now asks the
 * exclusion set itself, like every other exclusion path of the builder.
 */
class CqlBuilderReview20261103Test extends TestBase {

    private static Map<String, Object> row(final Object desc, final Object userName, final Object order) {
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("desc", desc);
        row.put("userName", userName);
        row.put("order", order);
        return row;
    }

    @Test
    void batchInsert_everyRowCarriesTheRenderedColumnList() {
        final List<Map<String, Object>> rows = List.of(row(1, "a", 10), row(2, "b'c", 20), row(3, "d", 30));

        final SP psc = PSC.batchInsert(rows).into("t").build();
        final String nsc = NSC.batchInsert(rows).into("ks.order").build().query();
        final String pac = PAC.batchInsert(rows).into("t").build().query();
        final String psb = PSB.batchInsert(rows).into("t").build().query();
        final String sccb = SCCB.batchInsert(rows).into("t").build().query();

        assertAll(
                () -> assertEquals("BEGIN BATCH INSERT INTO t (\"desc\", user_name, \"order\") VALUES (?, ?, ?);"
                        + " INSERT INTO t (\"desc\", user_name, \"order\") VALUES (?, ?, ?);"
                        + " INSERT INTO t (\"desc\", user_name, \"order\") VALUES (?, ?, ?); APPLY BATCH", psc.query()),
                () -> assertEquals(List.of(1, "a", 10, 2, "b'c", 20, 3, "d", 30), psc.parameters()),
                () -> assertEquals("BEGIN BATCH INSERT INTO ks.\"order\" (\"desc\", user_name, \"order\") VALUES (:desc_0, :userName_0, :order_0);"
                        + " INSERT INTO ks.\"order\" (\"desc\", user_name, \"order\") VALUES (:desc_1, :userName_1, :order_1);"
                        + " INSERT INTO ks.\"order\" (\"desc\", user_name, \"order\") VALUES (:desc_2, :userName_2, :order_2); APPLY BATCH", nsc),
                () -> assertEquals("BEGIN BATCH INSERT INTO t (\"desc\", USER_NAME, \"order\") VALUES (?, ?, ?);"
                        + " INSERT INTO t (\"desc\", USER_NAME, \"order\") VALUES (?, ?, ?);"
                        + " INSERT INTO t (\"desc\", USER_NAME, \"order\") VALUES (?, ?, ?); APPLY BATCH", pac),
                () -> assertEquals("BEGIN BATCH INSERT INTO t (\"desc\", userName, \"order\") VALUES (?, ?, ?);"
                        + " INSERT INTO t (\"desc\", userName, \"order\") VALUES (?, ?, ?);"
                        + " INSERT INTO t (\"desc\", userName, \"order\") VALUES (?, ?, ?); APPLY BATCH", psb),
                () -> assertEquals("BEGIN BATCH INSERT INTO t (\"desc\", user_name, \"order\") VALUES (1, 'a', 10);"
                        + " INSERT INTO t (\"desc\", user_name, \"order\") VALUES (2, 'b''c', 20);"
                        + " INSERT INTO t (\"desc\", user_name, \"order\") VALUES (3, 'd', 30); APPLY BATCH", sccb));
    }

    @Test
    void batchInsert_usingAndIfClausesAreAddedToEveryRow() {
        final List<Map<String, Object>> rows = List.of(row(1, "a", 10), row(2, "b", 20));

        assertEquals(
                "BEGIN BATCH INSERT INTO t (\"desc\", user_name, \"order\") VALUES (?, ?, ?) IF NOT EXISTS USING TTL 60;"
                        + " INSERT INTO t (\"desc\", user_name, \"order\") VALUES (?, ?, ?) IF NOT EXISTS USING TTL 60; APPLY BATCH",
                PSC.batchInsert(rows).into("t").usingTTL(60).ifNotExists().build().query());
    }

    @Test
    void batchInsert_intoEntityClass_mapsTheColumnsOfEveryRow() {
        final List<MappedRow> rows = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            final MappedRow row = new MappedRow();
            row.setId(i);
            row.setOrderNo("o" + i);
            row.setUserName("u" + i);
            rows.add(row);
        }

        final SP sp = PSC.batchInsert(rows).into(MappedRow.class).build();

        assertAll(
                () -> assertEquals("BEGIN BATCH INSERT INTO mapped_row (id, \"order\", user_name) VALUES (?, ?, ?);"
                        + " INSERT INTO mapped_row (id, \"order\", user_name) VALUES (?, ?, ?);"
                        + " INSERT INTO mapped_row (id, \"order\", user_name) VALUES (?, ?, ?); APPLY BATCH", sp.query()),
                () -> assertEquals(List.of(1L, "o1", "u1", 2L, "o2", "u2", 3L, "o3", "u3"), sp.parameters()));
    }

    @Test
    void batchInsert_rejectedColumnName_leavesNoPartialBatch() {
        final Map<String, Object> first = new LinkedHashMap<>();
        first.put("a", 1);
        first.put("b -- c", 2);

        final Map<String, Object> second = new LinkedHashMap<>(first);
        final CqlBuilder builder = PSC.batchInsert(List.of(first, second));

        assertThrows(IllegalArgumentException.class, () -> builder.into("t"));
        // Nothing of the rejected batch stays behind: the builder still has no table, so completing it fails as before.
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void selectFrom_reservedKeywordOnlyThroughColumnMapping_isQuoted() {
        // The memoized reserved-keyword answer is computed from the entity's column mapping (orderNo -> "order"), not from
        // the property names, which hold no reserved keyword.
        final String expected = "SELECT id AS \"id\", \"order\" AS \"orderNo\", user_name AS \"userName\" FROM mapped_row WHERE id = ?";

        for (int i = 0; i < 2; i++) {
            assertEquals(expected, PSC.selectFrom(MappedRow.class).where(Filters.eq("id", 1)).build().query());
        }
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void select_withSubEntityProperties_asksTheExclusionSetLikeEveryOtherExclusionPath() {
        final TreeSet<String> firstNameAnyCase = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        firstNameAnyCase.add("FIRSTNAME");

        final TreeSet<String> homeAnyCase = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        homeAnyCase.add("HOME");

        final TreeSet<Object> integers = new TreeSet<>();
        integers.add(1);
        final Set<String> wronglyTyped = (Set) integers;

        final String all = "SELECT id AS \"id\", first_name AS \"firstName\", sub_entity_addr.street AS \"home.street\", sub_entity_addr.city AS \"home.city\" FROM p";

        assertAll(
                // Without sub-entity properties the set itself decides (QueryUtil asks it); with them it must decide as well.
                () -> assertEquals("SELECT id AS \"id\" FROM p", PSC.select(SubEntityPerson.class, false, firstNameAnyCase).from("p").build().query()),
                () -> assertEquals("SELECT id AS \"id\", sub_entity_addr.street AS \"home.street\", sub_entity_addr.city AS \"home.city\" FROM p",
                        PSC.select(SubEntityPerson.class, true, firstNameAnyCase).from("p").build().query()),
                () -> assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM p",
                        PSC.select(SubEntityPerson.class, true, homeAnyCase).from("p").build().query()),
                // Unchanged for an ordinary set: a sub-entity name excludes its nested columns, a nested name only itself.
                () -> assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM p",
                        PSC.select(SubEntityPerson.class, true, Set.of("home")).from("p").build().query()),
                () -> assertEquals("SELECT id AS \"id\", first_name AS \"firstName\", sub_entity_addr.street AS \"home.street\" FROM p",
                        PSC.select(SubEntityPerson.class, true, new HashSet<>(List.of("home.city", "home.str", "home."))).from("p").build().query()),
                () -> assertEquals(all, PSC.select(SubEntityPerson.class, true, Set.of("x", "y")).from("p").build().query()),
                // A set that cannot compare a String excludes nothing, as on every other exclusion path (it threw ArrayStoreException).
                () -> assertEquals(all, PSC.select(SubEntityPerson.class, true, wronglyTyped).from("p").build().query()),
                () -> assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM p",
                        PSC.select(SubEntityPerson.class, false, wronglyTyped).from("p").build().query()));
    }

    public static class SubEntityAddr {
        private String street;
        private String city;

        public String getStreet() {
            return street;
        }

        public void setStreet(final String street) {
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    public static class SubEntityPerson {
        @Id
        private long id;
        private String firstName;
        private SubEntityAddr home;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public SubEntityAddr getHome() {
            return home;
        }

        public void setHome(final SubEntityAddr home) {
            this.home = home;
        }
    }

    public static class MappedRow {
        @Id
        private long id;
        @Column("order")
        private String orderNo;
        private String userName;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getOrderNo() {
            return orderNo;
        }

        public void setOrderNo(final String orderNo) {
            this.orderNo = orderNo;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }
    }
}
