package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSB;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.SCCB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.AbstractCondition;
import com.landawn.abacus.query.condition.And;
import com.landawn.abacus.query.condition.Cell;
import com.landawn.abacus.query.condition.Clause;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.On;
import com.landawn.abacus.query.condition.Operator;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.NamingPolicy;

class CqlBuilderLineCoverageTest extends TestBase {

    private static final String FOR_UPDATE_MESSAGE = "Cassandra CQL does not support FOR UPDATE (row locking); use a lightweight transaction (onlyIf(...)/ifExists()) for a conditional write instead";

    private static final String BLANK_SORT_TEXT = "   ";

    @Test
    public void intoRejectsAnInsertWhoseBeanPropertiesAreAllNull() {
        final IllegalStateException exception = assertThrows(IllegalStateException.class, () -> PSC.insert(new Note()).into("notes"));

        assertEquals("Column names must be set by insert() before calling into()", exception.getMessage());
    }

    @Test
    public void usingTimestampOnADeleteIsInsertedBeforeAnIfTokenOutsideQuotes() {
        final String cql = PSB.deleteFrom("t").where(Filters.expr("path = $$ab$$ IF b = 2")).usingTimestampMicros(5L).build().query();

        assertEquals("DELETE FROM t USING TIMESTAMP 5 WHERE path = $$ab$$ IF b = 2", cql);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void rawInsertInlinesAFiniteNonStandardNumber() {
        final String cql = SCCB.insert(java.util.Map.of("x", new AtomicInteger(7))).into("t").build().query();

        assertEquals("INSERT INTO t (x) VALUES (7)", cql);
    }

    @Test
    public void aSecondUsingOptionScansAnUnclosedQuoteToTheEndOfTheStatement() {
        final String cql = PSC.insert("x").into("t").usingTimestamp("5 'unterminated").usingTTL(60).build().query();

        assertEquals("INSERT INTO t (x) VALUES (?) USING TIMESTAMP 5 'unterminated AND TTL 60", cql);
    }

    @Test
    public void aSecondUsingOptionTreatsATrailingDoubleSlashAsAnUnterminatedLineComment() {
        final String cql = PSC.insert("x").into("t").usingTimestamp("5 // tail").usingTTL(60).build().query();

        assertEquals("INSERT INTO t (x) VALUES (?) USING TIMESTAMP 5 // tail AND TTL 60", cql);
    }

    @Test
    public void aSecondUsingOptionScansAnUnclosedDollarQuoteToTheEndOfTheStatement() {
        final String cql = PSC.insert("x").into("t").usingTimestamp("5 $$notclosed").usingTTL(60).build().query();

        assertEquals("INSERT INTO t (x) VALUES (?) USING TIMESTAMP 5 $$notclosed AND TTL 60", cql);
    }

    @Test
    public void onlyIfRejectsASelectStatement() {
        final IllegalStateException exception = assertThrows(IllegalStateException.class, () -> PSC.select("id").from("t").onlyIf("a = 1"));

        assertEquals("IF conditions are valid only for UPDATE and DELETE statements", exception.getMessage());
    }

    @Test
    public void setDelegatesANullOrNonBeanClassToTheParentBuilder() {
        final CqlBuilder update = PSC.update("users");

        final IllegalArgumentException nullClass = assertThrows(IllegalArgumentException.class, () -> update.set((Class<?>) null, Set.<String>of()));
        final IllegalArgumentException nonBean = assertThrows(IllegalArgumentException.class, () -> update.set(String.class, Set.<String>of()));

        assertEquals("'entityClass' cannot be null", nullClass.getMessage());
        assertEquals("Not a bean class: java.lang.String - CharSequence implementations are never treated as beans", nonBean.getMessage());
    }

    @Test
    public void selectFromLeavesAnUnterminatedQuotedColumnNameUnchanged() {
        final String cql = PSC.select(OpenQuote.class).from("t").build().query();

        assertEquals("SELECT \"unterminated AS \"label\" FROM t", cql);
    }

    @Test
    public void appendRejectsAForUpdateCell() {
        final CqlBuilder select = PSC.select("id").from("t");
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> select.append(new ForUpdate(Filters.eq("id", 1))));

        assertEquals(FOR_UPDATE_MESSAGE, exception.getMessage());
        assertEquals("SELECT id FROM t", select.build().query());
    }

    @Test
    public void hasSortDirectionIsFalseForANullSortList() throws ReflectiveOperationException {
        final Method hasSortDirection = CqlBuilder.class.getDeclaredMethod("hasSortDirection", Condition.class);
        hasSortDirection.setAccessible(true);

        assertEquals(Boolean.FALSE, hasSortDirection.invoke(null, new Object[] { null }));
    }

    @Test
    public void aGroupByWithBlankTextIsNotASortDirectionAndIsOtherwiseUnsupported() {
        final CqlBuilder select = PSC.select("id").from("t");
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> select.append(new GroupByClause(new TextCondition(BLANK_SORT_TEXT))));

        assertEquals("Unsupported condition: " + BLANK_SORT_TEXT, exception.getMessage());
        assertEquals("SELECT id FROM t", select.build().query());
    }

    @Test
    public void fromRejectsAKeyspaceQualifierWithNoTableName() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("users."));

        assertEquals(
                "expr must be one CQL table name (optionally keyspace-qualified); aliases, joins, subqueries, and multiple tables are not supported: users.",
                exception.getMessage());
    }

    @Test
    public void fromRendersAPlainTableAndAKeyspaceQualifiedTable() {
        assertEquals("SELECT id FROM users", PSC.select("id").from("users").build().query());
        assertEquals("SELECT id FROM ks.users", PSC.select("id").from("ks.users").build().query());
    }

    @Test
    public void aReservedKeywordChainThatEndsWithADotIsLeftUnquoted() {
        assertEquals("DELETE order. FROM t", PSC.delete("order.").from("t").build().query());
    }

    @Test
    public void deleteRendersAnUnterminatedQuotedMappedColumnUnchanged() {
        assertEquals("DELETE \"unterminated FROM open_quote", PSC.delete("label").from(OpenQuote.class).build().query());
    }

    @Test
    public void threeReservedKeywordPartsOfOneColumnAreQuoted() {
        assertEquals("SELECT \"order\".\"add\".\"desc\" AS \"order.add.desc\" FROM t", PSC.select("order.add.desc").from("t").build().query());
    }

    @Test
    public void fromRejectsASelectThatExcludedEveryColumn() {
        final IllegalStateException exception = assertThrows(IllegalStateException.class, () -> PSC.select(Note.class, Set.of("body")).from("notes"));

        assertEquals("Column names must be set by select() before calling from()", exception.getMessage());
    }

    @Test
    public void onlyIfRejectsAnOnCondition() {
        final CqlBuilder update = PSC.update("users").set("status").where(Filters.eq("id", 1));
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> update.onlyIf(new On(Filters.eq("b", 2))));

        assertEquals("Cassandra CQL does not support SQL clause condition: ON", exception.getMessage());
        assertEquals("UPDATE users SET status = ? WHERE id = ?", update.build().query());
    }

    @Test
    public void onlyIfRejectsAnEmptyAndJunction() {
        final CqlBuilder update = PSC.update("users").set("status").where(Filters.eq("id", 1));
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> update.onlyIf(new And()));

        assertEquals("The junction condition(AND) doesn't include any element.", exception.getMessage());
        assertEquals("UPDATE users SET status = ? WHERE id = ?", update.build().query());
    }

    @Test
    public void whereRejectsAnUnrecognizedCondition() {
        final CqlBuilder select = PSC.select("id").from("t");
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> select.where(new TextCondition("odd-condition")));

        assertEquals("Unsupported condition: odd-condition", exception.getMessage());
        assertEquals("SELECT id FROM t", select.build().query());
    }

    @Test
    public void forDialectReturnsTheCachedDslForAKnownDialect() {
        assertSame(PSC, CqlBuilder.Dsl.forDialect(PSC.sqlDialect));
    }

    @Test
    public void selectFromRejectsNestedEntityProperties() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> PSC.selectFrom(ParentWithChild.class, null, true, null));

        assertEquals("Cassandra CQL does not support the multi-table FROM clause required by nested entity properties", exception.getMessage());
    }

    public static class ChildPlace {
        private String street;

        public String getStreet() {
            return street;
        }

        public void setStreet(final String street) {
            this.street = street;
        }
    }

    public static class ParentWithChild {
        @Id
        private Long id;
        private ChildPlace child;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public ChildPlace getChild() {
            return child;
        }

        public void setChild(final ChildPlace child) {
            this.child = child;
        }
    }

    /** One nullable column, so an unset instance stages no INSERT values. */
    public static class Note {
        private String body;

        public String getBody() {
            return body;
        }

        public void setBody(final String body) {
            this.body = body;
        }
    }

    /** Column text is an opening quote that never closes. */
    public static class OpenQuote {
        @Column("\"unterminated")
        private String label;

        public String getLabel() {
            return label;
        }

        public void setLabel(final String label) {
            this.label = label;
        }
    }

    private static final class ForUpdate extends Cell {
        ForUpdate(final Condition condition) {
            super(Operator.FOR_UPDATE, condition);
        }
    }

    private static final class GroupByClause extends Clause {
        GroupByClause(final Condition condition) {
            super(Operator.GROUP_BY, condition);
        }
    }

    private static final class TextCondition extends AbstractCondition {
        private final String text;

        TextCondition(final String text) {
            super(Operator.EQUAL);
            this.text = text;
        }

        @Override
        public ImmutableList<Object> parameters() {
            return ImmutableList.empty();
        }

        @Override
        public String toSql(final NamingPolicy namingPolicy) {
            return text;
        }
    }
}
