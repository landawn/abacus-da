package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.query.Filters;

@Tag("base-test")
class CassandraReviewRegressionTest {

    @Test
    void deletingAllExcludedColumnsCannotBecomeAWholeRowDelete() {
        assertThrows(IllegalArgumentException.class,
                () -> PSC.delete(Item.class, Set.of("value")).from("items").where(Filters.eq("id", 1)).build());

        assertEquals("DELETE value FROM items WHERE id = ?",
                PSC.delete(Item.class).from("items").where(Filters.eq("id", 1)).build().query());
        assertEquals("DELETE FROM items WHERE id = ?", PSC.deleteFrom("items").where(Filters.eq("id", 1)).build().query());
    }

    @Test
    void deletingColumnsOfAnEntityWithOnlyKeysIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> PSC.delete(KeyOnly.class).from("items").where(Filters.eq("id", 1)).build());
    }

    @Test
    void loadingMapperKeepsTheCallerInputStreamOpen() {
        final TrackingInputStream input = new TrackingInputStream("<cqlMapper><cql id='find'>SELECT * FROM items</cql></cqlMapper>");

        assertEquals(1, CqlMapper.loadFrom(input).size());
        assertFalse(input.closed, "The caller owns and closes this stream");
    }

    @Test
    void loadingInvalidMapperKeepsTheCallerInputStreamOpen() {
        final TrackingInputStream input = new TrackingInputStream("<cqlMapper><cql>");

        assertThrows(com.landawn.abacus.exception.ParsingException.class, () -> CqlMapper.loadFrom(input));
        assertFalse(input.closed, "A failed parse must also preserve caller ownership");
    }

    private static final class TrackingInputStream extends ByteArrayInputStream {
        private boolean closed;

        TrackingInputStream(final String xml) {
            super(xml.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    public static class KeyOnly {
        @Id
        private long id;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }
    }

    public static class Item extends KeyOnly {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }
}
