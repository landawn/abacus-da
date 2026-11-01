package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.hadoop.hbase.Cell;
import org.junit.jupiter.api.Test;

class AnyDeleteValidationOrderTest {
    @Test
    void invalidTimestampIsRejectedBeforeConsultingTheFamilyComparator() {
        final byte[] family = { 1 };
        final byte[] qualifier = { 2 };
        final List<Consumer<AnyDelete>> invalidAdds = List.of(delete -> delete.addFamily("family", -1), delete -> delete.addFamily(family, -1),
                delete -> delete.addFamilyVersion("family", -1), delete -> delete.addFamilyVersion(family, -1),
                delete -> delete.addColumn("family", "qualifier", -1), delete -> delete.addColumn(family, qualifier, -1),
                delete -> delete.addColumns("family", "qualifier", -1), delete -> delete.addColumns(family, qualifier, -1));

        for (final Consumer<AnyDelete> add : invalidAdds) {
            final AnyDelete delete = AnyDelete.of("row", 1, new TreeMap<byte[], List<Cell>>());
            final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> add.accept(delete));
            assertEquals("Timestamp cannot be negative. ts=-1", failure.getMessage());
            assertTrue(delete.isEmpty());
        }
    }
}
