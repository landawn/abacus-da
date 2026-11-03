package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;

/**
 * A row (or UDT) read as a map class that rejects {@code null} values failed with a bare NPE from {@code Map.put} on a
 * {@code NULL} column, in both the 4.x and the 3.x executor; it is now an {@link IllegalArgumentException} naming the column and
 * the map class (as the DynamoDB executors' {@code fillMapRow} reports it). HEAD: every {@code assertThrows(IllegalArgumentException)}
 * below got a {@link NullPointerException}.
 */
class CassandraExecutorConsolidationReview20261102Test {

    // ---------------------------------------------------------------- 4.x

    @Test
    void v4_toList_concurrentHashMapRow_nullColumn_isIllegalArgument() {
        final ResultSet rs = v4Result(v4Row(new String[] { "id", "email" }, "1", null));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(rs, ConcurrentHashMap.class));

        assertRejected(e, "Column", "email", ConcurrentHashMap.class);
    }

    @Test
    void v4_toList_nestedMapRowInArray_nullColumn_isIllegalArgument() {
        // A nested row read as an array component goes through readRow's map branch.
        final Row outer = v4Row(new String[] { "nested" }, v4Row(new String[] { "a" }, (Object) null));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(v4Result(outer), Hashtable[].class));

        assertRejected(e, "Column", "a", Hashtable.class);
    }

    @Test
    void v4_toMap_hashtableSupplier_nullColumn_isIllegalArgument() {
        final Row row = v4Row(new String[] { "id", "email" }, "1", null);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toMap(row, size -> new Hashtable<>()));

        assertRejected(e, "Column", "email", Hashtable.class);
    }

    @Test
    void v4_toMap_unchangedWhenAccepted() {
        final Row row = v4Row(new String[] { "id", "nested" }, "1", v4Row(new String[] { "a" }, "x"));

        final Map<String, Object> map = CassandraExecutor.toMap(row, size -> new ConcurrentHashMap<>());

        assertEquals(ConcurrentHashMap.class, map.getClass());
        assertEquals("1", map.get("id"));
        assertEquals(Map.of("a", "x"), map.get("nested"));
        assertEquals(ConcurrentHashMap.class, map.get("nested").getClass());
        // A NULL column is still kept in a map that accepts it.
        assertTrue(CassandraExecutor.toMap(v4Row(new String[] { "a" }, (Object) null), HashMap::new).containsKey("a"));
    }

    @Test
    void v4_toMap_nullSupplierResult_keepsOldBehavior() {
        assertThrows(NullPointerException.class, () -> CassandraExecutor.toMap(v4Row(new String[] { "a" }, "x"), size -> null));
        assertNull(CassandraExecutor.toMap(v4Row(new String[0]), size -> null));
    }

    @Test
    void v4_udtDecode_concurrentHashMap_nullField_isIllegalArgument() {
        final UserDefinedType type = v4UdtType();
        final CassandraExecutor.UDTCodec<Map> hashMapCodec = CassandraExecutor.UDTCodec.create(type, Map.class);
        final CassandraExecutor.UDTCodec<ConcurrentHashMap> concurrentCodec = CassandraExecutor.UDTCodec.create(type, ConcurrentHashMap.class);
        final Map<String, Object> source = new HashMap<>();
        source.put("name", null);
        final ByteBuffer encoded = hashMapCodec.encode(source, ProtocolVersion.V4);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> concurrentCodec.decode(encoded, ProtocolVersion.V4));

        assertRejected(e, "Field", "name", ConcurrentHashMap.class);
        assertEquals(Map.of("name", "x"), concurrentCodec.decode(hashMapCodec.encode(Map.of("name", "x"), ProtocolVersion.V4), ProtocolVersion.V4));
    }

    // ---------------------------------------------------------------- 3.x

    @Test
    void v3_toList_concurrentHashMapRow_nullColumn_isIllegalArgument() {
        final com.datastax.driver.core.ResultSet rs = v3Result(v3Row(new String[] { "id", "email" }, "1", null));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(rs, ConcurrentHashMap.class));

        assertRejected(e, "Column", "email", ConcurrentHashMap.class);
    }

    @Test
    void v3_toList_nestedMapRowInArray_nullColumn_isIllegalArgument() {
        final com.datastax.driver.core.Row outer = v3Row(new String[] { "nested" }, v3Row(new String[] { "a" }, (Object) null));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(v3Result(outer), Hashtable[].class));

        assertRejected(e, "Column", "a", Hashtable.class);
    }

    @Test
    void v3_toMap_hashtableSupplier_nullColumn_isIllegalArgument() {
        final com.datastax.driver.core.Row row = v3Row(new String[] { "id", "email" }, "1", null);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toMap(row, size -> new Hashtable<>()));

        assertRejected(e, "Column", "email", Hashtable.class);
    }

    @Test
    void v3_toMap_unchangedWhenAccepted() {
        final com.datastax.driver.core.Row row = v3Row(new String[] { "id", "nested" }, "1", v3Row(new String[] { "a" }, "x"));

        final Map<String, Object> map = com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toMap(row, size -> new ConcurrentHashMap<>());

        assertEquals(ConcurrentHashMap.class, map.getClass());
        assertEquals("1", map.get("id"));
        assertEquals(Map.of("a", "x"), map.get("nested"));
        assertTrue(com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toMap(v3Row(new String[] { "a" }, (Object) null), HashMap::new).containsKey("a"));
    }

    @Test
    void v3_toMap_nullSupplierResult_keepsOldBehavior() {
        assertThrows(NullPointerException.class,
                () -> com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toMap(v3Row(new String[] { "a" }, "x"), size -> null));
        assertNull(com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toMap(v3Row(new String[0]), size -> null));
    }

    @Test
    void v3_udtDeserialize_concurrentHashMap_nullField_isIllegalArgument() throws ReflectiveOperationException {
        final com.datastax.driver.core.UserType type = v3UdtType();
        final com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec<Map> hashMapCodec = com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec
                .create(type, Map.class);
        final com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec<ConcurrentHashMap> concurrentCodec = com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec
                .create(type, ConcurrentHashMap.class);
        final Map<String, Object> source = new HashMap<>();
        source.put("name", null);
        final ByteBuffer encoded = hashMapCodec.serialize(source, com.datastax.driver.core.ProtocolVersion.V4);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> concurrentCodec.deserialize(encoded, com.datastax.driver.core.ProtocolVersion.V4));

        assertRejected(e, "Field", "name", ConcurrentHashMap.class);
        assertEquals(Map.of("name", "x"), concurrentCodec.deserialize(hashMapCodec.serialize(Map.of("name", "x"), com.datastax.driver.core.ProtocolVersion.V4),
                com.datastax.driver.core.ProtocolVersion.V4));
    }

    // ---------------------------------------------------------------- collection rows rejecting null (both versions)
    // Before this fix: a bare NullPointerException from Collection.add (HEAD: the same for ArrayDeque/ConcurrentLinkedQueue rows,
    // and "Not able to create instance" for every ArrayBlockingQueue row).

    @Test
    void v4_toList_queueRow_nullColumn_isIllegalArgument() {
        final ResultSet rs = v4Result(v4Row(new String[] { "id", "email" }, "1", null));

        assertRejectedByCollection(assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(rs, ArrayBlockingQueue.class)), "Column",
                "email", ArrayBlockingQueue.class);
        assertRejectedByCollection(assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(rs, ArrayDeque.class)), "Column", "email",
                ArrayDeque.class);
        // A collection that accepts null keeps the NULL column.
        assertEquals(Arrays.asList("1", null), CassandraExecutor.toList(rs, ArrayList.class).get(0));
    }

    @Test
    void v4_toList_nestedQueueRowInArray_nullColumn_isIllegalArgument() {
        // A nested row read as an array component goes through readRow's collection branch.
        final Row outer = v4Row(new String[] { "nested" }, v4Row(new String[] { "a" }, (Object) null));

        assertRejectedByCollection(assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(v4Result(outer), ConcurrentLinkedQueue[].class)),
                "Column", "a", ConcurrentLinkedQueue.class);
    }

    @Test
    void v4_udtDecode_queue_nullField_isIllegalArgument() {
        final UserDefinedType type = v4UdtType();
        final CassandraExecutor.UDTCodec<Map> hashMapCodec = CassandraExecutor.UDTCodec.create(type, Map.class);
        final CassandraExecutor.UDTCodec<ArrayDeque> dequeCodec = CassandraExecutor.UDTCodec.create(type, ArrayDeque.class);
        final Map<String, Object> source = new HashMap<>();
        source.put("name", null);

        assertRejectedByCollection(
                assertThrows(IllegalArgumentException.class, () -> dequeCodec.decode(hashMapCodec.encode(source, ProtocolVersion.V4), ProtocolVersion.V4)),
                "Field", "name", ArrayDeque.class);
        assertEquals(List.of("x"), List.copyOf(dequeCodec.decode(hashMapCodec.encode(Map.of("name", "x"), ProtocolVersion.V4), ProtocolVersion.V4)));
    }

    @Test
    void v3_toList_queueRow_nullColumn_isIllegalArgument() {
        final com.datastax.driver.core.ResultSet rs = v3Result(v3Row(new String[] { "id", "email" }, "1", null));

        assertRejectedByCollection(
                assertThrows(IllegalArgumentException.class, () -> com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(rs, ArrayBlockingQueue.class)),
                "Column", "email", ArrayBlockingQueue.class);
        assertRejectedByCollection(
                assertThrows(IllegalArgumentException.class, () -> com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(rs, ArrayDeque.class)), "Column",
                "email", ArrayDeque.class);
        assertEquals(Arrays.asList("1", null), com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(rs, ArrayList.class).get(0));
    }

    @Test
    void v3_toList_nestedQueueRowInArray_nullColumn_isIllegalArgument() {
        final com.datastax.driver.core.Row outer = v3Row(new String[] { "nested" }, v3Row(new String[] { "a" }, (Object) null));

        assertRejectedByCollection(
                assertThrows(IllegalArgumentException.class,
                        () -> com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(v3Result(outer), ConcurrentLinkedQueue[].class)),
                "Column", "a", ConcurrentLinkedQueue.class);
    }

    @Test
    void v3_udtDeserialize_queue_nullField_isIllegalArgument() throws ReflectiveOperationException {
        final com.datastax.driver.core.UserType type = v3UdtType();
        final com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec<Map> hashMapCodec = com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec
                .create(type, Map.class);
        final com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec<ArrayDeque> dequeCodec = com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec
                .create(type, ArrayDeque.class);
        final Map<String, Object> source = new HashMap<>();
        source.put("name", null);
        final ByteBuffer encoded = hashMapCodec.serialize(source, com.datastax.driver.core.ProtocolVersion.V4);

        assertRejectedByCollection(
                assertThrows(IllegalArgumentException.class, () -> dequeCodec.deserialize(encoded, com.datastax.driver.core.ProtocolVersion.V4)), "Field",
                "name", ArrayDeque.class);
        assertEquals(List.of("x"), List.copyOf(dequeCodec.deserialize(hashMapCodec.serialize(Map.of("name", "x"), com.datastax.driver.core.ProtocolVersion.V4),
                com.datastax.driver.core.ProtocolVersion.V4)));
    }

    // ---------------------------------------------------------------- helpers

    private static void assertRejected(final IllegalArgumentException e, final String kind, final String name, final Class<?> mapClass) {
        assertTrue(e.getMessage().startsWith(kind + " (" + name + ", null) is not accepted by the row map " + mapClass.getName()), e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
    }

    private static void assertRejectedByCollection(final IllegalArgumentException e, final String kind, final String name, final Class<?> collectionClass) {
        assertEquals(kind + " (" + name + ", null) is not accepted by the row collection " + collectionClass.getName(), e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
    }

    private static Row v4Row(final String[] names, final Object... values) {
        final Row row = mock(Row.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = mock(ColumnDefinition.class);
            when(columns.get(i)).thenReturn(column);
            when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
            when(column.getType()).thenReturn(DataTypes.TEXT);
            when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    private static ResultSet v4Result(final Row... rows) {
        final ResultSet result = mock(ResultSet.class);
        final ColumnDefinitions metadata = rows[0].getColumnDefinitions();
        when(result.getColumnDefinitions()).thenReturn(metadata);
        when(result.getAvailableWithoutFetching()).thenReturn(rows.length);
        when(result.iterator()).thenAnswer(invocation -> Arrays.asList(rows).iterator());
        when(result.all()).thenReturn(Arrays.asList(rows));
        return result;
    }

    private static UserDefinedType v4UdtType() {
        final CodecRegistry registry = new DefaultCodecRegistry("consolidation-review-udt");

        return new DefaultUserDefinedType(CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("named"), false,
                List.of(CqlIdentifier.fromInternal("name")), List.of(DataTypes.TEXT), new AttachmentPoint() {
                    @Override
                    public ProtocolVersion getProtocolVersion() {
                        return ProtocolVersion.V4;
                    }

                    @Override
                    public CodecRegistry getCodecRegistry() {
                        return registry;
                    }
                });
    }

    private static com.datastax.driver.core.Row v3Row(final String[] names, final Object... values) {
        final com.datastax.driver.core.Row row = mock(com.datastax.driver.core.Row.class);
        final com.datastax.driver.core.ColumnDefinitions columns = mock(com.datastax.driver.core.ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            when(columns.getName(i)).thenReturn(names[i]);
            when(columns.getType(i)).thenReturn(com.datastax.driver.core.DataType.text());
            when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    private static com.datastax.driver.core.ResultSet v3Result(final com.datastax.driver.core.Row... rows) {
        final com.datastax.driver.core.ResultSet result = mock(com.datastax.driver.core.ResultSet.class);
        final com.datastax.driver.core.ColumnDefinitions metadata = rows[0].getColumnDefinitions();
        when(result.getColumnDefinitions()).thenReturn(metadata);
        when(result.getAvailableWithoutFetching()).thenReturn(rows.length);
        when(result.iterator()).thenAnswer(invocation -> Arrays.asList(rows).iterator());
        when(result.all()).thenReturn(Arrays.asList(rows));
        when(result.one()).thenReturn(rows[0]);
        return result;
    }

    private static com.datastax.driver.core.UserType v3UdtType() throws ReflectiveOperationException {
        // Driver 3 exposes UDT metadata only through cluster discovery; construct the real metadata offline.
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class,
                com.datastax.driver.core.DataType.class);
        fieldConstructor.setAccessible(true);
        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, com.datastax.driver.core.ProtocolVersion.class, com.datastax.driver.core.CodecRegistry.class);
        typeConstructor.setAccessible(true);

        return typeConstructor.newInstance("ks", "named", false, List.of(fieldConstructor.newInstance("name", com.datastax.driver.core.DataType.text())),
                com.datastax.driver.core.ProtocolVersion.V4, new com.datastax.driver.core.CodecRegistry());
    }
}
