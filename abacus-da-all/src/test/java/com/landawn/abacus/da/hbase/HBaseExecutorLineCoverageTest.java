/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple2;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Covers HBaseExecutor lines the existing suites leave unexecuted. Private qualifier
 * and column-map helpers are invoked with real bean metadata; their return values are
 * the contract under test.
 */
public class HBaseExecutorLineCoverageTest extends MockitoTestBase {

    @Test
    public void multipleIdsRejectRowKeyResolution() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getRowKeySetMethod(TwoIdEntity.class));

        assertEquals("Multiple ids: " + List.of("leftId", "rightId") + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(TwoIdEntity.class),
                ex.getMessage());
    }

    @Test
    public void checkEntityClassRejectsNonBean() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.checkEntityClass(String.class));

        assertEquals("Unsupported type: java.lang.String. Only bean classes with getter/setter methods are supported", ex.getMessage());
    }

    @Test
    public void toListRejectsMapTargetOnNonEmptyResult() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> HBaseExecutor.toList(List.of(oneCell("row", "cf", "q", "v")), Map.class));

        assertEquals("Map type is not supported for HBase result conversion", ex.getMessage());
    }

    @Test
    public void toEntityReturnsDefaultWhenNonEmptyResultHasNoCell() throws IOException {
        final Result result = resultThatReportsCellsButDoesNotAdvance();

        assertNull(HBaseExecutor.toEntity(result, String.class));
    }

    @Test
    public void toListReturnsDefaultWhenSixArgConversionSeesAnEmptyResult() throws IOException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false, true);

        final List<String> values = HBaseExecutor.toList(List.of(result), String.class);

        assertEquals(1, values.size());
        assertNull(values.get(0));
    }

    @Test
    public void toListReturnsDefaultWhenCellScannerDoesNotAdvance() throws IOException {
        final List<String> values = HBaseExecutor.toList(List.of(resultThatReportsCellsButDoesNotAdvance()), String.class);

        assertEquals(1, values.size());
        assertNull(values.get(0));
    }

    @Test
    public void toListReturnsTheSingleScalarCell() {
        final List<String> values = HBaseExecutor.toList(List.of(oneCell("row", "cf", "q", "hello")), String.class);

        assertEquals(List.of("hello"), values);
    }

    @Test
    public void toEntityWrapsCellScannerIoException() throws IOException {
        final IOException failure = new IOException("cell advance failed");
        final Result result = resultWhoseScannerFails(failure);

        final UncheckedIOException ex = assertThrows(UncheckedIOException.class, () -> HBaseExecutor.toEntity(result, String.class));

        assertSame(failure, ex.getCause());
    }

    @Test
    public void toListWrapsCellScannerIoException() throws IOException {
        final IOException failure = new IOException("list advance failed");
        final Result result = resultWhoseScannerFails(failure);

        final UncheckedIOException ex = assertThrows(UncheckedIOException.class, () -> HBaseExecutor.toList(List.of(result), String.class));

        assertSame(failure, ex.getCause());
    }

    @Test
    public void toEntityCopiesByteArrayRowKey() {
        final byte[] row = new byte[] { 1, 2, 3, 4, (byte) 0xff };
        final Cell cell = new KeyValue(row, Bytes.toBytes("name"), Bytes.toBytes(HBaseExecutor.EMPTY_QUALIFIER), Bytes.toBytes("Ada"));

        final ByteKeyRow entity = HBaseExecutor.toEntity(Result.create(List.of(cell)), ByteKeyRow.class);

        assertNotNull(entity);
        assertArrayEquals(row, entity.getId());
        assertEquals("Ada", entity.getName());
    }

    @Test
    public void toEntityAllocatesNestedVersionMapBeforeAnyOtherVersionedColumn() {
        final Cell cell = versionedCell("row-map", "nested", "labels", 9L, "hello");

        final NestedMapHolder entity = HBaseExecutor.toEntity(Result.create(List.of(cell)), NestedMapHolder.class);

        assertEquals("row-map", entity.getId());
        assertNotNull(entity.getNested());
        assertEquals(Map.of(9L, HBaseColumn.valueOf("hello", 9L)), entity.getNested().getLabels());
    }

    @Test
    public void toEntityAllocatesNestedSingleVersionColumnBeforeAnyOtherVersionedColumn() {
        final Cell cell = versionedCell("row-flag", "nested", "flag", 4L, "on");

        final NestedFlagHolder entity = HBaseExecutor.toEntity(Result.create(List.of(cell)), NestedFlagHolder.class);

        assertEquals("row-flag", entity.getId());
        assertNotNull(entity.getNested());
        assertEquals(HBaseColumn.valueOf("on", 4L), entity.getNested().getFlag());
    }

    @Test
    public void toEntityKeepsTheFirstNestedScalarVersion() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(versionedCell("row-city", "nested", "city", 1L, "older"));
        cells.add(versionedCell("row-city", "nested", "city", 5L, "newer"));
        cells.sort(CellComparator.getInstance());

        final NestedCityHolder entity = HBaseExecutor.toEntity(Result.create(cells), NestedCityHolder.class);

        assertEquals("row-city", entity.getId());
        assertEquals("newer", entity.getNested().getCity());
    }

    @Test
    public void getTableLogsWhenDebugIsEnabled() throws Exception {
        final Level previous = enableHBaseExecutorDebug();
        final Connection conn = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        final Table table = mock(Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);
        final HBaseExecutor executor = new HBaseExecutor(conn);

        try {
            assertTrue(LoggerFactory.getLogger(HBaseExecutor.class).isDebugEnabled(), LoggerFactory.getLogger(HBaseExecutor.class).getClass().getName());
            assertSame(table, executor.getTable("users"));
        } finally {
            Logger.getLogger(HBaseExecutor.class.getName()).setLevel(previous);
            executor.close();
        }
    }

    @Test
    public void scanWrapsScannerOpenFailure() throws Exception {
        final Connection conn = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        final Table table = mock(Table.class);
        final IOException failure = new IOException("scan failed");
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);
        when(table.getScanner(any(Scan.class))).thenThrow(failure);
        final HBaseExecutor executor = new HBaseExecutor(conn);

        try (com.landawn.abacus.util.stream.Stream<Result> rows = executor.scan("users", new Scan())) {
            final UncheckedIOException ex = assertThrows(UncheckedIOException.class, rows::count);

            assertSame(failure, ex.getCause());
        } finally {
            executor.close();
        }
    }

    @Test
    public void scanReturnsAnEmptyStreamWhenTheScannerOpens() throws Exception {
        final Connection conn = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        final Table table = mock(Table.class);
        final ResultScanner scanner = mock(ResultScanner.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.iterator()).thenReturn(List.<Result>of().iterator());
        final HBaseExecutor executor = new HBaseExecutor(conn);

        try (com.landawn.abacus.util.stream.Stream<Result> rows = executor.scan("users", new Scan())) {
            assertEquals(0L, rows.count());
        } finally {
            executor.close();
        }
    }

    @Test
    public void closeWrapsCheckedExceptionThatIsNotIoException() throws Exception {
        final Connection conn = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        final Exception checked = new Exception("not io");
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.isClosed()).thenReturn(true);
        doAnswer(invocation -> {
            throw checked;
        }).when(admin).close();
        final HBaseExecutor executor = new HBaseExecutor(conn);

        final IOException wrapped = assertThrows(IOException.class, executor::close);

        assertSame(checked, wrapped.getCause());
        assertEquals("java.lang.Exception: not io", wrapped.getMessage());
    }

    @Test
    public void nestedQualifierRoutingFollowsColumnNamesAndNamingVariants() throws Exception {
        final BeanInfo parent = ParserUtil.getBeanInfo(RoutingParent.class);

        assertTrue(invoke(boolean.class, "isNestedBeanQualifier", new Class<?>[] { BeanInfo.class, String.class, String.class, String.class }, parent, "info",
                "info", "exact_q"));
        assertFalse(invoke(boolean.class, "isNestedBeanQualifier", new Class<?>[] { BeanInfo.class, String.class, String.class, String.class }, parent, "info",
                "info", "missing"));

        final Tuple2<String, Boolean> address = Tuple.of("address", false);
        final Map<String, Tuple2<String, Boolean>> family = new HashMap<>();
        family.put("", address);
        family.put("street", address);
        assertTrue(invoke(boolean.class, "isSoleRoutableNestedBeanFallback", new Class<?>[] { BeanInfo.class, Map.class, Tuple2.class }, parent, family,
                address));

        final Tuple2<String, Boolean> label = Tuple.of("label", false);
        family.put("", label);
        assertFalse(invoke(boolean.class, "isSoleRoutableNestedBeanFallback", new Class<?>[] { BeanInfo.class, Map.class, Tuple2.class }, parent, family,
                label));

        final PropInfo collision = parent.getPropInfo("nestedCollision");
        assertFalse(invoke(boolean.class, "readsBackIntoWriter", new Class<?>[] { PropInfo.class, String.class, String.class, String.class }, collision, null,
                "shared", "user_name"));

        @SuppressWarnings("unchecked")
        final Map<String, String> nestedColumns = invoke(Map.class, "getNestedColumnFieldNameMap", new Class<?>[] { BeanInfo.class },
                ParserUtil.getBeanInfo(FixedFamilyNested.class));
        assertEquals(Map.of("explicit_col", "titled", "emailAddress", "emailAddress", "EmailAddress", "emailAddress", "email_address", "email_address",
                "EMAIL_ADDRESS", "emailAddress", "email-address", "emailAddress"), nestedColumns);

        final Tuple2<String, Boolean> primary = Tuple.of("primary", false);
        final Tuple2<String, Boolean> secondary = Tuple.of("secondary", false);
        final BeanInfo owner = ParserUtil.getBeanInfo(OwnerParent.class);
        final Tuple2<String, Boolean> byColumn = invoke(Tuple2.class, "findNestedBeanOwner", new Class<?>[] { BeanInfo.class, List.class, String.class }, owner,
                List.of(primary, secondary), "city_code");
        final Tuple2<String, Boolean> byName = invoke(Tuple2.class, "findNestedBeanOwner", new Class<?>[] { BeanInfo.class, List.class, String.class }, owner,
                List.of(secondary, primary), "street");

        assertEquals("primary", byColumn._1);
        assertEquals(Boolean.FALSE, byColumn._2);
        assertEquals("secondary", byName._1);
        assertEquals(Boolean.FALSE, byName._2);
    }

    private static Result oneCell(final String row, final String family, final String qualifier, final String value) {
        return Result.create(List.of(new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))));
    }

    private static Cell versionedCell(final String row, final String family, final String qualifier, final long timestamp, final String value) {
        return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier), timestamp, Bytes.toBytes(value));
    }

    private static Result resultThatReportsCellsButDoesNotAdvance() throws IOException {
        final Result result = mock(Result.class);
        final CellScanner scanner = mock(CellScanner.class);
        when(result.isEmpty()).thenReturn(false);
        when(result.cellScanner()).thenReturn(scanner);
        when(scanner.advance()).thenReturn(false);
        return result;
    }

    private static Result resultWhoseScannerFails(final IOException failure) throws IOException {
        final Result result = mock(Result.class);
        final CellScanner scanner = mock(CellScanner.class);
        when(result.isEmpty()).thenReturn(false);
        when(result.cellScanner()).thenReturn(scanner);
        when(scanner.advance()).thenThrow(failure);
        return result;
    }

    private static Level enableHBaseExecutorDebug() {
        final String name = HBaseExecutor.class.getName();
        final Logger jul = Logger.getLogger(name);
        final Level previous = jul.getLevel();
        jul.setLevel(Level.ALL);
        try {
            final Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
            final Object debug = levelClass.getField("DEBUG").get(null);
            final Class<?> configurator = Class.forName("org.apache.logging.log4j.core.config.Configurator");
            configurator.getMethod("setLevel", String.class, levelClass).invoke(null, name, debug);
        } catch (final Throwable ignored) {
            // Log4j 2 is not on the classpath.
        }
        try {
            final Class<?> levelClass = Class.forName("org.apache.log4j.Level");
            final Object debug = levelClass.getField("DEBUG").get(null);
            final Class<?> loggerClass = Class.forName("org.apache.log4j.Logger");
            final Object logger = loggerClass.getMethod("getLogger", String.class).invoke(null, name);
            loggerClass.getMethod("setLevel", levelClass).invoke(logger, debug);
        } catch (final Throwable ignored) {
            // reload4j is not the active backend.
        }
        return previous;
    }

    @SuppressWarnings("unchecked")
    private static <T> T invoke(final Class<T> returnType, final String name, final Class<?>[] parameterTypes, final Object... args) throws Exception {
        final Method method = HBaseExecutor.class.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        final Object value = method.invoke(null, args);
        if (value != null && !returnType.isPrimitive() && !returnType.isInstance(value)) {
            throw new ClassCastException(value.getClass().getName() + " cannot be cast to " + returnType.getName());
        }
        return (T) value;
    }

    @Data
    public static class TwoIdEntity {
        @Id
        private String leftId;

        @Id
        private String rightId;
    }

    @Data
    @NoArgsConstructor
    public static class ByteKeyRow {
        @Id
        private byte[] id;

        private String name;
    }

    @Data
    @NoArgsConstructor
    public static class NestedLabels {
        private Map<Long, HBaseColumn<String>> labels;
    }

    @Data
    @NoArgsConstructor
    public static class NestedMapHolder {
        @Id
        private String id;

        private NestedLabels nested;
    }

    @Data
    @NoArgsConstructor
    public static class NestedFlag {
        private HBaseColumn<String> flag;
    }

    @Data
    @NoArgsConstructor
    public static class NestedFlagHolder {
        @Id
        private String id;

        private NestedFlag nested;
    }

    @Data
    @NoArgsConstructor
    public static class NestedCity {
        private String city;
    }

    @Data
    @NoArgsConstructor
    public static class NestedCityHolder {
        @Id
        private String id;

        private NestedCity nested;
    }

    @Data
    public static class RoutingAddress {
        private String street;
    }

    @Data
    public static class RoutingExact {
        @Column("exact_q")
        private String titled;
    }

    @Data
    public static class RoutingCollision {
        private String userName;

        private String user_name;
    }

    @Data
    public static class RoutingParent {
        private String label;

        private RoutingAddress address;

        @Column("tagged")
        private RoutingAddress columnBean;

        private RoutingAddress plain;

        @ColumnFamily("other")
        private RoutingAddress otherFam;

        @ColumnFamily("shared")
        private RoutingCollision nestedCollision;

        private RoutingExact nestedExact;
    }

    @Data
    @ColumnFamily("fam")
    public static class FixedFamilyNested {
        @Column("explicit_col")
        private String titled;

        private String emailAddress;

        private String email_address;
    }

    @Data
    public static class OwnerCity {
        @Column("city_code")
        private String city;

        private String street;
    }

    @Data
    public static class OwnerParent {
        private OwnerCity primary;

        private OwnerCity secondary;
    }
}
