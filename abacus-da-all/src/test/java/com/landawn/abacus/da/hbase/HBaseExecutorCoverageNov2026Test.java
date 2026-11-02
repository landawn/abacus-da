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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Service;
import com.google.protobuf.StringValue;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.NamingPolicy;

/**
 * Closes the 2026-11-01 JaCoCo gaps of {@link HBaseExecutor} and the {@code Any*} operation
 * wrappers: driver interaction branches (coprocessor throwable wrapping, close, lazy scan
 * stream, debug logging), static row-key registration validation, and the result-to-entity
 * mapping paths (versioned containers, nested-bean qualifier routing, contested cells,
 * qualifier-variant claims).
 *
 * <h2>Unreachable gaps</h2>
 * <ul>
 * <li>HBaseExecutor 327-328 (constructor admin-leak cleanup): needs an {@link IOException}
 *     thrown after {@code conn.getAdmin()} succeeded, but the only call in between (the
 *     {@code AsyncHBaseExecutor} constructor) cannot throw one, so {@code tmpAdmin} is always
 *     {@code null} when the finally block runs exceptionally.</li>
 * <li>HBaseExecutor 631 branch outcome: needs a bean-typed property carrying a present but
 *     EMPTY {@code @Column} value; ParserUtil normalizes an empty {@code @Column} to absent.</li>
 * <li>HBaseExecutor 704 branch outcome ({@code columnFieldMap == null}): every family the
 *     cell-writer map writes was registered in the family map by some property - a variant
 *     dropped as claimed is claimed exactly because another property registered it.</li>
 * <li>HBaseExecutor 901 ternary null arm and 904 false outcome (readsBackIntoWriter): a
 *     flattened bean's qualifier enters the cell-writer map only through a matching nested
 *     column/name, so the nested writer is never null and the nested qualifier map always
 *     resolves the qualifier back to the writing property.</li>
 * <li>HBaseExecutor 977 positive ternary match and 978 return (findNestedBeanOwner): a
 *     qualifier reaching the nested-bean search is written by no flattened bean of the family,
 *     and the name/{@code @Column} match there re-derives exactly the coordinates whose absence
 *     already caused the earlier registration/routing checks to reject it.</li>
 * <li>HBaseExecutor 1438 / 1456 false outcome: {@code familyColumnValueTypeMap} always gains the
 *     family in the same pass that registers {@code familyColumnMapMap}/{@code familyColumnCollectionMap},
 *     so the {@code familyTypeMap != null} guard is always true when reached.</li>
 * <li>HBaseExecutor 1490 / 1495: concurrency guard - sequentially getRowKeySetMethod resolves and
 *     registers the row key (clearing the family-map cache) before the sole-writer map is built,
 *     so a sole-writer entry for the row-key property can never be observed.</li>
 * <li>HBaseExecutor 1526 / 1527: the EMPTY-qualifier fallback is held by a scalar only in that
 *     scalar's own unfixed family, which no other property writes, so a contested cell never
 *     resolves to a non-bean fallback.</li>
 * <li>HBaseExecutor 1575 branch outcome ({@code familyPropInfo == null}): fieldName always
 *     originates from maps keyed by registered property names, so the lookup cannot miss.</li>
 * <li>HBaseExecutor 2034 branch outcome and 2035-2036 (checkEntityClass rejection): the only call
 *     site is AnyPut's private create, and every public AnyPut.create entry rejects a non-bean
 *     earlier through ParserUtil.getBeanInfo, so the executor-level rejection message is dead.</li>
 * <li>AnyGet 627-628, AnyScan 924-925, AnyIncrement 641-642: the wrapped HBase
 *     {@code setTimeRange} implementations validate their arguments with
 *     {@link IllegalArgumentException} and never throw the declared checked {@link IOException},
 *     so the wrapping catch blocks are dead with the installed driver.</li>
 * </ul>
 */
class HBaseExecutorCoverageNov2026Test {

    // ========================================================================================================
    // executor lifecycle: close()
    // ========================================================================================================

    @Test
    void closeWrapsCheckedAdminFailureInIoExceptionAndSuppressesConnectionFailure() throws Exception {
        final Connection connection = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        when(connection.getAdmin()).thenReturn(admin);
        when(connection.isClosed()).thenReturn(false);

        final Throwable adminFailure = new Throwable("admin close failed");
        doAnswer(invocation -> {
            throw adminFailure;
        }).when(admin).close();

        final RuntimeException connectionFailure = new IllegalStateException("connection close failed");
        doAnswer(invocation -> {
            throw connectionFailure;
        }).when(connection).close();

        final HBaseExecutor executor = new HBaseExecutor(connection);

        final IOException thrown = assertThrows(IOException.class, executor::close);
        assertSame(adminFailure, thrown.getCause());
        // the connection failure is suppressed on the original admin failure, which travels as the cause
        assertEquals(1, thrown.getCause().getSuppressed().length);
        assertSame(connectionFailure, thrown.getCause().getSuppressed()[0]);
    }

    @Test
    void closeSkipsClosingAnAlreadyClosedConnection() throws Exception {
        final Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        when(connection.isClosed()).thenReturn(true);

        final HBaseExecutor executor = new HBaseExecutor(connection);
        executor.close();

        verify(connection, never()).close();
    }

    // ========================================================================================================
    // coprocessor services: Throwable wrapping branches
    // ========================================================================================================

    @Test
    void coprocessorServiceWrapsNonExceptionThrowableAndRethrowsRuntimeExceptionAsIs() throws Throwable {
        final Table table = mock(Table.class);
        final HBaseExecutor executor = executorFor(table);

        try {
            final Batch.Call<Service, String> callable = service -> "ok";
            final Batch.Callback<String> callback = (region, row, value) -> {
            };

            final Throwable boom = new Throwable("region coprocessor failed");
            doAnswer(invocation -> {
                throw boom;
            }).when(table).coprocessorService(eq(Service.class), any(byte[].class), any(byte[].class), eq(callable), eq(callback));

            final Exception wrapped = assertThrows(Exception.class,
                    () -> executor.coprocessorService("coproc-tbl", Service.class, "s", "e", callable, callback));
            assertSame(boom, wrapped.getCause());

            final RuntimeException rt = new IllegalStateException("region call blew up");
            doAnswer(invocation -> {
                throw rt;
            }).when(table).coprocessorService(eq(Service.class), any(byte[].class), any(byte[].class), eq(callable), eq(callback));

            final RuntimeException rethrown = assertThrows(IllegalStateException.class,
                    () -> executor.coprocessorService("coproc-tbl", Service.class, "s", "e", callable, callback));
            assertSame(rt, rethrown);

            verify(table, times(2)).close();
        } finally {
            executor.close();
        }
    }

    @Test
    void batchCoprocessorServiceWrapsNonExceptionThrowableAndRethrowsRuntimeExceptionAsIs() throws Throwable {
        final Table table = mock(Table.class);
        final HBaseExecutor executor = executorFor(table);

        try {
            final Descriptors.MethodDescriptor method = mock(Descriptors.MethodDescriptor.class);
            final StringValue request = StringValue.of("request");
            final StringValue response = StringValue.of("response");

            final Throwable boom = new Throwable("batch coprocessor failed");
            doAnswer(invocation -> {
                throw boom;
            }).when(table).batchCoprocessorService(eq(method), eq(request), any(byte[].class), any(byte[].class), eq(response));

            final Exception wrapped = assertThrows(Exception.class, () -> executor.batchCoprocessorService("coproc-tbl", method, request, "s", "e", response));
            assertSame(boom, wrapped.getCause());

            final RuntimeException rt = new IllegalStateException("batch call blew up");
            doAnswer(invocation -> {
                throw rt;
            }).when(table).batchCoprocessorService(eq(method), eq(request), any(byte[].class), any(byte[].class), eq(response));

            final RuntimeException rethrown = assertThrows(IllegalStateException.class,
                    () -> executor.batchCoprocessorService("coproc-tbl", method, request, "s", "e", response));
            assertSame(rt, rethrown);

            verify(table, times(2)).close();
        } finally {
            executor.close();
        }
    }

    @Test
    void batchCoprocessorServiceWithCallbackWrapsNonExceptionThrowableAndRethrowsRuntimeExceptionAsIs() throws Throwable {
        final Table table = mock(Table.class);
        final HBaseExecutor executor = executorFor(table);

        try {
            final Descriptors.MethodDescriptor method = mock(Descriptors.MethodDescriptor.class);
            final StringValue request = StringValue.of("request");
            final StringValue response = StringValue.of("response");
            final Batch.Callback<StringValue> callback = (region, row, value) -> {
            };

            final Throwable boom = new Throwable("batch callback coprocessor failed");
            doAnswer(invocation -> {
                throw boom;
            }).when(table).batchCoprocessorService(eq(method), eq(request), any(byte[].class), any(byte[].class), eq(response), eq(callback));

            final Exception wrapped = assertThrows(Exception.class,
                    () -> executor.batchCoprocessorService("coproc-tbl", method, request, "s", "e", response, callback));
            assertSame(boom, wrapped.getCause());

            final RuntimeException rt = new IllegalStateException("batch callback blew up");
            doAnswer(invocation -> {
                throw rt;
            }).when(table).batchCoprocessorService(eq(method), eq(request), any(byte[].class), any(byte[].class), eq(response), eq(callback));

            final RuntimeException rethrown = assertThrows(IllegalStateException.class,
                    () -> executor.batchCoprocessorService("coproc-tbl", method, request, "s", "e", response, callback));
            assertSame(rt, rethrown);

            verify(table, times(2)).close();
        } finally {
            executor.close();
        }
    }

    // ========================================================================================================
    // lazy scan stream
    // ========================================================================================================

    @Test
    void scanStreamOpensTheScannerOnFirstTraversalAndCollectsRows() throws Exception {
        final Table table = mock(Table.class);
        final Result first = resultOf(cell("r1", "cf", "name", "n1"));
        final Result second = resultOf(cell("r2", "cf", "name", "n2"));
        final ResultScanner scanner = mock(ResultScanner.class);
        when(scanner.iterator()).thenReturn(List.of(first, second).iterator());
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);

        final HBaseExecutor executor = executorFor(table);

        try {
            final List<Result> rows = executor.scan("scan-tbl", new Scan()).collect(Collectors.toList());

            assertEquals(2, rows.size());
            assertSame(first, rows.get(0));
            assertSame(second, rows.get(1));
        } finally {
            executor.close();
        }
    }

    @Test
    void scanStreamWrapsScannerOpenFailureInUncheckedIOException() throws Exception {
        final Table table = mock(Table.class);
        when(table.getScanner(any(Scan.class))).thenThrow(new IOException("scanner open failed"));

        final HBaseExecutor executor = executorFor(table);

        try {
            assertThrows(UncheckedIOException.class, () -> executor.scan("scan-tbl", new Scan()).collect(Collectors.toList()));
        } finally {
            executor.close();
        }
    }

    // ========================================================================================================
    // getTable debug logging
    // ========================================================================================================

    @Test
    void getTableLogsTheAcquisitionWhenDebugLevelIsEnabled() throws Exception {
        final Table table = mock(Table.class);
        final Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        when(connection.getTable(TableName.valueOf("logged-table"))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(connection);

        // The executor's abacus logger is JUL-backed, so the debug branch can be enabled per test
        // through the platform logging API and restored afterwards.
        final java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger(HBaseExecutor.class.getName());
        final List<java.util.logging.LogRecord> records = new ArrayList<>();
        final java.util.logging.Handler handler = new java.util.logging.Handler() {
            @Override
            public void publish(final java.util.logging.LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };

        julLogger.addHandler(handler);
        julLogger.setLevel(java.util.logging.Level.FINE);

        try {
            assertSame(table, executor.getTable("logged-table"));
            assertTrue(records.stream()
                    .anyMatch(record -> (record.getMessage() != null && record.getMessage().contains("logged-table")) || (record.getParameters() != null
                            && Arrays.stream(record.getParameters()).anyMatch(parameter -> String.valueOf(parameter).contains("logged-table")))));
        } finally {
            julLogger.setLevel(null);
            julLogger.removeHandler(handler);
        }
    }

    // ========================================================================================================
    // row-key registration validation
    // ========================================================================================================

    @SuppressWarnings("deprecation")
    @Test
    void registerRowKeyPropertyRejectsRowKeyWithoutGetterOrWithoutSetter() {
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.registerRowKeyProperty(GetterOnlyIdEntity.class, "id"));
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.registerRowKeyProperty(SetterOnlyIdEntity.class, "id"));
    }

    @SuppressWarnings("deprecation")
    @Test
    void registerRowKeyPropertyRejectsHBaseColumnTypedRowKeys() {
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.registerRowKeyProperty(SingleColumnRowKeyEntity.class, "col"));
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.registerRowKeyProperty(ColumnListRowKeyEntity.class, "cols"));
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.registerRowKeyProperty(ColumnMapRowKeyEntity.class, "cols"));
    }

    @Test
    void toEntityRejectsClassWithMultipleIdProperties() {
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> AnyPut.create(new TwoIdEntity()));
        assertTrue(thrown.getMessage().contains("Multiple ids"));
    }

    @Test
    void anyPutCreateRejectsEntityWithoutRowKeyProperty() {
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> AnyPut.create(new PlainNoIdBean()));
        assertTrue(thrown.getMessage().contains("Row key property is required"));
    }

    @Test
    void anyPutCreateRejectsNonBeanEntity() {
        // the public factory rejects the non-bean through bean introspection before the executor's
        // own checkEntityClass runs, so that inner message is unreachable (see the class javadoc)
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> AnyPut.create(new Object()));
        assertTrue(thrown.getMessage().contains("Not a bean class"));
    }

    // ========================================================================================================
    // toList / toValue result conversion
    // ========================================================================================================

    @Test
    void toListMapsBeanRowsAndSkipsEmptyResults() {
        final Result full = resultOf(cell("r1", "name", "", "Ada"));
        final Result empty = resultOf();

        final List<IdName> rows = HBaseExecutor.toList(Arrays.asList(full, empty), IdName.class);

        assertEquals(1, rows.size());
        assertEquals("r1", rows.get(0).getId());
        assertEquals("Ada", rows.get(0).getName());
    }

    @Test
    void toListReadsScalarRowsAndBeansWithoutRowKey() {
        final Result scalar = resultOf(cell("r1", "cf", "v", "Ada"));
        assertEquals(List.of("Ada"), HBaseExecutor.toList(List.of(scalar), String.class));

        final Result beanRow = resultOf(cell("r2", "name", "", "Grace"));
        final List<PlainNoIdBean> beans = HBaseExecutor.toList(List.of(beanRow), PlainNoIdBean.class);

        assertEquals(1, beans.size());
        assertEquals("Grace", beans.get(0).getName());
    }

    @Test
    void toListRejectsScalarResultWithMoreThanOneCell() {
        final Result twoCells = resultOf(cell("r", "cf", "a", "1"), cell("r", "cf", "b", "2"));

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(List.of(twoCells), String.class));
        assertTrue(thrown.getMessage().contains("more than one cell"));
    }

    @Test
    void toListReturnsDefaultValueForScalarResultWithoutCells() throws Exception {
        final CellScanner exhausted = mock(CellScanner.class);
        when(exhausted.advance()).thenReturn(false);
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);
        when(result.cellScanner()).thenReturn(exhausted);

        final List<String> values = HBaseExecutor.toList(List.of(result), String.class);

        assertEquals(1, values.size());
        assertNull(values.get(0));
    }

    @Test
    void toListRejectsMapTargetType() {
        final Result oneCell = resultOf(cell("r", "cf", "a", "1"));

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(List.of(oneCell), Map.class));
        assertTrue(thrown.getMessage().contains("Map type"));
    }

    @Test
    void toListReturnsDefaultValueWhenResultTurnsEmptyDuringConversion() {
        final Result flip = mock(Result.class);
        when(flip.isEmpty()).thenReturn(false, true);

        final List<IdName> rows = HBaseExecutor.toList(List.of(flip), IdName.class);

        assertEquals(1, rows.size());
        assertNull(rows.get(0));
    }

    @Test
    void toListWrapsIoExceptionRaisedWhileReadingResultCells() {
        final Result bad = resultWhoseCellScannerThrows();

        final UncheckedIOException thrown = assertThrows(UncheckedIOException.class, () -> HBaseExecutor.toList(List.of(bad), String.class));
        assertNotNull(thrown.getCause());
        assertEquals("cell scan failed", thrown.getCause().getMessage());
    }

    @Test
    void toValueWrapsIoExceptionRaisedWhileReadingResultCells() {
        final Result bad = resultWhoseCellScannerThrows();

        final UncheckedIOException thrown = assertThrows(UncheckedIOException.class, () -> HBaseExecutor.toValue(bad, String.class));
        assertNotNull(thrown.getCause());
        assertEquals("cell scan failed", thrown.getCause().getMessage());
    }

    @Test
    void toValueReturnsDefaultWhenTheCellScannerIsExhausted() throws Exception {
        final CellScanner exhausted = mock(CellScanner.class);
        when(exhausted.advance()).thenReturn(false);
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);
        when(result.cellScanner()).thenReturn(exhausted);

        assertNull(HBaseExecutor.toValue(result, String.class));
    }

    @Test
    void byteArrayRowKeyIsReadBackAsRawBytes() {
        final ByteArrayIdEntity source = new ByteArrayIdEntity();
        source.setId(new byte[] { 1, 2, 3, 4 });
        source.setName("Ada");

        final AnyPut put = AnyPut.create(source);
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, put.val().getRow());

        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);

        final ByteArrayIdEntity restored = HBaseExecutor.toEntity(Result.create(cells), ByteArrayIdEntity.class);

        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, restored.getId());
        assertEquals("Ada", restored.getName());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void mapperDeleteFallsBackToTheMappersRowKeyPropertyForAForeignEntity() throws Exception {
        final Table table = mock(Table.class);
        final HBaseExecutor executor = executorFor(table);

        try {
            final HBaseExecutor.HBaseMapper mapper = executor.mapper(IdName.class, "foreign-tbl", null);

            // The runtime class has no row-key property at all, so getRowKeySetMethod yields null and the
            // mapper's own row-key property name ("id") is used to read the value - which this bean lacks.
            final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> mapper.delete(new PlainNoIdBean()));
            assertTrue(thrown.getMessage().contains("id"));
            verify(table, never()).delete(any(Delete.class));
        } finally {
            executor.close();
        }
    }

    // ========================================================================================================
    // nested-bean qualifier routing (isNestedBeanQualifier / findNestedBeanOwner surroundings)
    // ========================================================================================================

    @Test
    void nestedBeanQualifierVariantMatchingAnnotatedColumnIsRoutedToTheOwningBean() {
        // (cfq, primary_name) is kept for the flattened bean because NameHolder's @Column("primary_name")
        // matches it; the @Column-annotated nested bean travels as its single JSON cell.
        final Result result = resultOf(cell("r", "other", "primaryName", "top"), cell("r", "cfq", "primary_name", "alice"),
                cell("r", "cfq", "nb", "{\"deep\":\"d\"}"));

        final NestedQualifierByColumnEntity restored = HBaseExecutor.toEntity(result, NestedQualifierByColumnEntity.class);

        assertEquals("top", restored.getPrimaryName());
        assertNotNull(restored.getPrimaryNameBean());
        assertEquals("alice", restored.getPrimaryNameBean().getValue());
        assertNotNull(restored.getNestedBlob());
        assertEquals("d", restored.getNestedBlob().getDeep());
    }

    @Test
    void anyPutWritesColumnAnnotatedNestedBeanAsSingleCell() {
        final NestedQualifierByColumnEntity source = new NestedQualifierByColumnEntity();
        source.setId("rq1");
        source.setPrimaryName("top");
        final NameHolder holder = new NameHolder();
        holder.setValue("alice");
        source.setPrimaryNameBean(holder);
        final InnerBean blob = new InnerBean();
        blob.setDeep("d");
        source.setNestedBlob(blob);

        final AnyPut put = AnyPut.create(source);

        boolean singleCellFound = false;
        for (final List<Cell> cells : put.val().getFamilyCellMap().values()) {
            for (final Cell cell : cells) {
                if ("nb".equals(qualifierOf(cell))) {
                    singleCellFound = true;
                    assertTrue(new String(valueOf(cell)).contains("d"));
                }
            }
        }
        assertTrue(singleCellFound);
    }

    @Test
    void nestedBeanQualifierVariantWithoutMatchingColumnIsNotRoutedIntoTheBean() {
        // (cfu, primary_name) matches no nested column of DeepHolder (@Column("dz") only), so the
        // variant is dropped instead of being forced into the bean; (cfu, dz) still routes to it.
        final Result result = resultOf(cell("r", "other2", "primaryName", "top"), cell("r", "cfu", "dz", "dv"), cell("r", "cfu", "primary_name", "pv"));

        final NestedQualifierUnknownEntity restored = HBaseExecutor.toEntity(result, NestedQualifierUnknownEntity.class);

        assertEquals("top", restored.getPrimaryName());
        assertNotNull(restored.getDeepHolder());
        assertEquals("dv", restored.getDeepHolder().getZ());
    }

    @Test
    void nestedBeanQualifierVariantUnderPropertyLevelFamilyWithoutClassDefaultIsDropped() {
        // (cf3, deep_holder) matches no nested column of DeepHolder3 and the search inspects a bean
        // without any family annotation along the way; the unknown qualifier is dropped.
        final Result result = resultOf(cell("r", "cf3", "deep_holder", "v"));

        final PropFamilyNestedQualifierEntity restored = HBaseExecutor.toEntity(result, PropFamilyNestedQualifierEntity.class);

        assertNull(restored.getDeepHolder());
    }

    @Test
    void fixedFamilyInnerBeanQualifierAmbiguityIsContestedAndResolvesThroughTheQualifierMap() {
        // (cff, foo_bar) is written by both the foo_bar and fooBar properties of FixedFamilyInner under
        // different naming policies: readsBackIntoWriter reports the ambiguity (contested), and the cell
        // keeps the routing of the EMPTY-qualifier fallback resolved through the inner bean's own map.
        final Result result = resultOf(cell("r", "cff", "plain", "p"), cell("r", "cff", "fx", "fv"), cell("r", "cff", "foo_bar", "fb"));

        final FixedFamilyHolder restored = HBaseExecutor.toEntity(result, FixedFamilyHolder.class);

        assertNotNull(restored.getInner());
        assertEquals("p", restored.getInner().getPlain());
        assertEquals("fv", restored.getInner().getLabeled());
        assertEquals("fb", restored.getInner().getFoo_bar());
    }

    @Test
    void contestedNestedBeanCellIsReroutedByTheOwningBeanSearch() {
        // (cfc, q) is written by two flattened beans: the cell is contested and the fallback search
        // hands it to whichever matching bean the family map visits first - exactly one of them wins.
        final Result result = resultOf(cell("r", "cfc", "sc", "sv"), cell("r", "cfc", "note", "nv"), cell("r", "cfc", "qa", "av"),
                cell("r", "cfc", "q", "shared"));

        final ContestedNestedEntity restored = HBaseExecutor.toEntity(result, ContestedNestedEntity.class);

        assertEquals("sv", restored.getLabeledScalar());
        assertEquals("nv", restored.getNote());
        assertNotNull(restored.getBeanA());
        assertEquals("av", restored.getBeanA().getA());

        final boolean routedToB = restored.getBeanB() != null && "shared".equals(restored.getBeanB().getQ());
        final boolean routedToC = restored.getBeanC() != null && "shared".equals(restored.getBeanC().getQ());
        assertTrue(routedToB ^ routedToC);
    }

    // ========================================================================================================
    // qualifier-variant claims (family names claimed across naming policies)
    // ========================================================================================================

    @Test
    void scalarQualifierVariantClaimOnScalarOwnedFamilyIsRejected() {
        // The variant families of foo_bar that fooBar's naming policies would write to are claimed by the
        // earlier foo_bar property; the claim on a scalar-held EMPTY fallback is rejected and both cells
        // still round-trip under their CAMEL_CASE coordinates.
        final VariantClaimEntity source = new VariantClaimEntity();
        source.setId("rq2");
        source.setFoo_bar("first");
        source.setFooBar("second");

        final AnyPut put = AnyPut.create(source);
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);

        final VariantClaimEntity restored = HBaseExecutor.toEntity(Result.create(cells), VariantClaimEntity.class);

        // CAMEL_CASE keeps each property's own name as its family, so both scalars round-trip untouched
        assertEquals("first", restored.getFoo_bar());
        assertEquals("second", restored.getFooBar());
    }

    @Test
    void scalarQualifierVariantClaimOnBeanOwnedFamilyIsAccepted() {
        final ClaimAcceptedEntity source = new ClaimAcceptedEntity();
        source.setId("rq3");
        final NameHolder holder = new NameHolder();
        holder.setValue("held");
        source.setFoo_bar(holder);
        source.setFooBar("fbv");
        source.setExtra("ev");

        final AnyPut put = AnyPut.create(source);
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);

        final ClaimAcceptedEntity restored = HBaseExecutor.toEntity(Result.create(cells), ClaimAcceptedEntity.class);

        assertNotNull(restored.getFoo_bar());
        assertEquals("held", restored.getFoo_bar().getValue());
        assertEquals("fbv", restored.getFooBar());
        assertEquals("ev", restored.getExtra());
    }

    @Test
    void beanEntryInClaimedFamilyRejectsTheScalarVariantClaim() {
        final ClaimRejectedEntity source = new ClaimRejectedEntity();
        source.setId("rq4");
        final NameHolder holder = new NameHolder();
        holder.setValue("held");
        source.setFoo_bar(holder);
        source.setFooBar("fbv");
        final PlainHolder extra = new PlainHolder();
        extra.setW("wv");
        source.setExtraBean(extra);

        final AnyPut put = AnyPut.create(source);
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);

        final ClaimRejectedEntity restored = HBaseExecutor.toEntity(Result.create(cells), ClaimRejectedEntity.class);

        assertNotNull(restored.getFoo_bar());
        assertEquals("held", restored.getFoo_bar().getValue());
        assertEquals("fbv", restored.getFooBar());
        assertNotNull(restored.getExtraBean());
        assertEquals("wv", restored.getExtraBean().getW());
    }

    // ========================================================================================================
    // versioned containers (Map<Long, HBaseColumn> / Collection<HBaseColumn>)
    // ========================================================================================================

    @Test
    void nestedVersionedContainersRoundTripThroughTheirCells() {
        final Result result = resultOf(cellAt("r", "cfv", "single", 10L, "sv"), cellAt("r", "cfv", "counters", 5L, "1"),
                cellAt("r", "cfv", "counters", 6L, "2"), cellAt("r", "cfv", "events", 7L, "9"), cellAt("r", "cfv", "simple", 100L, "a"),
                cellAt("r", "cfv", "simple", 90L, "b"), cell("r", "cfv", "qn", "lab"));

        final VersionedNestedHolder restored = HBaseExecutor.toEntity(result, VersionedNestedHolder.class);

        assertNotNull(restored.getInner());
        assertEquals("sv", restored.getInner().getSingle().value());
        assertEquals(10L, restored.getInner().getSingle().version());

        final Map<Long, HBaseColumn<Integer>> counters = restored.getInner().getCounters();
        assertEquals(2, counters.size());
        assertEquals(1, counters.get(5L).value());
        assertEquals(2, counters.get(6L).value());

        assertEquals(1, restored.getInner().getEvents().size());
        assertEquals(9, restored.getInner().getEvents().get(0).value());

        // the second cell for the same (family, qualifier) is skipped for a single-version scalar
        assertEquals("a", restored.getInner().getSimple());
        assertEquals("lab", restored.getInner().getLabeled());
    }

    @Test
    void nestedVersionedContainersAreWrittenAsVersionedCells() {
        final VersionedNestedHolder source = new VersionedNestedHolder();
        source.setId("rq5");
        final InnerVersions inner = new InnerVersions();
        inner.setLabeled("lab");
        inner.setSingle(HBaseColumn.valueOf("sv", 10L));

        final Map<Long, HBaseColumn<Integer>> counters = new HashMap<>();
        counters.put(5L, HBaseColumn.valueOf(1, 5L));
        counters.put(6L, HBaseColumn.valueOf(2, 6L));
        inner.setCounters(counters);

        final List<HBaseColumn<Integer>> events = new ArrayList<>();
        events.add(HBaseColumn.valueOf(9, 7L));
        inner.setEvents(events);
        inner.setSimple("s");
        source.setInner(inner);

        final AnyPut put = AnyPut.create(source);

        int counterCells = 0;
        boolean eventCellFound = false;
        boolean singleCellFound = false;

        for (final List<Cell> cells : put.val().getFamilyCellMap().values()) {
            for (final Cell cell : cells) {
                final String qualifier = qualifierOf(cell);

                if ("counters".equals(qualifier)) {
                    counterCells++;
                } else if ("events".equals(qualifier)) {
                    eventCellFound = true;
                } else if ("single".equals(qualifier)) {
                    singleCellFound = true;
                    assertEquals(10L, cell.getTimestamp());
                }
            }
        }

        assertEquals(2, counterCells);
        assertTrue(eventCellFound);
        assertTrue(singleCellFound);
    }

    @Test
    void topLevelVersionedContainersRoundTripThroughTheirCells() {
        final Result result = resultOf(cell("r", "cft", "name", "nm"), cellAt("r", "cft", "counters", 5L, "1"), cellAt("r", "cft", "counters", 6L, "2"),
                cellAt("r", "cft", "events", 7L, "9"));

        final VersionedTopEntity restored = HBaseExecutor.toEntity(result, VersionedTopEntity.class);

        assertEquals("nm", restored.getName());
        assertEquals(2, restored.getCounters().size());
        assertEquals(1, restored.getCounters().get(5L).value());
        assertEquals(2, restored.getCounters().get(6L).value());
        assertEquals(1, restored.getEvents().size());
        assertEquals(9, restored.getEvents().get(0).value());
    }

    // ========================================================================================================
    // Any* add-column cleanup and inspection
    // ========================================================================================================

    @Test
    void failedColumnAddRemovesOnlyTheEmptyFamilyEntryFromThePut() {
        final AnyPut put = AnyPut.of("row").addColumn("ok", "q", "v");
        final String longFamily = "f".repeat(200);

        assertThrows(IllegalArgumentException.class, () -> put.addColumn(longFamily, "q", "v"));

        assertEquals(1, put.val().getFamilyCellMap().size());
        assertTrue(put.val().getFamilyCellMap().containsKey(Bytes.toBytes("ok")));
    }

    @Test
    void failedIncrementAddRemovesOnlyTheEmptyFamilyEntry() {
        final AnyIncrement increment = AnyIncrement.of("row").addColumn("ok", "q", 1L);
        final String longFamily = "f".repeat(200);

        assertThrows(IllegalArgumentException.class, () -> increment.addColumn(longFamily, "q", 2L));

        assertEquals(1, increment.val().getFamilyCellMap().size());
        assertTrue(increment.val().getFamilyCellMap().containsKey(Bytes.toBytes("ok")));
    }

    @Test
    void failedDeleteMarkerAddRemovesOnlyTheEmptyFamilyEntry() {
        final AnyDelete delete = AnyDelete.of("row").addColumns("ok", "q", 1L);
        final String longFamily = "f".repeat(200);

        assertThrows(IllegalArgumentException.class, () -> delete.addColumns(longFamily, "q", 1L));

        assertEquals(1, delete.val().getFamilyCellMap().size());
        assertTrue(delete.val().getFamilyCellMap().containsKey(Bytes.toBytes("ok")));
    }

    @Test
    void hasWithTimestampChecksTheQueuedCellsOfAPresentFamily() {
        final AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");

        assertTrue(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q"), HConstants.LATEST_TIMESTAMP));
        assertFalse(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q"), 42L));
        assertFalse(put.has(Bytes.toBytes("missing"), Bytes.toBytes("q"), HConstants.LATEST_TIMESTAMP));
    }

    // ========================================================================================================
    // helpers
    // ========================================================================================================

    private static HBaseExecutor executorFor(final Table table) throws Exception {
        final Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        when(connection.getTable(any(TableName.class))).thenReturn(table);
        return new HBaseExecutor(connection);
    }

    private static Result resultWhoseCellScannerThrows() {
        final CellScanner failing = mock(CellScanner.class);
        try {
            when(failing.advance()).thenThrow(new IOException("cell scan failed"));
        } catch (final IOException impossible) {
            throw new AssertionError(impossible);
        }

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);
        when(result.cellScanner()).thenReturn(failing);

        return result;
    }

    private static Cell cell(final String row, final String family, final String qualifier, final Object value) {
        return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(String.valueOf(value)));
    }

    private static Cell cellAt(final String row, final String family, final String qualifier, final long timestamp, final Object value) {
        return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier), timestamp, Bytes.toBytes(String.valueOf(value)));
    }

    private static Result resultOf(final Cell... cells) {
        return Result.create(Arrays.asList(cells));
    }

    private static String qualifierOf(final Cell cell) {
        return Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    }

    private static byte[] valueOf(final Cell cell) {
        return Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    // ========================================================================================================
    // fixtures
    // ========================================================================================================

    public static class IdName {
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

    public static class PlainNoIdBean {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class GetterOnlyIdEntity {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class SetterOnlyIdEntity {
        private String id;
        private String name;

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

    public static class SingleColumnRowKeyEntity {
        private HBaseColumn<String> col;
        private String name;

        public HBaseColumn<String> getCol() {
            return col;
        }

        public void setCol(final HBaseColumn<String> col) {
            this.col = col;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class ColumnListRowKeyEntity {
        private List<HBaseColumn<String>> cols;
        private String name;

        public List<HBaseColumn<String>> getCols() {
            return cols;
        }

        public void setCols(final List<HBaseColumn<String>> cols) {
            this.cols = cols;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class ColumnMapRowKeyEntity {
        private Map<String, HBaseColumn<String>> cols;
        private String name;

        public Map<String, HBaseColumn<String>> getCols() {
            return cols;
        }

        public void setCols(final Map<String, HBaseColumn<String>> cols) {
            this.cols = cols;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class TwoIdEntity {
        @Id
        private String id;

        @Id
        private String code;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }

        public void setCode(final String code) {
            this.code = code;
        }
    }

    public static class ByteArrayIdEntity {
        @Id
        private byte[] id;

        private String name;

        public byte[] getId() {
            return id;
        }

        public void setId(final byte[] id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class NameHolder {
        @Column("primary_name")
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }

    public static class InnerBean {
        private String deep;

        public String getDeep() {
            return deep;
        }

        public void setDeep(final String deep) {
            this.deep = deep;
        }
    }

    @ColumnFamily("cfq")
    public static class NestedQualifierByColumnEntity {
        private String id;

        @ColumnFamily("other")
        private String primaryName;

        private NameHolder primaryNameBean;

        @Column("nb")
        private InnerBean nestedBlob;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getPrimaryName() {
            return primaryName;
        }

        public void setPrimaryName(final String primaryName) {
            this.primaryName = primaryName;
        }

        public NameHolder getPrimaryNameBean() {
            return primaryNameBean;
        }

        public void setPrimaryNameBean(final NameHolder primaryNameBean) {
            this.primaryNameBean = primaryNameBean;
        }

        public InnerBean getNestedBlob() {
            return nestedBlob;
        }

        public void setNestedBlob(final InnerBean nestedBlob) {
            this.nestedBlob = nestedBlob;
        }
    }

    public static class DeepHolder {
        @Column("dz")
        private String z;

        public String getZ() {
            return z;
        }

        public void setZ(final String z) {
            this.z = z;
        }
    }

    @ColumnFamily("cfu")
    public static class NestedQualifierUnknownEntity {
        @ColumnFamily("other2")
        private String primaryName;

        private DeepHolder deepHolder;

        public String getPrimaryName() {
            return primaryName;
        }

        public void setPrimaryName(final String primaryName) {
            this.primaryName = primaryName;
        }

        public DeepHolder getDeepHolder() {
            return deepHolder;
        }

        public void setDeepHolder(final DeepHolder deepHolder) {
            this.deepHolder = deepHolder;
        }
    }

    public static class DeepHolder3 {
        @Column("dz")
        private String z;

        public String getZ() {
            return z;
        }

        public void setZ(final String z) {
            this.z = z;
        }
    }

    public static class PlainHolder {
        private String w;

        public String getW() {
            return w;
        }

        public void setW(final String w) {
            this.w = w;
        }
    }

    public static class PropFamilyNestedQualifierEntity {
        @ColumnFamily("o3")
        private String deep_holder;

        @ColumnFamily("cf3")
        private DeepHolder3 deepHolder;

        private PlainHolder otherHolder;

        public String getDeep_holder() {
            return deep_holder;
        }

        public void setDeep_holder(final String deep_holder) {
            this.deep_holder = deep_holder;
        }

        public DeepHolder3 getDeepHolder() {
            return deepHolder;
        }

        public void setDeepHolder(final DeepHolder3 deepHolder) {
            this.deepHolder = deepHolder;
        }

        public PlainHolder getOtherHolder() {
            return otherHolder;
        }

        public void setOtherHolder(final PlainHolder otherHolder) {
            this.otherHolder = otherHolder;
        }
    }

    public static class InnerDeeper {
        private String leaf;

        public String getLeaf() {
            return leaf;
        }

        public void setLeaf(final String leaf) {
            this.leaf = leaf;
        }
    }

    @ColumnFamily("cfi")
    public static class FixedFamilyInner {
        private String plain;

        @Column("fx")
        private String labeled;

        private String foo_bar;

        private String fooBar;

        private InnerDeeper deeper;

        public String getPlain() {
            return plain;
        }

        public void setPlain(final String plain) {
            this.plain = plain;
        }

        public String getLabeled() {
            return labeled;
        }

        public void setLabeled(final String labeled) {
            this.labeled = labeled;
        }

        public String getFoo_bar() {
            return foo_bar;
        }

        public void setFoo_bar(final String foo_bar) {
            this.foo_bar = foo_bar;
        }

        public String getFooBar() {
            return fooBar;
        }

        public void setFooBar(final String fooBar) {
            this.fooBar = fooBar;
        }

        public InnerDeeper getDeeper() {
            return deeper;
        }

        public void setDeeper(final InnerDeeper deeper) {
            this.deeper = deeper;
        }
    }

    @ColumnFamily("cff")
    public static class FixedFamilyHolder {
        private FixedFamilyInner inner;

        public FixedFamilyInner getInner() {
            return inner;
        }

        public void setInner(final FixedFamilyInner inner) {
            this.inner = inner;
        }
    }

    public static class BeanA {
        @Column("qa")
        private String a;

        public String getA() {
            return a;
        }

        public void setA(final String a) {
            this.a = a;
        }
    }

    public static class BeanB {
        @Column("q")
        private String q;

        public String getQ() {
            return q;
        }

        public void setQ(final String q) {
            this.q = q;
        }
    }

    public static class BeanC {
        @Column("q")
        private String q;

        public String getQ() {
            return q;
        }

        public void setQ(final String q) {
            this.q = q;
        }
    }

    public static class BeanQ {
        @Column("qq")
        private String qq;

        public String getQq() {
            return qq;
        }

        public void setQq(final String qq) {
            this.qq = qq;
        }
    }

    @ColumnFamily("cfc")
    public static class ContestedNestedEntity {
        private BeanA beanA;

        private BeanB beanB;

        private BeanC beanC;

        private BeanQ beanQ;

        @Column("sc")
        private String labeledScalar;

        private String note;

        public BeanA getBeanA() {
            return beanA;
        }

        public void setBeanA(final BeanA beanA) {
            this.beanA = beanA;
        }

        public BeanB getBeanB() {
            return beanB;
        }

        public void setBeanB(final BeanB beanB) {
            this.beanB = beanB;
        }

        public BeanC getBeanC() {
            return beanC;
        }

        public void setBeanC(final BeanC beanC) {
            this.beanC = beanC;
        }

        public BeanQ getBeanQ() {
            return beanQ;
        }

        public void setBeanQ(final BeanQ beanQ) {
            this.beanQ = beanQ;
        }

        public String getLabeledScalar() {
            return labeledScalar;
        }

        public void setLabeledScalar(final String labeledScalar) {
            this.labeledScalar = labeledScalar;
        }

        public String getNote() {
            return note;
        }

        public void setNote(final String note) {
            this.note = note;
        }
    }

    public static class VariantClaimEntity {
        private String id;

        private String foo_bar;

        private String fooBar;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getFoo_bar() {
            return foo_bar;
        }

        public void setFoo_bar(final String foo_bar) {
            this.foo_bar = foo_bar;
        }

        public String getFooBar() {
            return fooBar;
        }

        public void setFooBar(final String fooBar) {
            this.fooBar = fooBar;
        }
    }

    public static class ClaimAcceptedEntity {
        private String id;

        private NameHolder foo_bar;

        private String fooBar;

        @ColumnFamily("foo_bar")
        private String extra;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public NameHolder getFoo_bar() {
            return foo_bar;
        }

        public void setFoo_bar(final NameHolder foo_bar) {
            this.foo_bar = foo_bar;
        }

        public String getFooBar() {
            return fooBar;
        }

        public void setFooBar(final String fooBar) {
            this.fooBar = fooBar;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(final String extra) {
            this.extra = extra;
        }
    }

    public static class ClaimRejectedEntity {
        private String id;

        private NameHolder foo_bar;

        private String fooBar;

        @ColumnFamily("foo_bar")
        private PlainHolder extraBean;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public NameHolder getFoo_bar() {
            return foo_bar;
        }

        public void setFoo_bar(final NameHolder foo_bar) {
            this.foo_bar = foo_bar;
        }

        public String getFooBar() {
            return fooBar;
        }

        public void setFooBar(final String fooBar) {
            this.fooBar = fooBar;
        }

        public PlainHolder getExtraBean() {
            return extraBean;
        }

        public void setExtraBean(final PlainHolder extraBean) {
            this.extraBean = extraBean;
        }
    }

    public static class InnerVersions {
        @Column("qn")
        private String labeled;

        private HBaseColumn<String> single;

        private Map<Long, HBaseColumn<Integer>> counters;

        private List<HBaseColumn<Integer>> events;

        private String simple;

        public String getLabeled() {
            return labeled;
        }

        public void setLabeled(final String labeled) {
            this.labeled = labeled;
        }

        public HBaseColumn<String> getSingle() {
            return single;
        }

        public void setSingle(final HBaseColumn<String> single) {
            this.single = single;
        }

        public Map<Long, HBaseColumn<Integer>> getCounters() {
            return counters;
        }

        public void setCounters(final Map<Long, HBaseColumn<Integer>> counters) {
            this.counters = counters;
        }

        public List<HBaseColumn<Integer>> getEvents() {
            return events;
        }

        public void setEvents(final List<HBaseColumn<Integer>> events) {
            this.events = events;
        }

        public String getSimple() {
            return simple;
        }

        public void setSimple(final String simple) {
            this.simple = simple;
        }
    }

    @ColumnFamily("cfv")
    public static class VersionedNestedHolder {
        private String id;

        private InnerVersions inner;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public InnerVersions getInner() {
            return inner;
        }

        public void setInner(final InnerVersions inner) {
            this.inner = inner;
        }
    }

    @ColumnFamily("cft")
    public static class VersionedTopEntity {
        private String name;

        private Map<Long, HBaseColumn<Integer>> counters;

        private List<HBaseColumn<Integer>> events;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public Map<Long, HBaseColumn<Integer>> getCounters() {
            return counters;
        }

        public void setCounters(final Map<Long, HBaseColumn<Integer>> counters) {
            this.counters = counters;
        }

        public List<HBaseColumn<Integer>> getEvents() {
            return events;
        }

        public void setEvents(final List<HBaseColumn<Integer>> events) {
            this.events = events;
        }
    }
}
