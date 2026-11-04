/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.NavigableSet;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientAsyncPrefetchScanner;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.ClientSimpleScanner;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.ConnectionImplementation;
import org.apache.hadoop.hbase.client.ConnectionRegistry;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.ReversedClientScanner;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

class AnyScanReview20261103Test extends TestBase {

    private static final String CACHING_KEY = "hbase.client.scanner.caching";
    private static final String MAX_RESULT_SIZE_KEY = "hbase.client.scanner.max.result.size";
    private static final String ASYNC_PREFETCH_KEY = "hbase.client.scanner.async.prefetch";

    // HBase's synchronous HTable over a mocked ConnectionImplementation: getScanner builds the real client scanner
    // (choosing its class and applying the connection defaults) without making any RPC, so the AnyScan Javadoc on
    // caching, maximum result size and async prefetch is checked against hbase-client itself.
    private static Table offlineTable(final Configuration conf) throws Exception {
        final Constructor<ConnectionConfiguration> ctor = ConnectionConfiguration.class.getDeclaredConstructor(Configuration.class);
        ctor.setAccessible(true);
        final ConnectionConfiguration connConf = ctor.newInstance(conf);

        final ConnectionImplementation conn = mock(ConnectionImplementation.class);
        final ConnectionRegistry registry = mock(ConnectionRegistry.class);
        when(registry.getConnectionString()).thenReturn("offline");
        when(conn.getConfiguration()).thenReturn(conf);
        when(conn.getConnectionConfiguration()).thenReturn(connConf);
        when(conn.getConnectionRegistry()).thenReturn(registry);
        when(conn.getNewRpcRetryingCallerFactory(any())).thenReturn(mock(RpcRetryingCallerFactory.class));
        when(conn.getTableBuilder(any(), any())).thenCallRealMethod();

        final Field connConfField = ConnectionImplementation.class.getDeclaredField("connectionConfig");
        connConfField.setAccessible(true);
        connConfField.set(conn, connConf);

        return conn.getTableBuilder(TableName.valueOf("t"), mock(ExecutorService.class)).build();
    }

    private static Object scannerField(final ResultScanner scanner, final String name) throws Exception {
        final Field field = ClientScanner.class.getDeclaredField(name);
        field.setAccessible(true);

        return field.get(scanner);
    }

    // Opens the scanner for anyScan and returns "<scanner class> caching=<n> maxResultSize=<n>".
    private static String openedScanner(final Table table, final AnyScan anyScan) throws Exception {
        try (ResultScanner scanner = table.getScanner(anyScan.val())) {
            return scanner.getClass().getSimpleName() + " caching=" + scannerField(scanner, "caching") + " maxResultSize="
                    + scannerField(scanner, "maxScannerResultSize");
        }
    }

    @Test
    void nonPositiveCachingAndMaxResultSizeUseTheClientsConfiguredDefault() throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.setInt(CACHING_KEY, 77);
        conf.setLong(MAX_RESULT_SIZE_KEY, 4242L);
        final Table table = offlineTable(conf);

        final AnyScan unset = AnyScan.create();
        final AnyScan zero = AnyScan.create().setCaching(0).setMaxResultSize(0);
        final AnyScan negative = AnyScan.create().setCaching(-5).setMaxResultSize(-5);
        final AnyScan positive = AnyScan.create().setCaching(5).setMaxResultSize(9);

        try {
            assertAll(() -> assertEquals("ClientSimpleScanner caching=77 maxResultSize=4242", openedScanner(table, unset)),
                    () -> assertEquals("ClientSimpleScanner caching=77 maxResultSize=4242", openedScanner(table, zero)),
                    () -> assertEquals("ClientSimpleScanner caching=77 maxResultSize=4242", openedScanner(table, negative)),
                    () -> assertEquals("ClientSimpleScanner caching=5 maxResultSize=9", openedScanner(table, positive)),
                    // The getters keep returning what was set: HTable applies the defaults to a copy of the Scan.
                    () -> assertEquals(-1, unset.getCaching()), () -> assertEquals(-1L, unset.getMaxResultSize()), () -> assertEquals(0, zero.getCaching()),
                    () -> assertEquals(0L, zero.getMaxResultSize()), () -> assertEquals(-5, negative.getCaching()),
                    () -> assertEquals(-5L, negative.getMaxResultSize()));
        } finally {
            table.close();
        }
    }

    @Test
    void unsetAsyncPrefetchUsesTheConnectionSettingAndAReversedScanIgnoresIt() throws Exception {
        final Table defaultConf = offlineTable(HBaseConfiguration.create());

        final Configuration prefetchConf = HBaseConfiguration.create();
        prefetchConf.setBoolean(ASYNC_PREFETCH_KEY, true);
        final Table prefetchOn = offlineTable(prefetchConf);

        try {
            assertAll(() -> assertNull(AnyScan.create().isAsyncPrefetch()),
                    () -> assertEquals(ClientSimpleScanner.class.getSimpleName(), openedScanner(defaultConf, AnyScan.create()).split(" ")[0]),
                    () -> assertEquals(ClientAsyncPrefetchScanner.class.getSimpleName(),
                            openedScanner(defaultConf, AnyScan.create().setAsyncPrefetch(true)).split(" ")[0]),
                    () -> assertEquals(ReversedClientScanner.class.getSimpleName(),
                            openedScanner(defaultConf, AnyScan.create().setAsyncPrefetch(true).setReversed(true)).split(" ")[0]),
                    () -> assertEquals(ClientAsyncPrefetchScanner.class.getSimpleName(), openedScanner(prefetchOn, AnyScan.create()).split(" ")[0]),
                    () -> assertEquals(ClientSimpleScanner.class.getSimpleName(),
                            openedScanner(prefetchOn, AnyScan.create().setAsyncPrefetch(false)).split(" ")[0]),
                    () -> assertEquals(ReversedClientScanner.class.getSimpleName(),
                            openedScanner(prefetchOn, AnyScan.create().setReversed(true)).split(" ")[0]));
        } finally {
            defaultConf.close();
            prefetchOn.close();
        }
    }

    @Test
    void addColumnLimitsAFamilyAddedEarlierAndAddFamilyResetsEarlierColumns() {
        final byte[] cf = Bytes.toBytes("cf");
        final byte[] q = Bytes.toBytes("q");

        final AnyScan columnAfterFamily = AnyScan.create().addFamily("cf").addColumn("cf", "q");
        final AnyScan columnAfterFamilyBytes = AnyScan.create().addFamily(cf).addColumn(cf, q);
        final AnyScan familyAfterColumnBytes = AnyScan.create().addColumn(cf, q).addColumn(cf, Bytes.toBytes("q2")).addFamily(cf);

        assertAll(() -> {
            final NavigableSet<byte[]> qualifiers = columnAfterFamily.getFamilyMap().get(cf);
            assertNotNull(qualifiers);
            assertEquals(1, qualifiers.size());
            assertArrayEquals(q, qualifiers.first());
        }, () -> {
            final NavigableSet<byte[]> qualifiers = columnAfterFamilyBytes.getFamilyMap().get(cf);
            assertNotNull(qualifiers);
            assertEquals(1, qualifiers.size());
            assertArrayEquals(q, qualifiers.first());
        }, () -> {
            assertTrue(familyAfterColumnBytes.getFamilyMap().containsKey(cf));
            assertNull(familyAfterColumnBytes.getFamilyMap().get(cf)); // null = every qualifier of the family
        });
    }
}
