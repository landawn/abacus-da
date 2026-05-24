/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Pure unit tests for the abstract {@link AnyQuery} base class. Exercised
 * through its concrete subclass {@link AnyGet} (and {@code AnyScan} where
 * applicable). Tests cover filter, consistency, isolation, authorization,
 * ACL, replica id, on-demand column family loading, and column-family time ranges.
 */
public class AnyQueryTest extends TestBase {

    // ---------------------------------------------------------------------
    // Filter
    // ---------------------------------------------------------------------

    @Test
    public void testSetFilter_andGetter() {
        FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
        AnyGet get = AnyGet.of("row");
        AnyGet returned = get.setFilter(filter);
        assertSame(get, returned);
        assertSame(filter, get.getFilter());
    }

    @Test
    public void testSetFilter_nullClearsFilter() {
        AnyGet get = AnyGet.of("row").setFilter(new FirstKeyOnlyFilter());
        get.setFilter(null);
        assertNull(get.getFilter());
    }

    @Test
    public void testSetFilter_filterList() {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(new FirstKeyOnlyFilter());
        AnyGet get = AnyGet.of("row").setFilter(list);
        assertSame(list, get.getFilter());
    }

    // ---------------------------------------------------------------------
    // Authorizations
    // ---------------------------------------------------------------------

    @Test
    public void testSetAuthorizations_andGetter() throws DeserializationException {
        Authorizations auths = new Authorizations("PUBLIC", "INTERNAL");
        AnyGet get = AnyGet.of("row");
        AnyGet returned = get.setAuthorizations(auths);
        assertSame(get, returned);
        Authorizations retrieved = get.getAuthorizations();
        assertNotNull(retrieved);
        assertEquals(2, retrieved.getLabels().size());
    }

    // ---------------------------------------------------------------------
    // ACL
    // ---------------------------------------------------------------------

    @Test
    public void testGetACL_initiallyNull() {
        AnyGet get = AnyGet.of("row");
        assertNull(get.getACL());
    }

    @Test
    public void testSetACL_singleUser() {
        AnyGet get = AnyGet.of("row").setACL("alice", new Permission(Permission.Action.READ));
        // ACL is serialized to a byte array internally
        assertNotNull(get.getACL());
    }

    @Test
    public void testSetACL_userMap() {
        Map<String, Permission> acl = new HashMap<>();
        acl.put("alice", new Permission(Permission.Action.READ));
        acl.put("bob", new Permission(Permission.Action.READ));
        AnyGet get = AnyGet.of("row").setACL(acl);
        assertNotNull(get.getACL());
    }

    // ---------------------------------------------------------------------
    // Consistency
    // ---------------------------------------------------------------------

    @Test
    public void testSetConsistency_strong() {
        AnyGet get = AnyGet.of("row").setConsistency(Consistency.STRONG);
        assertEquals(Consistency.STRONG, get.getConsistency());
    }

    @Test
    public void testSetConsistency_timeline() {
        AnyGet get = AnyGet.of("row").setConsistency(Consistency.TIMELINE);
        assertEquals(Consistency.TIMELINE, get.getConsistency());
    }

    // ---------------------------------------------------------------------
    // Replica ID
    // ---------------------------------------------------------------------

    @Test
    public void testSetReplicaId_andGetter() {
        AnyGet get = AnyGet.of("row").setReplicaId(3);
        assertEquals(3, get.getReplicaId());
    }

    @Test
    public void testGetReplicaId_default() {
        AnyGet get = AnyGet.of("row");
        // Default replica id is -1 (no specific replica)
        assertEquals(-1, get.getReplicaId());
    }

    // ---------------------------------------------------------------------
    // Isolation level
    // ---------------------------------------------------------------------

    @Test
    public void testSetIsolationLevel_readCommitted() {
        AnyGet get = AnyGet.of("row").setIsolationLevel(IsolationLevel.READ_COMMITTED);
        assertEquals(IsolationLevel.READ_COMMITTED, get.getIsolationLevel());
    }

    @Test
    public void testSetIsolationLevel_readUncommitted() {
        AnyGet get = AnyGet.of("row").setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        assertEquals(IsolationLevel.READ_UNCOMMITTED, get.getIsolationLevel());
    }

    // ---------------------------------------------------------------------
    // Load column families on demand
    // ---------------------------------------------------------------------

    @Test
    public void testSetLoadColumnFamiliesOnDemand_true() {
        AnyGet get = AnyGet.of("row").setLoadColumnFamiliesOnDemand(true);
        assertEquals(Boolean.TRUE, get.getLoadColumnFamiliesOnDemandValue());
        assertTrue(get.doLoadColumnFamiliesOnDemand());
    }

    @Test
    public void testSetLoadColumnFamiliesOnDemand_false() {
        AnyGet get = AnyGet.of("row").setLoadColumnFamiliesOnDemand(false);
        assertEquals(Boolean.FALSE, get.getLoadColumnFamiliesOnDemandValue());
    }

    @Test
    public void testGetLoadColumnFamiliesOnDemandValue_unsetReturnsNull() {
        AnyGet get = AnyGet.of("row");
        assertNull(get.getLoadColumnFamiliesOnDemandValue());
    }

    // ---------------------------------------------------------------------
    // Column-family time range
    // ---------------------------------------------------------------------

    @Test
    public void testSetColumnFamilyTimeRange_string() {
        AnyGet get = AnyGet.of("row").setColumnFamilyTimeRange("cf", 100L, 200L);
        Map<byte[], TimeRange> map = get.getColumnFamilyTimeRange();
        assertEquals(1, map.size());
    }

    @Test
    public void testSetColumnFamilyTimeRange_byteArray() {
        AnyGet get = AnyGet.of("row").setColumnFamilyTimeRange(Bytes.toBytes("cf"), 100L, 200L);
        Map<byte[], TimeRange> map = get.getColumnFamilyTimeRange();
        assertEquals(1, map.size());
    }

    @Test
    public void testSetColumnFamilyTimeRange_multipleFamilies() {
        AnyGet get = AnyGet.of("row").setColumnFamilyTimeRange("cf1", 100L, 200L).setColumnFamilyTimeRange("cf2", 300L, 400L);
        Map<byte[], TimeRange> map = get.getColumnFamilyTimeRange();
        assertEquals(2, map.size());
    }

    @Test
    public void testGetColumnFamilyTimeRange_emptyByDefault() {
        AnyGet get = AnyGet.of("row");
        Map<byte[], TimeRange> map = get.getColumnFamilyTimeRange();
        assertNotNull(map);
        assertEquals(0, map.size());
    }

    // ---------------------------------------------------------------------
    // Same behavior via AnyScan (different concrete subclass)
    // ---------------------------------------------------------------------

    @Test
    public void testSetConsistency_viaAnyScan() {
        AnyScan scan = AnyScan.create().setConsistency(Consistency.TIMELINE);
        assertEquals(Consistency.TIMELINE, scan.getConsistency());
    }

    @Test
    public void testSetFilter_viaAnyScan() {
        FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
        AnyScan scan = AnyScan.create().setFilter(filter);
        assertSame(filter, scan.getFilter());
    }
}
