package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.landawn.abacus.util.NamingPolicy;

public class BigQueryExecutorTest extends BigQueryExecutorTestSupport {

    @Test
    public void testConstructorWithBigQuery() {
        BigQueryExecutor executor = new BigQueryExecutor(mockBigQuery);
        assertNotNull(executor);
        assertEquals(mockBigQuery, executor.bigQuery());
    }

    @Test
    public void testConstructorWithBigQueryAndNamingPolicy() {
        BigQueryExecutor executor = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        assertNotNull(executor);
        assertEquals(mockBigQuery, executor.bigQuery());
    }

    @Test
    public void testBigQuery() {
        assertEquals(mockBigQuery, executor.bigQuery());
    }

    @Test
    public void testExecutorWithDifferentNamingPolicies() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        // Test SNAKE_CASE (default)
        BigQueryExecutor executor1 = new BigQueryExecutor(mockBigQuery, NamingPolicy.SNAKE_CASE);
        TestEntity entity1 = new TestEntity();
        entity1.setId(1);
        executor1.insert(entity1);

        // Test SCREAMING_SNAKE_CASE
        BigQueryExecutor executor2 = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        TestEntity entity2 = new TestEntity();
        entity2.setId(2);
        executor2.insert(entity2);

        // Test CAMEL_CASE
        BigQueryExecutor executor3 = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        TestEntity entity3 = new TestEntity();
        entity3.setId(3);
        executor3.insert(entity3);

        verify(mockBigQuery, times(3)).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUnsupportedNamingPolicy() {
        // The constructor now fails fast on an unsupported policy (was a deferred ISE on first DML).
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE));
    }

    // ---------- Additional coverage: constructor validation ----------

    @Test
    public void testConstructor_NullBigQuery() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(null));
    }

    @Test
    public void testConstructor_NullNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, null));
    }

    @Test
    public void testConstructor_NullBigQueryWithNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(null, NamingPolicy.SNAKE_CASE));
    }

    // ---------- bigQuery() accessor returns the same instance ----------

    @Test
    public void testBigQuery_ReturnsSameInstance() {
        BigQuery another = mock(BigQuery.class);
        BigQueryExecutor exec = new BigQueryExecutor(another);
        assertSame(another, exec.bigQuery());
    }
}
