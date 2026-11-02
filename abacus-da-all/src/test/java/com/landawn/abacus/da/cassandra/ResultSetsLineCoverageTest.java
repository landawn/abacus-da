package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.landawn.abacus.da.MockitoTestBase;

class ResultSetsLineCoverageTest extends MockitoTestBase {

    @Test
    void nextOnAnEmptyPageThrowsNoSuchElementException() {
        final AsyncResultSet page = mock(AsyncResultSet.class);
        when(page.currentPage()).thenReturn(List.of());
        when(page.hasMorePages()).thenReturn(false);
        when(page.getExecutionInfo()).thenReturn(mock(ExecutionInfo.class));

        final ResultSet result = ResultSets.wrap(page);
        final Iterator<Row> rows = result.iterator();

        assertFalse(rows.hasNext());
        final NoSuchElementException exception = assertThrows(NoSuchElementException.class, rows::next);
        assertNull(exception.getMessage());
        verify(page, never()).fetchNextPage();
    }
}
