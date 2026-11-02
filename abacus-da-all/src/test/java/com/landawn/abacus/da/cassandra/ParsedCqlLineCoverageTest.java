package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolableAdapter;

class ParsedCqlLineCoverageTest extends MockitoTestBase {

    @Test
    public void aQuotedLiteralInsideBracesClosesTheQuoteScan() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'k': 'v'} WHERE id = 1");

        assertEquals("UPDATE t SET m = {'k': 'v'} WHERE id = 1", parsed.parameterizedCql());
        assertEquals("UPDATE t SET m = {'k': 'v'} WHERE id = 1", parsed.originalCql());
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void aBackslashInsideAQuotedLiteralThatAlsoContainsAMarkerIsSkipped() {
        final String cql = "INSERT INTO t (c) VALUES ({'a\\b#{x}'})";
        final ParsedCql parsed = ParsedCql.parse(cql);

        assertEquals(cql, parsed.parameterizedCql());
        assertEquals(cql, parsed.originalCql());
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void anUnclosedBlockCommentIsNotADataOperation() {
        final ParsedCql parsed = ParsedCql.parse("/* never closed SELECT * FROM t");

        assertEquals("/* never closed SELECT * FROM t", parsed.parameterizedCql());
        assertEquals("/* never closed SELECT * FROM t", parsed.originalCql());
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void commentOnlyAndEmptyTextAreNotDataOperations() {
        final ParsedCql line = ParsedCql.parse("-- only a comment");
        final ParsedCql block = ParsedCql.parse("/* closed */");
        final ParsedCql blank = ParsedCql.parse("   ");

        assertEquals("-- only a comment", line.parameterizedCql());
        assertEquals(0, line.parameterCount());
        assertTrue(line.namedParameters().isEmpty());

        assertEquals("/* closed */", block.parameterizedCql());
        assertEquals(0, block.parameterCount());
        assertTrue(block.namedParameters().isEmpty());

        assertEquals("", blank.parameterizedCql());
        assertEquals("", blank.originalCql());
        assertEquals(0, blank.parameterCount());
        assertTrue(blank.namedParameters().isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void aCacheThatClosesDuringLookupAndInsertStillReturnsTheParsedStatement() throws IllegalStateException {
        final KeyedObjectPool<String, PoolableAdapter<ParsedCql>> cache = mock(KeyedObjectPool.class);
        when(cache.isClosed()).thenReturn(false);
        when(cache.get(any())).thenThrow(new IllegalStateException("get closed"));
        when(cache.put(any(), any())).thenThrow(new IllegalStateException("put closed"));

        final ParsedCql parsed = ParsedCql.parse("SELECT a FROM t WHERE id = :id", cache);

        assertEquals("SELECT a FROM t WHERE id = ?", parsed.parameterizedCql());
        assertEquals("SELECT a FROM t WHERE id = :id", parsed.originalCql());
        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
    }
}
