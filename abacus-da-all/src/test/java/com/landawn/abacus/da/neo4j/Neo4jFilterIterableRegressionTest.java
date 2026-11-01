package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.neo4j.ogm.cypher.ComparisonOperator;
import org.neo4j.ogm.cypher.Filter;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

class Neo4jFilterIterableRegressionTest {
    @Test
    void singleUseIterablePreservesPrefixAndSuffixWhenAFilterCannotBeCopied() {
        final Filter first = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter custom = new Filter("age", ComparisonOperator.LESS_THAN, 65) {
        };
        final Filter last = new Filter("active", ComparisonOperator.EQUALS, true);
        final List<Filter> expected = List.of(first, custom, last);

        final List<Filter> actual = passedFilters(singleUse(expected));

        assertEquals(3, actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertSame(expected.get(i), actual.get(i));
        }
    }

    @Test
    void singleUseIterablePreservesNullForDriverValidation() {
        final Filter first = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final List<Filter> expected = Arrays.asList(first, null, new Filter("active", ComparisonOperator.EQUALS, true));

        assertEquals(expected, passedFilters(singleUse(expected)));
    }

    @Test
    void emptySingleUseIterableIsNotOpenedAgain() {
        assertTrue(passedFilters(singleUse(List.of())).isEmpty());
    }

    @Test
    void supportedSingleUseFiltersAreCopiedAndIndexed() {
        final Filter first = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter second = new Filter("age", ComparisonOperator.LESS_THAN, 65);

        final List<Filter> actual = passedFilters(singleUse(List.of(first, second)));

        assertEquals(2, actual.size());
        assertNotSame(first, actual.get(0));
        assertNotSame(second, actual.get(1));
        assertEquals(Map.of("age_0", 18), actual.get(0).parameters());
        assertEquals(Map.of("age_1", 65), actual.get(1).parameters());
        assertEquals(Map.of("age_0", 65), second.parameters());
    }

    private static Iterable<Filter> singleUse(final List<Filter> filters) {
        final AtomicBoolean opened = new AtomicBoolean();
        final Iterator<Filter> iterator = filters.iterator();
        return () -> {
            assertTrue(opened.compareAndSet(false, true), "The input iterable must only be opened once");
            return iterator;
        };
    }

    private static List<Filter> passedFilters(final Iterable<Filter> filters) {
        final SessionFactory factory = mock(SessionFactory.class);
        final Session session = mock(Session.class);
        when(factory.openSession()).thenReturn(session);
        final List<Filter> passed = new ArrayList<>();
        when(session.count(eq(Object.class), any())).thenAnswer(invocation -> {
            final Iterable<Filter> argument = invocation.getArgument(1);
            argument.forEach(passed::add);
            return 7L;
        });

        assertEquals(7L, new Neo4jExecutor(factory).count(Object.class, filters));
        return passed;
    }
}
