package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.transaction.Transaction;

public class Neo4jExecutorDeepReview2026TransactionCleanupTest {

    private SessionFactory sessionFactory;
    private Session session;
    private Transaction transaction;
    private Neo4jExecutor executor;

    @BeforeEach
    public void setUp() {
        sessionFactory = mock(SessionFactory.class);
        session = mock(Session.class);
        transaction = mock(Transaction.class);
        when(sessionFactory.openSession()).thenReturn(session);
        when(session.getTransaction()).thenReturn(transaction);
        executor = new Neo4jExecutor(sessionFactory);
    }

    @Test
    public void testRollbackFailureAttemptsCloseAndDiscardsSession() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        doThrow(new RuntimeException("rollback failed")).when(transaction).rollback();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.run(s -> {
            throw actionFailure;
        }));

        assertSame(actionFailure, thrown);
        verify(transaction).close();
        verify(session, never()).clear();

        executor.run(s -> {
        });
        verify(sessionFactory, times(2)).openSession();
    }

    @Test
    public void testRollbackAndCloseFailuresDoNotMaskActionFailure() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        doThrow(new RuntimeException("rollback failed")).when(transaction).rollback();
        doThrow(new RuntimeException("close failed")).when(transaction).close();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.run(s -> {
            throw actionFailure;
        }));

        assertSame(actionFailure, thrown);
        verify(transaction).close();
        verify(session, never()).clear();
    }

    @Test
    public void testSameRollbackAndCloseFailureDoesNotMaskActionFailure() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException cleanupFailure = new RuntimeException("cleanup failed");
        doThrow(cleanupFailure).when(transaction).rollback();
        doThrow(cleanupFailure).when(transaction).close();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.run(s -> {
            throw actionFailure;
        }));

        assertSame(actionFailure, thrown);
        verify(transaction).close();
        verify(session, never()).clear();
    }

    @Test
    public void testRollbackFailureDoesNotCloseCommitPendingTransaction() {
        when(transaction.status()).thenReturn(Transaction.Status.COMMIT_PENDING);
        doThrow(new RuntimeException("rollback failed")).when(transaction).rollback();

        executor.run(s -> {
        });

        verify(transaction, never()).close();
        verify(session, never()).clear();
    }
}
