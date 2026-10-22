package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.ogm.context.MappingContext;
import org.neo4j.ogm.driver.Driver;
import org.neo4j.ogm.exception.core.TransactionManagerException;
import org.neo4j.ogm.session.Neo4jSession;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.session.transaction.DefaultTransactionManager;
import org.neo4j.ogm.transaction.AbstractTransaction;
import org.neo4j.ogm.transaction.Transaction;
import org.neo4j.ogm.transaction.TransactionManager;

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
        // Discarded, but cleared first so its mapping context is released (see closeSession).
        verify(session).clear();

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
        verify(session).clear();
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
        verify(session).clear();
    }

    @Test
    public void testRollbackFailureDoesNotCloseCommitPendingTransaction() {
        when(transaction.status()).thenReturn(Transaction.Status.COMMIT_PENDING);
        doThrow(new RuntimeException("rollback failed")).when(transaction).rollback();

        executor.run(s -> {
        });

        verify(transaction, never()).close();
        verify(session).clear();
    }

    // ---- 2026-10-19 sliceW ----

    @Test
    public void testRollbackFailure_clearFailureDoesNotMaskActionFailureAndSessionIsStillDiscarded() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        doThrow(new RuntimeException("rollback failed")).when(transaction).rollback();
        doThrow(new RuntimeException("clear failed")).when(session).clear();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.run(s -> {
            throw actionFailure;
        }));

        when(session.getTransaction()).thenReturn(null);
        executor.run(s -> {
        });

        assertAll(() -> assertSame(actionFailure, thrown), () -> verify(session, times(2)).clear(),
                // the session whose rollback failed was not pooled: the second run opened a new one
                () -> verify(sessionFactory, times(2)).openSession());
    }

    @Test
    public void testRollbackFailure_closeFailureAfterSuccessfulRollbackStillClearsAndDiscards() {
        doThrow(new RuntimeException("close failed")).when(transaction).close();

        executor.run(s -> {
        });

        when(session.getTransaction()).thenReturn(null);
        executor.run(s -> {
        });

        assertAll(() -> verify(transaction).rollback(), () -> verify(session, times(2)).clear(), () -> verify(sessionFactory, times(2)).openSession());
    }

    // ---- 2026-10-20 hbnfix ----

    private static final class CleanupError extends Error {
        private static final long serialVersionUID = 1L;

        CleanupError(final String message) {
            super(message);
        }
    }

    @Test
    public void testRollbackFailure_errorFromClearCarriesTheRollbackFailureAsSuppressed() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        doThrow(new CleanupError("clear blew up")).when(session).clear();

        // releaseDiscardedSession lets an Error through on purpose; before the fix it escaped before the warn log ran
        // and carried nothing about the rollback failure that had triggered the clean-up.
        final CleanupError thrown = assertThrows(CleanupError.class, () -> executor.run(s -> {
        }));

        assertAll(() -> assertEquals(List.of(rollbackFailure), Arrays.asList(thrown.getSuppressed())), () -> verify(transaction).close(),
                () -> verify(session).clear());
    }

    @Test
    public void testRollbackFailure_clearFailureIsSuppressedOnTheRollbackFailure() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        final RuntimeException clearFailure = new RuntimeException("clear failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        doThrow(clearFailure).when(session).clear();

        executor.run(s -> {
        });

        // the logged rollback failure carries the secondary failure instead of being replaced by it
        assertEquals(List.of(clearFailure), Arrays.asList(rollbackFailure.getSuppressed()));
    }

    private Neo4jSession neo4jSessionWithTransactionManager(final TransactionManager txManager) {
        final Neo4jSession neo4jSession = mock(Neo4jSession.class);
        when(neo4jSession.getTransactionManager()).thenReturn(txManager);
        when(neo4jSession.getTransaction()).thenReturn(transaction);
        when(sessionFactory.openSession()).thenReturn(neo4jSession);
        return neo4jSession;
    }

    @Test
    public void testRollbackFailure_detachFailureIsSuppressedAndSessionIsStillCleared() {
        final TransactionManager txManager = mock(TransactionManager.class);
        final Neo4jSession neo4jSession = neo4jSessionWithTransactionManager(txManager);
        when(txManager.getCurrentTransaction()).thenReturn(transaction);
        final TransactionManagerException detachFailure = new TransactionManagerException("Transaction is not current for this thread");
        doThrow(detachFailure).when(txManager).close(eq(transaction), any());
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();

        executor.run(s -> {
        });

        assertAll(() -> assertEquals(List.of(detachFailure), Arrays.asList(rollbackFailure.getSuppressed())),
                () -> verify(txManager).close(eq(transaction), any()), () -> verify(neo4jSession).clear());
    }

    @Test
    public void testRollbackFailure_nonCurrentTransactionIsNotDetachedButSessionIsStillCleared() {
        final TransactionManager txManager = mock(TransactionManager.class);
        final Neo4jSession neo4jSession = neo4jSessionWithTransactionManager(txManager);
        // another transaction is current (e.g. the close() retry already deregistered ours): close(tx, cb) would throw
        when(txManager.getCurrentTransaction()).thenReturn(mock(Transaction.class));
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();

        executor.run(s -> {
        });

        assertAll(() -> verify(txManager, never()).close(any(), any()), () -> verify(neo4jSession).clear(),
                () -> assertEquals(0, rollbackFailure.getSuppressed().length));
    }

    /** Real OGM transaction whose native rollback can be made to fail. */
    private static final class FailingRollbackTx extends AbstractTransaction {
        private final AtomicInteger rollback0Calls;
        private final AtomicInteger commit0Calls;

        FailingRollbackTx(final TransactionManager manager, final AtomicInteger rollback0Calls, final AtomicInteger commit0Calls) {
            super(manager);
            this.rollback0Calls = rollback0Calls;
            this.commit0Calls = commit0Calls;
        }

        @Override
        protected void rollback0() {
            rollback0Calls.incrementAndGet();
            throw new RuntimeException("native rollback failed");
        }

        @Override
        protected void commit0() {
            commit0Calls.incrementAndGet();
        }
    }

    private static DefaultTransactionManager realTransactionManager(final Neo4jSession neo4jSession, final AtomicInteger rollback0Calls,
            final AtomicInteger commit0Calls) {
        final Driver driver = mock(Driver.class);
        when(driver.getTransactionFactorySupplier()).thenReturn(m -> (type, bookmarks) -> new FailingRollbackTx(m, rollback0Calls, commit0Calls));
        final DefaultTransactionManager txManager = new DefaultTransactionManager(driver, neo4jSession);
        when(neo4jSession.context()).thenReturn(mock(MappingContext.class));
        when(neo4jSession.getTransactionManager()).thenReturn(txManager);
        when(neo4jSession.getTransaction()).thenAnswer(i -> txManager.getCurrentTransaction());
        when(neo4jSession.beginTransaction()).thenAnswer(i -> txManager.openTransaction());
        return txManager;
    }

    @Test
    public void testRollbackFailure_realOgmTransactionIsDetachedFromThreadLocalAndNothingIsCommitted() {
        // OGM's DefaultTransactionManager only removes the transaction from its ThreadLocal after the
        // native rollback succeeded, so a failed rollback used to leave tx -> manager -> ThreadLocal
        // pinning the discarded session for the thread's lifetime.
        final Neo4jSession neo4jSession = mock(Neo4jSession.class);
        final AtomicInteger rollback0Calls = new AtomicInteger();
        final AtomicInteger commit0Calls = new AtomicInteger();
        final DefaultTransactionManager txManager = realTransactionManager(neo4jSession, rollback0Calls, commit0Calls);
        final SessionFactory factory = mock(SessionFactory.class);
        when(factory.openSession()).thenReturn(neo4jSession);
        final Neo4jExecutor neo4jExecutor = new Neo4jExecutor(factory);

        neo4jExecutor.run(s -> s.beginTransaction());

        assertAll(() -> assertNull(txManager.getCurrentTransaction(), "stale transaction must be detached from the manager's ThreadLocal"),
                // rollback() + the close() retry; the detach itself must not touch the native transaction
                () -> assertEquals(2, rollback0Calls.get()), () -> assertEquals(0, commit0Calls.get()), () -> verify(neo4jSession).clear());

        neo4jExecutor.run(s -> {
        });
        verify(factory, times(2)).openSession();
    }

    @Test
    public void testRollbackFailure_realOgmCommitPendingTransactionIsDetachedWithoutCommit() {
        final Neo4jSession neo4jSession = mock(Neo4jSession.class);
        final AtomicInteger rollback0Calls = new AtomicInteger();
        final AtomicInteger commit0Calls = new AtomicInteger();
        final DefaultTransactionManager txManager = realTransactionManager(neo4jSession, rollback0Calls, commit0Calls);
        final SessionFactory factory = mock(SessionFactory.class);
        when(factory.openSession()).thenReturn(neo4jSession);
        final Neo4jExecutor neo4jExecutor = new Neo4jExecutor(factory);

        neo4jExecutor.run(s -> {
            s.beginTransaction();
            final Transaction inner = s.beginTransaction();
            inner.commit(); // root becomes COMMIT_PENDING
            inner.close();
        });

        assertAll(() -> assertNull(txManager.getCurrentTransaction()), () -> assertEquals(1, rollback0Calls.get()), () -> assertEquals(0, commit0Calls.get()),
                () -> verify(neo4jSession).clear());
    }

    // ---- 2026-10-20 sliceF ----

    /**
     * Pins the other Error path of releaseDiscardedSession: an Error thrown by the detach ({@code TransactionManager.close(tx, cb)})
     * is deliberately not caught, so it leaves the release before {@code clear()} runs and propagates out of the session return
     * with the rollback failure that triggered the clean-up attached as suppressed (the Error-from-clear path is pinned above).
     */
    @Test
    public void testRollbackFailure_errorFromDetachPropagatesWithRollbackFailureSuppressedAndSkipsClear() {
        final TransactionManager txManager = mock(TransactionManager.class);
        final Neo4jSession neo4jSession = neo4jSessionWithTransactionManager(txManager);
        when(txManager.getCurrentTransaction()).thenReturn(transaction);
        doThrow(new CleanupError("detach blew up")).when(txManager).close(eq(transaction), any());
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();

        final CleanupError thrown = assertThrows(CleanupError.class, () -> executor.run(s -> {
        }));

        assertAll(() -> assertEquals(List.of(rollbackFailure), Arrays.asList(thrown.getSuppressed())), () -> verify(txManager).close(eq(transaction), any()),
                () -> verify(transaction).close(),
                // the Error left the release before step 2: the mapping context is not cleared on this path
                () -> verify(neo4jSession, never()).clear());
    }

    // ---- 2026-10-21 fixN: the action's exception always wins over an Error thrown by the session return ----

    /**
     * Before the fix every borrowing method returned its session from a plain {@code finally} block, so an Error escaping
     * the discard path (see {@link #testRollbackFailure_errorFromClearCarriesTheRollbackFailureAsSuppressed}) replaced the
     * exception the action had thrown; the Error carried the rollback failure, but the action's exception was lost.
     */
    @Test
    public void testActionFailure_run_errorFromClearDuringDiscardIsSuppressedOnTheActionFailure() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        final CleanupError clearError = new CleanupError("clear blew up");
        doThrow(clearError).when(session).clear();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.run(s -> {
            throw actionFailure;
        }));

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(List.of(clearError), Arrays.asList(thrown.getSuppressed())),
                // the Error still carries the rollback failure that triggered the discard
                () -> assertEquals(List.of(rollbackFailure), Arrays.asList(clearError.getSuppressed())), () -> verify(transaction).close(),
                () -> verify(session).clear());
    }

    @Test
    public void testActionFailure_call_errorFromClearDuringDiscardIsSuppressedOnTheActionFailure() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        final CleanupError clearError = new CleanupError("clear blew up");
        doThrow(clearError).when(session).clear();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.call(s -> {
            throw actionFailure;
        }));

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(List.of(clearError), Arrays.asList(thrown.getSuppressed())),
                () -> assertEquals(List.of(rollbackFailure), Arrays.asList(clearError.getSuppressed())), () -> verify(session).clear());
    }

    @Test
    public void testActionFailure_load_errorFromClearDuringDiscardIsSuppressedOnTheActionFailure() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        final CleanupError clearError = new CleanupError("clear blew up");
        doThrow(clearError).when(session).clear();
        final RuntimeException actionFailure = new RuntimeException("load failed");
        when(session.load(Object.class, 1L)).thenThrow(actionFailure);

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.load(Object.class, 1L));

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(List.of(clearError), Arrays.asList(thrown.getSuppressed())),
                () -> assertEquals(List.of(rollbackFailure), Arrays.asList(clearError.getSuppressed())), () -> verify(session).clear());
    }

    @Test
    public void testActionFailure_run_errorFromClearWithoutLeftoverTransactionIsSuppressedOnTheActionFailure() {
        // no transaction to roll back: clear() is reached directly and its Error used to escape the finally block as-is
        when(session.getTransaction()).thenReturn(null);
        final CleanupError clearError = new CleanupError("clear blew up");
        doThrow(clearError).when(session).clear();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.run(s -> {
            throw actionFailure;
        }));

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(List.of(clearError), Arrays.asList(thrown.getSuppressed())),
                () -> assertEquals(0, clearError.getSuppressed().length));
    }

    @Test
    public void testActionFailure_run_errorFromDetachIsSuppressedOnTheActionFailure() {
        final TransactionManager txManager = mock(TransactionManager.class);
        final Neo4jSession neo4jSession = neo4jSessionWithTransactionManager(txManager);
        when(txManager.getCurrentTransaction()).thenReturn(transaction);
        final CleanupError detachError = new CleanupError("detach blew up");
        doThrow(detachError).when(txManager).close(eq(transaction), any());
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.run(s -> {
            throw actionFailure;
        }));

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(List.of(detachError), Arrays.asList(thrown.getSuppressed())),
                () -> assertEquals(List.of(rollbackFailure), Arrays.asList(detachError.getSuppressed())), () -> verify(txManager).close(eq(transaction), any()),
                // unchanged: the Error leaves the release before clear() (see the sliceF test above)
                () -> verify(neo4jSession, never()).clear());
    }

    @Test
    public void testActionFailure_call_errorFromDetachIsSuppressedOnTheActionFailure() {
        final TransactionManager txManager = mock(TransactionManager.class);
        final Neo4jSession neo4jSession = neo4jSessionWithTransactionManager(txManager);
        when(txManager.getCurrentTransaction()).thenReturn(transaction);
        final CleanupError detachError = new CleanupError("detach blew up");
        doThrow(detachError).when(txManager).close(eq(transaction), any());
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.call(s -> {
            throw actionFailure;
        }));

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(List.of(detachError), Arrays.asList(thrown.getSuppressed())),
                () -> assertEquals(List.of(rollbackFailure), Arrays.asList(detachError.getSuppressed())), () -> verify(neo4jSession, never()).clear());
    }

    @Test
    public void testActionFailure_realOgmTransactionLeftOpenByFailingAction_errorFromClearIsSuppressedAndTransactionIsDetached() {
        final Neo4jSession neo4jSession = mock(Neo4jSession.class);
        final AtomicInteger rollback0Calls = new AtomicInteger();
        final AtomicInteger commit0Calls = new AtomicInteger();
        final DefaultTransactionManager txManager = realTransactionManager(neo4jSession, rollback0Calls, commit0Calls);
        final CleanupError clearError = new CleanupError("clear blew up");
        doThrow(clearError).when(neo4jSession).clear();
        final SessionFactory factory = mock(SessionFactory.class);
        when(factory.openSession()).thenReturn(neo4jSession);
        final Neo4jExecutor neo4jExecutor = new Neo4jExecutor(factory);
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> neo4jExecutor.run(s -> {
            s.beginTransaction();
            throw actionFailure;
        }));

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(List.of(clearError), Arrays.asList(thrown.getSuppressed())),
                () -> assertEquals(1, clearError.getSuppressed().length),
                () -> assertEquals("native rollback failed", clearError.getSuppressed()[0].getMessage()),
                () -> assertNull(txManager.getCurrentTransaction(), "stale transaction must still be detached"), () -> assertEquals(2, rollback0Calls.get()),
                () -> assertEquals(0, commit0Calls.get()));
    }

    // (c) unchanged: with a successful action a clean-up Error propagates (run is pinned by the hbnfix/sliceF tests above)

    @Test
    public void testSuccessfulAction_call_errorFromClearDuringDiscardStillPropagates() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        doThrow(new CleanupError("clear blew up")).when(session).clear();

        final CleanupError thrown = assertThrows(CleanupError.class, () -> executor.call(s -> "ok"));

        assertAll(() -> assertEquals(List.of(rollbackFailure), Arrays.asList(thrown.getSuppressed())), () -> verify(session).clear());
    }

    @Test
    public void testSuccessfulAction_load_errorFromClearDuringDiscardStillPropagates() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        doThrow(new CleanupError("clear blew up")).when(session).clear();
        when(session.load(Object.class, 1L)).thenReturn("loaded");

        final CleanupError thrown = assertThrows(CleanupError.class, () -> executor.load(Object.class, 1L));

        assertAll(() -> assertEquals(List.of(rollbackFailure), Arrays.asList(thrown.getSuppressed())), () -> verify(session).clear());
    }

    @Test
    public void testSuccessfulAction_call_returnsTheActionResultWhenTheSessionIsPooled() {
        when(session.getTransaction()).thenReturn(null);

        assertEquals("ok", executor.call(s -> "ok"));

        executor.run(s -> {
        });
        // the first session was cleared and pooled, so the second borrow reused it
        assertAll(() -> verify(sessionFactory, times(1)).openSession(), () -> verify(session, times(2)).clear());
    }

    // (d) unchanged: a RuntimeException from the clean-up is only logged, never attached to or replacing the action failure

    @Test
    public void testActionFailure_call_runtimeExceptionFromClearIsOnlyLogged() {
        when(transaction.status()).thenReturn(Transaction.Status.OPEN);
        final RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        doThrow(rollbackFailure).when(transaction).rollback();
        final RuntimeException clearFailure = new RuntimeException("clear failed");
        doThrow(clearFailure).when(session).clear();
        final RuntimeException actionFailure = new RuntimeException("action failed");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.call(s -> {
            throw actionFailure;
        }));

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(0, thrown.getSuppressed().length),
                // the clear failure is attached to the logged rollback failure, as before
                () -> assertEquals(List.of(clearFailure), Arrays.asList(rollbackFailure.getSuppressed())), () -> verify(session).clear());
    }

    @Test
    public void testActionFailure_load_runtimeExceptionFromClearIsOnlyLogged() {
        when(session.getTransaction()).thenReturn(null);
        doThrow(new RuntimeException("clear failed")).when(session).clear();
        final RuntimeException actionFailure = new RuntimeException("load failed");
        when(session.load(Object.class, 1L)).thenThrow(actionFailure);

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.load(Object.class, 1L));

        executor.run(s -> {
        });

        assertAll(() -> assertSame(actionFailure, thrown), () -> assertEquals(0, thrown.getSuppressed().length),
                // the session whose clear failed was discarded: the second borrow opened a new one
                () -> verify(sessionFactory, times(2)).openSession());
    }
}
