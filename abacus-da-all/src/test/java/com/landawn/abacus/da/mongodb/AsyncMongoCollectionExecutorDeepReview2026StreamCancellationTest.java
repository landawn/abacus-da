package com.landawn.abacus.da.mongodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.stream.Stream;

public class AsyncMongoCollectionExecutorDeepReview2026StreamCancellationTest {

    @Test
    public void testStreamingMethodInventoryIncludesAllCancellationSensitiveOverloads() {
        Assertions.assertEquals(19, streamingMethods().size(), streamingMethods()::toString);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("streamingMethods")
    @SuppressWarnings("unchecked")
    public void testDirectExecutionDelegatesEveryStreamingOverloadAndLeavesCompletedStreamOpen(final Method method) throws Exception {
        final Stream<Document> expected = mock(Stream.class);
        final AtomicInteger invocationCount = new AtomicInteger();
        final AtomicReference<Method> delegatedMethod = new AtomicReference<>();
        final AtomicReference<Object[]> delegatedArguments = new AtomicReference<>();
        final MongoCollectionExecutor sync = mock(MongoCollectionExecutor.class, invocation -> {
            invocationCount.incrementAndGet();
            delegatedMethod.set(invocation.getMethod());
            delegatedArguments.set(invocation.getArguments());
            return expected;
        });
        final AsyncMongoCollectionExecutor async = new AsyncMongoCollectionExecutor(sync, new AsyncExecutor(Runnable::run));
        final Object[] arguments = argumentsFor(method);

        final ContinuableFuture<?> future = (ContinuableFuture<?>) method.invoke(async, arguments);

        Assertions.assertSame(expected, future.get());
        Assertions.assertEquals(1, invocationCount.get());
        Assertions.assertEquals(method.getName(), delegatedMethod.get().getName());
        Assertions.assertArrayEquals(method.getParameterTypes(), delegatedMethod.get().getParameterTypes());
        assertSameArguments(arguments, delegatedArguments.get());

        final AtomicReference<Thread> callbackThread = new AtomicReference<>();
        future.thenRunAsync(() -> callbackThread.set(Thread.currentThread())).get();
        Assertions.assertSame(Thread.currentThread(), callbackThread.get());
        Assertions.assertFalse(future.cancel(false));
        verify(expected, never()).close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("streamingMethods")
    @SuppressWarnings("unchecked")
    public void testCancellationDuringEverySyncStreamCreationClosesLateResult(final Method method) throws Exception {
        final CountDownLatch syncCallStarted = new CountDownLatch(1);
        final CountDownLatch allowSyncCallToReturn = new CountDownLatch(1);
        final Stream<Document> expected = mock(Stream.class);
        final MongoCollectionExecutor sync = mock(MongoCollectionExecutor.class, invocation -> {
            syncCallStarted.countDown();

            if (!allowSyncCallToReturn.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to release the synchronous stream call");
            }

            return expected;
        });
        final AtomicReference<Runnable> submitted = new AtomicReference<>();
        final AsyncMongoCollectionExecutor async = new AsyncMongoCollectionExecutor(sync, new AsyncExecutor(submitted::set));
        final ContinuableFuture<?> future = (ContinuableFuture<?>) method.invoke(async, argumentsFor(method));
        final Thread worker = new Thread(submitted.get(), "async-mongo-stream-cancellation-test");
        worker.setDaemon(true);

        try {
            worker.start();
            Assertions.assertTrue(syncCallStarted.await(5, TimeUnit.SECONDS), "Synchronous stream call did not start");
            Assertions.assertTrue(future.cancel(false));
            Assertions.assertTrue(future.isCancelled());
        } finally {
            allowSyncCallToReturn.countDown();
        }

        worker.join(5000);
        Assertions.assertFalse(worker.isAlive(), "Submitted stream task did not finish");
        Assertions.assertThrows(CancellationException.class, future::get);
        verify(expected).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    @SuppressWarnings("unchecked")
    public void testCancellationAfterPublicationClosesUndeliveredStreamExactlyOnce(final boolean mayInterrupt) throws Exception {
        final Stream<Document> expected = mock(Stream.class);
        final Class<?> futureClass = Class.forName(AsyncMongoCollectionExecutor.class.getName() + "$CloseOnCancelFuture");
        final var constructor = futureClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        final Future<Stream<Document>> future = (Future<Stream<Document>>) constructor.newInstance();
        final Method setDelegate = futureClass.getDeclaredMethod("setDelegate", Future.class);
        final Method onResult = futureClass.getDeclaredMethod("onResult", Stream.class);
        setDelegate.setAccessible(true);
        onResult.setAccessible(true);

        // Hold the delegate incomplete after publication, deterministically exercising the window
        // between onResult() and FutureTask completion without relying on thread scheduling.
        final FutureTask<Stream<Document>> delegate = new FutureTask<>(() -> expected);
        setDelegate.invoke(future, delegate);
        onResult.invoke(future, expected);
        Assertions.assertFalse(future.isDone());
        verify(expected, never()).close();

        Assertions.assertTrue(future.cancel(mayInterrupt));
        Assertions.assertTrue(future.isCancelled());
        Assertions.assertTrue(future.isDone());
        Assertions.assertThrows(CancellationException.class, future::get);
        Assertions.assertFalse(future.cancel(mayInterrupt));
        verify(expected).close();
    }

    private static List<Method> streamingMethods() {
        return Arrays.stream(AsyncMongoCollectionExecutor.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> {
                    if (!(method.getGenericReturnType() instanceof final ParameterizedType returnType) || returnType.getRawType() != ContinuableFuture.class) {
                        return false;
                    }

                    final Type resultType = returnType.getActualTypeArguments()[0];

                    if (resultType instanceof final Class<?> resultClass) {
                        return Stream.class.isAssignableFrom(resultClass);
                    }

                    return resultType instanceof final ParameterizedType parameterizedResult
                            && parameterizedResult.getRawType() instanceof final Class<?> resultClass && Stream.class.isAssignableFrom(resultClass);
                })
                .sorted(Comparator.comparing(Method::toGenericString))
                .toList();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFailingCloseDoesNotTurnSuccessfulCancellationIntoAnException() throws Exception {
        final Stream<Document> expected = mock(Stream.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("close failed")).when(expected).close();
        final Class<?> futureClass = Class.forName(AsyncMongoCollectionExecutor.class.getName() + "$CloseOnCancelFuture");
        final var constructor = futureClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        final Future<Stream<Document>> future = (Future<Stream<Document>>) constructor.newInstance();
        final Method setDelegate = futureClass.getDeclaredMethod("setDelegate", Future.class);
        final Method onResult = futureClass.getDeclaredMethod("onResult", Stream.class);
        setDelegate.setAccessible(true);
        onResult.setAccessible(true);

        final FutureTask<Stream<Document>> delegate = new FutureTask<>(() -> expected);
        setDelegate.invoke(future, delegate);
        onResult.invoke(future, expected);

        // A cleanup failure must not mask a cancellation that already succeeded. The delegate never ran,
        // so there is no runner to interrupt and cancel(false) exercises the same cleanup path as cancel(true).
        Assertions.assertTrue(future.cancel(false));
        Assertions.assertTrue(future.isCancelled());
        verify(expected).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFailingCloseOfLateStreamDoesNotEscapeTheCancelledTask() throws Exception {
        final Stream<Document> expected = mock(Stream.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("close failed")).when(expected).close();
        final Class<?> futureClass = Class.forName(AsyncMongoCollectionExecutor.class.getName() + "$CloseOnCancelFuture");
        final var constructor = futureClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        final Future<Stream<Document>> future = (Future<Stream<Document>>) constructor.newInstance();
        final Method setDelegate = futureClass.getDeclaredMethod("setDelegate", Future.class);
        final Method onResult = futureClass.getDeclaredMethod("onResult", Stream.class);
        setDelegate.setAccessible(true);
        onResult.setAccessible(true);

        final FutureTask<Stream<Document>> delegate = new FutureTask<>(() -> expected);
        setDelegate.invoke(future, delegate);
        Assertions.assertTrue(future.cancel(false));

        Assertions.assertDoesNotThrow(() -> onResult.invoke(future, expected));
        verify(expected).close();
    }

    private static Object[] argumentsFor(final Method method) {
        return Arrays.stream(method.getParameterTypes()).map(AsyncMongoCollectionExecutorDeepReview2026StreamCancellationTest::argumentFor).toArray();
    }

    private static Object argumentFor(final Class<?> parameterType) {
        if (parameterType == String.class) {
            return "field";
        } else if (parameterType == int.class) {
            return 0;
        } else if (parameterType == Class.class) {
            return Document.class;
        } else if (Bson.class.isAssignableFrom(parameterType)) {
            return new Document();
        } else if (List.class.isAssignableFrom(parameterType)) {
            return List.of(new Document());
        } else if (Collection.class.isAssignableFrom(parameterType)) {
            return List.of("field");
        }

        throw new AssertionError("Unhandled streaming parameter type: " + parameterType);
    }

    private static void assertSameArguments(final Object[] expected, final Object[] actual) {
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(expected.length, actual.length);

        for (int i = 0; i < expected.length; i++) {
            Assertions.assertSame(expected[i], actual[i], "Argument " + i);
        }
    }
}
