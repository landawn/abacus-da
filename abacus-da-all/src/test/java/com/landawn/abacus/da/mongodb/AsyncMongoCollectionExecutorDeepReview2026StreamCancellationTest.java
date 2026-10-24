package com.landawn.abacus.da.mongodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;

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

    // ---- 2026-10-19 sliceR ----

    @Test
    public void testCancelWithInterruptClosesLateStreamWithInterruptClearedAndThenRestoresIt() throws Exception {
        final CountDownLatch syncCallStarted = new CountDownLatch(1);
        final CountDownLatch allowSyncCallToReturn = new CountDownLatch(1);
        final AtomicReference<Boolean> interruptedDuringClose = new AtomicReference<>();
        final AtomicReference<Boolean> interruptedAfterTask = new AtomicReference<>();
        final Stream<Document> late = Stream.of(List.of(new Document())).onClose(() -> interruptedDuringClose.set(Thread.currentThread().isInterrupted()));
        final MongoCollectionExecutor sync = mock(MongoCollectionExecutor.class, invocation -> {
            syncCallStarted.countDown();
            boolean interrupted = false;

            while (true) {
                try {
                    if (!allowSyncCallToReturn.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting to release the synchronous stream call");
                    }

                    break;
                } catch (final InterruptedException e) {
                    interrupted = true;
                }
            }

            // Like a driver call whose cursor was already open when cancel(true) arrived: the result is
            // still produced and the worker's interrupt flag is still set.
            if (interrupted) {
                Thread.currentThread().interrupt();
            }

            return late;
        });
        final AtomicReference<Runnable> submitted = new AtomicReference<>();
        final AsyncMongoCollectionExecutor async = new AsyncMongoCollectionExecutor(sync, new AsyncExecutor(submitted::set));
        final ContinuableFuture<Stream<Document>> future = async.stream(new Document());
        final Thread worker = new Thread(() -> {
            submitted.get().run();
            interruptedAfterTask.set(Thread.currentThread().isInterrupted());
        }, "async-mongo-stream-interrupt-test");
        worker.setDaemon(true);
        final boolean cancelled;

        try {
            worker.start();
            Assertions.assertTrue(syncCallStarted.await(5, TimeUnit.SECONDS), "Synchronous stream call did not start");
            cancelled = future.cancel(true);
        } finally {
            allowSyncCallToReturn.countDown();
        }

        worker.join(5000);
        Assertions.assertFalse(worker.isAlive(), "Submitted stream task did not finish");
        Assertions.assertAll(() -> Assertions.assertTrue(cancelled), () -> Assertions.assertThrows(CancellationException.class, future::get),
                // An interrupted MongoDB cursor close returns normally without sending killCursors (server cursor leaks).
                () -> Assertions.assertEquals(Boolean.FALSE, interruptedDuringClose.get(), "late stream must be closed with the interrupt cleared"),
                () -> Assertions.assertEquals(Boolean.TRUE, interruptedAfterTask.get(), "the cancellation interrupt must be restored after close"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCancelFromInterruptedThreadClosesPublishedStreamWithInterruptClearedAndThenRestoresIt() throws Exception {
        final AtomicReference<Boolean> interruptedDuringClose = new AtomicReference<>();
        final Stream<Document> published = Stream.of(List.of(new Document())).onClose(() -> interruptedDuringClose.set(Thread.currentThread().isInterrupted()));
        final Class<?> futureClass = Class.forName(AsyncMongoCollectionExecutor.class.getName() + "$CloseOnCancelFuture");
        final var constructor = futureClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        final Future<Stream<Document>> future = (Future<Stream<Document>>) constructor.newInstance();
        final Method setDelegate = futureClass.getDeclaredMethod("setDelegate", Future.class);
        final Method onResult = futureClass.getDeclaredMethod("onResult", Stream.class);
        setDelegate.setAccessible(true);
        onResult.setAccessible(true);
        setDelegate.invoke(future, new FutureTask<>(() -> published));
        onResult.invoke(future, published);

        final boolean cancelled;
        final boolean interruptedAfterCancel;

        Thread.currentThread().interrupt();

        try {
            cancelled = future.cancel(false);
        } finally {
            interruptedAfterCancel = Thread.interrupted();
        }

        Assertions.assertAll(() -> Assertions.assertTrue(cancelled), () -> Assertions.assertEquals(Boolean.FALSE, interruptedDuringClose.get()),
                () -> Assertions.assertTrue(interruptedAfterCancel, "the caller's interrupt status must be preserved"));
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

    // ---- 2026-10-24 sliceR ----

    @Test
    public void testSharedDefaultExecutorShutdownFailsStreamCallBeforeTheQueryIsSubmitted(@TempDir final Path tempDir) throws Exception {
        // A separate JVM: the probe shuts down abacus-common's shared default executor, which cannot be undone.
        final Path output = tempDir.resolve("shared-executor-probe.txt");
        final Path arguments = tempDir.resolve("shared-executor-probe.args");
        final String executable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        // The test classpath approaches the Windows command-line limit, so pass it in a launcher argument file. Quoted
        // argument-file values treat backslashes as escapes, and the launcher reads the file in the native encoding.
        Files.writeString(arguments, "-cp\n\"" + classPath.replace("\\", "\\\\") + "\"\n", Charset.forName(System.getProperty("native.encoding")));
        final Process process = new ProcessBuilder(executable, "-Xmx256m", "-XX:+UseSerialGC", "@" + arguments, SharedExecutorShutdownProbe.class.getName())
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();

        try {
            Assertions.assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Forked shared-executor probe timed out");
            Assertions.assertEquals(0, process.exitValue(), () -> {
                try {
                    return Files.readString(output);
                } catch (final java.io.IOException e) {
                    return e.toString();
                }
            });
        } finally {
            process.destroyForcibly();
        }
    }

    public static final class SharedExecutorShutdownProbe {
        public static void main(final String[] args) throws Exception {
            final AtomicInteger findCalls = new AtomicInteger();
            final MongoCollection<Document> collection = driverProxy(MongoCollection.class, findCalls);
            // The caller's own executor stays usable; only the shared default one is gone, as during JVM shutdown.
            final AsyncMongoCollectionExecutor async = new MongoCollectionExecutor(collection, new AsyncExecutor(Runnable::run)).async();
            final Field sharedExecutor = N.class.getDeclaredField("ASYNC_EXECUTOR");
            sharedExecutor.setAccessible(true);
            ((AsyncExecutor) sharedExecutor.get(null)).shutdown(); // what that pool's JVM shutdown hook does

            Assertions.assertThrows(IllegalStateException.class, () -> async.stream(new Document()));
            // The failure used to be raised after the inline task had already run the find (opening a cursor that no
            // future could cancel or close).
            Assertions.assertEquals(0, findCalls.get(), "the query ran although the stream call failed");
        }

        @SuppressWarnings("unchecked")
        private static <T> T driverProxy(final Class<T> type, final AtomicInteger findCalls) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, (proxy, method, methodArgs) -> {
                switch (method.getName()) {
                    case "find" -> findCalls.incrementAndGet();
                    case "getNamespace" -> {
                        return new MongoNamespace("probe.c");
                    }
                    case "hashCode" -> {
                        return System.identityHashCode(proxy);
                    }
                    case "equals" -> {
                        return proxy == methodArgs[0];
                    }
                    case "toString" -> {
                        return type.getSimpleName() + " probe";
                    }
                    default -> {
                    }
                }

                final Class<?> returnType = method.getReturnType();

                return returnType.isInterface() ? driverProxy(returnType, findCalls) : returnType == boolean.class ? Boolean.FALSE : null;
            });
        }
    }
}
