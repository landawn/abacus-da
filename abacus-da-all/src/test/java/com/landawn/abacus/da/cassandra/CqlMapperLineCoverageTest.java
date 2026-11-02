package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.exception.UncheckedException;
import com.landawn.abacus.exception.UncheckedIOException;

class CqlMapperLineCoverageTest extends TestBase {

    @Test
    public void loadFromLogsTheStatementCountWhenDebugIsEnabled() throws ReflectiveOperationException {
        final Runnable restore = enableDebug(CqlMapper.class.getName());
        try {
            assertTrue(CqlMapper.logger.isDebugEnabled());

            final String xml = "<cqlMapper><cql id=\"a\">SELECT 1 FROM t</cql></cqlMapper>";
            final CqlMapper mapper = CqlMapper.loadFrom(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

            assertEquals("SELECT 1 FROM t", mapper.get("a").originalCql());
            assertEquals("SELECT 1 FROM t", mapper.get("a").parameterizedCql());
            assertEquals(0, mapper.get("a").parameterCount());
        } finally {
            restore.run();
        }
        assertEquals(debugWasAlreadyOn, CqlMapper.logger.isDebugEnabled());
    }

    @Test
    public void saveToReportsAFlushFailureThatHappensAfterTheDocumentIsWritten() {
        final CqlMapper mapper = new CqlMapper();
        mapper.add("q", "SELECT 1 FROM t");
        final IOException flushFailure = new IOException("flush failed");
        final OutputStream stream = new ByteArrayOutputStream() {
            @Override
            public void flush() throws IOException {
                if (flushedByCqlMapperWrite()) {
                    throw flushFailure;
                }
            }
        };

        final UncheckedIOException exception = assertThrows(UncheckedIOException.class, () -> mapper.saveTo(stream));

        assertSame(flushFailure, exception.getCause());
        assertEquals("flush failed", exception.getCause().getMessage());
        assertEquals("SELECT 1 FROM t", mapper.get("q").originalCql());
    }

    @Test
    public void anUncheckedIoExceptionIsReturnedUnchanged() throws ReflectiveOperationException {
        final Method ioFailureOrSelf = CqlMapper.class.getDeclaredMethod("ioFailureOrSelf", UncheckedException.class);
        ioFailureOrSelf.setAccessible(true);
        final UncheckedIOException failure = new UncheckedIOException(new IOException("x"));

        assertSame(failure, ioFailureOrSelf.invoke(null, failure));
    }

    @Test
    public void aNullAttributeValueNeedsNoXmlCharacterCheck() throws ReflectiveOperationException {
        final Method checkXmlCharacters = CqlMapper.class.getDeclaredMethod("checkXmlCharacters", String.class, String.class);
        checkXmlCharacters.setAccessible(true);

        assertNull(checkXmlCharacters.invoke(null, new Object[] { null, "Attribute 'timeout' for 'q'" }));
    }

    /** The immediate caller of this stream's {@code flush} is {@code CqlMapper.write}. */
    private static boolean flushedByCqlMapperWrite() {
        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();

        for (int i = 0; i < stack.length; i++) {
            final StackTraceElement frame = stack[i];

            if ("flush".equals(frame.getMethodName()) && frame.getClassName().contains("CqlMapperLineCoverageTest")) {
                if (i + 1 >= stack.length) {
                    return false;
                }

                final StackTraceElement caller = stack[i + 1];
                return CqlMapper.class.getName().equals(caller.getClassName()) && "write".equals(caller.getMethodName());
            }
        }

        return false;
    }

    /** True when this JVM already had debug on before the test changed any level. */
    private static boolean debugWasAlreadyOn;

    /**
     * Turns debug on for whichever logging backend {@code LoggerFactory} selected, and returns a
     * restore action. Parent loggers are included only when the named logger's own level is not
     * enough, and every level this method changes is put back so later tests keep the default.
     */
    private static Runnable enableDebug(final String loggerName) throws ReflectiveOperationException {
        debugWasAlreadyOn = CqlMapper.logger.isDebugEnabled();
        if (debugWasAlreadyOn) {
            return () -> {
            };
        }

        final String backend = CqlMapper.logger.getClass().getName();

        if (backend.endsWith("JdkLogger")) {
            final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(loggerName);
            final List<java.util.logging.Logger> changed = new ArrayList<>();
            final List<java.util.logging.Level> previous = new ArrayList<>();
            java.util.logging.Logger current = jul;

            while (current != null && !CqlMapper.logger.isDebugEnabled()) {
                changed.add(current);
                previous.add(current.getLevel());
                current.setLevel(java.util.logging.Level.ALL);
                current = current.getParent();
            }

            return () -> {
                for (int i = changed.size() - 1; i >= 0; i--) {
                    changed.get(i).setLevel(previous.get(i));
                }
            };
        }

        final Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
        final Object debug = levelClass.getField("DEBUG").get(null);
        final Class<?> configurator = Class.forName("org.apache.logging.log4j.core.config.Configurator");
        final Class<?> loggerContextClass = Class.forName("org.apache.logging.log4j.core.LoggerContext");
        final Object context = Class.forName("org.apache.logging.log4j.LogManager").getMethod("getContext", boolean.class).invoke(null, false);
        final Object config = loggerContextClass.getMethod("getConfiguration").invoke(context);
        final Object previousNamed = config.getClass().getMethod("getLoggerConfig", String.class).invoke(config, loggerName);
        final Object previousNamedLevel = previousNamed.getClass().getMethod("getLevel").invoke(previousNamed);
        configurator.getMethod("setLevel", String.class, levelClass).invoke(null, loggerName, debug);

        final boolean raisedRoot = !CqlMapper.logger.isDebugEnabled();
        final Object previousRootLevel;
        if (raisedRoot) {
            final Object root = Class.forName("org.apache.logging.log4j.LogManager").getMethod("getRootLogger").invoke(null);
            previousRootLevel = root.getClass().getMethod("getLevel").invoke(root);
            configurator.getMethod("setRootLevel", levelClass).invoke(null, debug);
        } else {
            previousRootLevel = null;
        }

        return () -> {
            try {
                configurator.getMethod("setLevel", String.class, levelClass).invoke(null, loggerName, previousNamedLevel);
                if (raisedRoot) {
                    configurator.getMethod("setRootLevel", levelClass).invoke(null, previousRootLevel);
                }
            } catch (final ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        };
    }
}
