package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.landawn.abacus.exception.UncheckedException;
import com.landawn.abacus.exception.UncheckedIOException;

class CqlMapperIoTest {

    @TempDir
    Path directory;

    @Test
    void delimitedFilePathsLoadAllMappings() throws Exception {
        final Path first = directory.resolve("first.xml");
        final Path second = directory.resolve("second.xml");
        Files.writeString(first, "<cqlMapper><cql id='first'>SELECT * FROM first_table</cql></cqlMapper>");
        Files.writeString(second, "<cqlMapper><cql id='second'>SELECT * FROM second_table</cql></cqlMapper>");
        for (final String separator : new String[] { ",", ";" }) {
            final CqlMapper result = CqlMapper.loadFrom(first + separator + second);
            assertEquals(Set.of("first", "second"), result.ids());
            assertEquals("SELECT * FROM first_table", result.get("first").originalCql());
            assertEquals("SELECT * FROM second_table", result.get("second").originalCql());
        }
    }

    @Test
    void streamReadFailureRetainsCauseAndCallerOwnership() {
        final IOException failure = new IOException("read failed");
        final AtomicBoolean closed = new AtomicBoolean();
        final InputStream input = new InputStream() {
            @Override
            public int read() throws IOException {
                throw failure;
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
        assertSame(failure, assertThrows(UncheckedIOException.class, () -> CqlMapper.loadFrom(input)).getCause());
        assertFalse(closed.get());
    }

    @Test
    void missingIdsAndUnreadableFilesFailExplicitly() {
        final byte[] xml = "<cqlMapper><cql>SELECT * FROM items</cql></cqlMapper>".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.loadFrom(new ByteArrayInputStream(xml)));
        assertThrows(UncheckedIOException.class, () -> CqlMapper.loadFrom(directory.resolve("missing.xml").toFile()));
    }

    @Test
    void blockedParentDirectoriesDoNotOverwriteExistingFiles() throws Exception {
        final Path blocked = directory.resolve("blocked");
        Files.writeString(blocked, "existing data");
        final CqlMapper mapper = new CqlMapper();
        mapper.add("find", "SELECT * FROM items");
        assertThrows(UncheckedIOException.class, () -> mapper.saveTo(blocked.resolve("nested/out.xml").toFile()));
        assertEquals("existing data", Files.readString(blocked));
    }

    @Test
    void saveFailureRetainsCauseAndDoesNotCloseCallerStream() {
        final IOException failure = new IOException("write failed");
        final AtomicBoolean closed = new AtomicBoolean();
        final OutputStream output = new OutputStream() {
            @Override
            public void write(final int value) throws IOException {
                throw failure;
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
        final CqlMapper mapper = new CqlMapper();
        mapper.add("find", "SELECT * FROM items");
        Throwable cause = assertThrows(UncheckedException.class, () -> mapper.saveTo(output));
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertSame(failure, cause);
        assertFalse(closed.get());
    }

    @Test
    void aReservedIdAttributeCannotReplaceTheMappingIdDuringSave() {
        final CqlMapper mapper = new CqlMapper();
        mapper.add("find", "SELECT * FROM items", Map.of("id", "wrong", "note", "a\tb\nc\rd 😀"));
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        mapper.saveTo(output);
        final CqlMapper restored = CqlMapper.loadFrom(new ByteArrayInputStream(output.toByteArray()));
        assertEquals(Set.of("find"), restored.ids());
        assertEquals("a\tb\nc\rd 😀", restored.getAttributes("find").get("note"));
    }
}
