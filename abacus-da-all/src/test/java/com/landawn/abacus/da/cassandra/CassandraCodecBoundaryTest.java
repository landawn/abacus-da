package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.landawn.abacus.da.cassandra.CassandraExecutor.UDTCodec;

class CassandraCodecBoundaryTest {

    @SuppressWarnings({ "deprecation", "rawtypes" })
    @Test
    void legacyBinaryMethodsRemainCompatibleWithTheDriverCodec() {
        final UserDefinedType type = userType();
        final UDTCodec<Map> codec = UDTCodec.create(type, Map.class);
        final Map<String, String> value = Map.of("name", "O'Brien");
        final ByteBuffer encoded = codec.serialize(value, ProtocolVersion.V4);
        assertEquals(value, codec.decode(encoded.duplicate(), ProtocolVersion.V4));
        assertEquals(value, codec.deserialize(codec.encode(value, ProtocolVersion.V4), ProtocolVersion.V4));
        assertNull(codec.serialize(null, ProtocolVersion.V4));
        assertNull(codec.deserialize(null, ProtocolVersion.V4));
        assertEquals(GenericType.of(Map.class), codec.getJavaType());
        assertSame(type, codec.getCqlType());
        assertEquals(value, codec.parse(codec.format(value)));
        assertNull(codec.parse(""));
        assertNull(codec.parse("null"));
        assertNull(codec.parse(null));
        assertThrows(IllegalArgumentException.class, () -> UDTCodec.create(type, List.class).serialize(List.of("one", "two")));
    }

    @Test
    void sessionMetadataResolvesTheRequestedUdtAndReportsMissingTypes() {
        final CqlSession session = mock(CqlSession.class);
        final Metadata metadata = mock(Metadata.class);
        final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        final UserDefinedType type = userType();
        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace("ks")).thenReturn(Optional.of(keyspace));
        when(keyspace.getUserDefinedType("contact")).thenReturn(Optional.of(type));
        assertSame(type, UDTCodec.create(session, "ks", "contact", Map.class).getCqlType());
        assertThrows(IllegalArgumentException.class, () -> UDTCodec.create(session, "ks", "contact", null));
        when(keyspace.getUserDefinedType("contact")).thenReturn(Optional.empty());
        assertThrows(NoSuchElementException.class, () -> UDTCodec.create(session, "ks", "contact", Map.class));
        when(metadata.getKeyspace("ks")).thenReturn(Optional.empty());
        assertThrows(NoSuchElementException.class, () -> UDTCodec.create(session, "ks", "contact", Map.class));
    }

    @Test
    void registeredBeanCodecRoundTripsThroughTheSessionRegistry() {
        final DefaultCodecRegistry registry = new DefaultCodecRegistry("coverage-bean");
        final CqlSession session = mock(CqlSession.class);
        final DriverContext context = mock(DriverContext.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(registry);
        final CassandraExecutor executor = new CassandraExecutor(session);
        try {
            assertThrows(IllegalArgumentException.class, () -> executor.registerTypeCodec(null));
            executor.registerTypeCodec(Contact.class);
            final var codec = registry.codecFor(DataTypes.TEXT, Contact.class);
            final Contact value = new Contact();
            value.setName("Ada");
            assertEquals("Ada", codec.decode(codec.encode(value, ProtocolVersion.V4), ProtocolVersion.V4).getName());
            final var batch = executor.prepareBatchStatement(null);
            assertEquals(BatchType.LOGGED, batch.getBatchType());
            assertEquals(0, batch.size());
            assertEquals(BatchType.UNLOGGED, executor.prepareBatchStatement(BatchType.UNLOGGED).getBatchType());
        } finally {
            executor.close();
        }
    }

    private static UserDefinedType userType() {
        final CodecRegistry registry = new DefaultCodecRegistry("coverage-udt");
        return new DefaultUserDefinedType(CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("contact"), false,
                List.of(CqlIdentifier.fromInternal("name")), List.of(DataTypes.TEXT), new AttachmentPoint() {
                    @Override
                    public ProtocolVersion getProtocolVersion() {
                        return ProtocolVersion.V4;
                    }

                    @Override
                    public CodecRegistry getCodecRegistry() {
                        return registry;
                    }
                });
    }

    public static class Contact {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }
}
