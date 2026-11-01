package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

class CassandraUdtFieldTraversalTest {
    @Test
    void beanDecodingTraversesFieldMetadataOnceAfterResolvingProperties() throws Exception {
        final String[] names = { "ignored", "start", "times", "timesByName", "raw" };
        final DataType[] types = { DataType.text(), DataType.time(), DataType.list(DataType.time()), DataType.map(DataType.text(), DataType.time()),
                DataType.time() };
        final UserType userType = spy(userType(names, types));
        final CassandraExecutor.UDTCodec<TimedFields> codec = CassandraExecutor.UDTCodec.create(userType, TimedFields.class);
        final UDTValue value = mock(UDTValue.class);
        final LocalTime time = LocalTime.of(12, 34, 56, 123);
        final long nanos = time.toNanoOfDay();
        when(value.getObject(0)).thenReturn("ignored");
        when(value.getObject(1)).thenReturn(nanos);
        when(value.getObject(2)).thenReturn(List.of(nanos));
        when(value.getObject(3)).thenReturn(Map.of("first", nanos));
        when(value.getObject(4)).thenReturn(nanos);
        final TimedFields expected = new TimedFields(time, List.of(time), Map.of("first", time), nanos);

        assertEquals(expected, codec.deserialize(value));
        clearInvocations(userType);
        assertEquals(expected, codec.deserialize(value));
        verify(userType, times(1)).iterator();
    }

    private static UserType userType(final String[] names, final DataType[] types) throws Exception {
        final var fieldConstructor = UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final Collection<UserType.Field> fields = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            fields.add(fieldConstructor.newInstance(names[i], types[i]));
        }
        final var constructor = UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class, Collection.class, ProtocolVersion.class,
                CodecRegistry.class);
        constructor.setAccessible(true);
        return constructor.newInstance("ks", "timed_fields", false, fields, ProtocolVersion.V4, new CodecRegistry());
    }

    public record TimedFields(LocalTime start, List<LocalTime> times, Map<String, LocalTime> timesByName, Long raw) {
    }
}
