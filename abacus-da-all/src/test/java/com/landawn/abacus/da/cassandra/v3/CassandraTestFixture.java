package com.landawn.abacus.da.cassandra.v3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.mockito.Answers;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;

/** Driver objects are mocked; statement construction and parameter conversion remain real. */
final class CassandraTestFixture implements AutoCloseable {
    final Session session = mock(Session.class);
    final Cluster cluster = mock(Cluster.class);
    final PreparedStatement prepared = mock(PreparedStatement.class);
    final ColumnDefinitions columns = mock(ColumnDefinitions.class);
    final List<Object[]> values = new ArrayList<>();
    final List<BoundStatement> statements = new ArrayList<>();
    final CassandraExecutor executor;

    CassandraTestFixture() {
        this(new CodecRegistry(), null);
    }

    CassandraTestFixture(final CassandraExecutor.StatementSettings settings) {
        this(new CodecRegistry(), settings);
    }

    CassandraTestFixture(final CodecRegistry codecRegistry) {
        this(codecRegistry, null);
    }

    private CassandraTestFixture(final CodecRegistry codecRegistry, final CassandraExecutor.StatementSettings settings) {
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.init()).thenReturn(session);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(codecRegistry);
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
        when(session.prepare(anyString())).thenReturn(prepared);
        when(prepared.getVariables()).thenReturn(columns);
        when(prepared.bind(any(Object[].class))).thenAnswer(invocation -> {
            values.add(((Object[]) invocation.getRawArguments()[0]).clone());
            final BoundStatement statement = mock(BoundStatement.class, Answers.RETURNS_SELF);
            statements.add(statement);
            return statement;
        });
        executor = new CassandraExecutor(session, settings);
    }

    void columns(final DataType type, final String... names) {
        when(columns.size()).thenReturn(names.length);
        when(columns.getType(anyInt())).thenReturn(type);
        when(columns.getName(anyInt())).thenAnswer(invocation -> names[(Integer) invocation.getArgument(0)]);
    }

    @Override
    public void close() {
        executor.close();
    }
}
