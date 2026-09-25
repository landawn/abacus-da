package com.landawn.abacus.da;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.da.gcp.BigQueryExecutor;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class ExceptionContractTest extends TestBase {

    @Test
    void dynamoInFiltersPropagateValueConversionFailures() {
        final IllegalStateException failure = new IllegalStateException("cannot encode condition value");
        final Object value = new Object() {
            @Override
            public String toString() {
                throw failure;
            }
        };

        // Both overloads delegate value conversion; the original converter failure must remain visible.
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.Filters.in("value", value)));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.Filters.in("value", List.of(value))));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters.in("value", value)));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters.in("value", List.of(value))));
    }

    @Test
    void compositeDynamoKeysValidateTheFirstValueBeforeTheSecondName() {
        final IllegalArgumentException v1 = assertThrows(IllegalArgumentException.class,
                () -> com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.asKey("id", null, "", 2));
        final IllegalArgumentException v2 = assertThrows(IllegalArgumentException.class,
                () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asKey("id", null, "", 2));

        assertTrue(v1.getMessage().contains("'id' must not be null"));
        assertTrue(v2.getMessage().contains("'id' must not be null"));
    }

    @Test
    void dynamoMappersValidateTheEntityTypeBeforeTheTableName() {
        final var v1Client = mock(com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient.class);
        final var v2Client = mock(DynamoDbAsyncClient.class);
        final var v1 = new com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor(v1Client);
        final var v2 = new com.landawn.abacus.da.aws.dynamodb.v2.AsyncDynamoDBExecutor(v2Client);

        final IllegalArgumentException v1Error = assertThrows(IllegalArgumentException.class, () -> v1.mapper(String.class, null, null));
        final IllegalArgumentException v2Error = assertThrows(IllegalArgumentException.class, () -> v2.mapper(String.class, null, null));

        assertTrue(v1Error.getMessage().contains("not an entity class"));
        assertTrue(v2Error.getMessage().contains("not an entity class"));
        final IllegalArgumentException v1Keys = assertThrows(IllegalArgumentException.class, () -> v1.mapper(EntityWithoutId.class, null, null));
        final IllegalArgumentException v2Keys = assertThrows(IllegalArgumentException.class, () -> v2.mapper(EntityWithoutId.class, null, null));

        assertTrue(v1Keys.getMessage().contains("0 ID properties"));
        assertTrue(v2Keys.getMessage().contains("0 ID properties"));
        verifyNoInteractions(v1Client, v2Client);
    }

    public static class EntityWithoutId {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @Test
    void consumedV1BuilderChecksStateBeforeArgumentsOrValueConversion() {
        final var builder = com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.Filters.builder();
        builder.build();

        assertThrows(NullPointerException.class, () -> builder.eq(null, Double.NaN));
        assertThrows(NullPointerException.class, () -> builder.in(null, (Object[]) null));
        assertNull(builder.build());
    }

    @Test
    void consumedV2BuilderChecksStateBeforeArgumentsOrValueConversion() {
        final var builder = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters.builder();
        builder.build();

        assertThrows(NullPointerException.class, () -> builder.eq(null, Double.NaN));
        assertThrows(NullPointerException.class, () -> builder.in(null, (Object[]) null));
        assertNull(builder.build());
    }

    @Test
    void typedDynamoRequestsValidateBeforeCopyingTheKeyMap() {
        final DynamoDbClient client = mock(DynamoDbClient.class);
        final DynamoDbAsyncClient asyncClient = mock(DynamoDbAsyncClient.class);
        final var executor = new com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor(client);
        final var async = new com.landawn.abacus.da.aws.dynamodb.v2.AsyncDynamoDBExecutor(asyncClient);
        final Map<String, AttributeValue> key = new AbstractMap<>() {
            @Override
            public Set<Entry<String, AttributeValue>> entrySet() {
                throw new AssertionError("Request construction must follow argument validation");
            }
        };

        assertThrows(IllegalArgumentException.class, () -> executor.getItem("table", key, (Class<Object>) null));
        assertThrows(IllegalArgumentException.class, () -> async.getItem("table", key, (Class<Object>) null));
        verifyNoInteractions(client, asyncClient);
    }

    @Test
    void bigQueryEntityValidationUsesSignatureOrder() {
        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> BigQueryExecutor.toEntity((FieldList) null, null, String.class));

        assertTrue(error.getMessage().contains("fields"));
    }

    @Test
    void nullBigQuerySqlFailsBeforeCallingTheClient() {
        final BigQuery client = mock(BigQuery.class);
        final Object parameter = new Object() {
            @Override
            public String toString() {
                throw new AssertionError("SQL must be validated before parameter conversion");
            }
        };

        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(client).execute(null, parameter));
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(client).execute("", parameter));
        verifyNoInteractions(client);
    }

    @Test
    void bigQueryRowCountOverflowOnlyAppliesToResultsWithASchema() {
        final TableResult result = mock(TableResult.class);
        when(result.getTotalRows()).thenReturn((long) Integer.MAX_VALUE + 1);

        assertTrue(BigQueryExecutor.toList(result, Object[].class).isEmpty());
        when(result.getSchema()).thenReturn(Schema.of(Field.of("value", StandardSQLTypeName.INT64)));
        assertThrows(ArithmeticException.class, () -> BigQueryExecutor.toList(result, Object[].class));
        assertThrows(ArithmeticException.class, () -> BigQueryExecutor.extractData(result, Object[].class));
    }
}
