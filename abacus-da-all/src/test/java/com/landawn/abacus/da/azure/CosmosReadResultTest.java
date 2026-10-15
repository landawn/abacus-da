package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;

class CosmosReadResultTest {

    @Test
    @SuppressWarnings("unchecked")
    void gettReturnsTheBodyForBothRequestOptionOverloads() {
        final CosmosContainer container = mock(CosmosContainer.class);
        final CosmosContainerExecutor executor = new CosmosContainerExecutor(container);
        final PartitionKey key = new PartitionKey("partition");
        final CosmosItemRequestOptions options = new CosmosItemRequestOptions();
        final CosmosItemResponse<String> response = mock(CosmosItemResponse.class);
        final String value = new String("body");
        when(response.getItem()).thenReturn(value);
        when(container.readItem("id", key, String.class)).thenReturn(response);
        when(container.readItem("id", key, options, String.class)).thenReturn(response);
        assertSame(value, executor.gett("id", key, String.class));
        assertSame(value, executor.gett("id", key, options, String.class));
    }

    @ParameterizedTest
    @ValueSource(ints = { 404, 429, 500 })
    void onlyNotFoundIsConvertedToAnAbsentValue(final int status) {
        final CosmosContainer container = mock(CosmosContainer.class);
        final CosmosContainerExecutor executor = new CosmosContainerExecutor(container);
        final PartitionKey key = new PartitionKey("partition");
        final CosmosItemRequestOptions options = new CosmosItemRequestOptions();
        final CosmosException failure = mock(CosmosException.class);
        when(failure.getStatusCode()).thenReturn(status);
        when(container.readItem("id", key, String.class)).thenThrow(failure);
        when(container.readItem("id", key, options, String.class)).thenThrow(failure);
        if (status == 404) {
            assertNull(executor.gett("id", key, String.class));
            assertNull(executor.gett("id", key, options, String.class));
            assertTrue(executor.get("id", key, options, String.class).isEmpty());
        } else {
            assertSame(failure, assertThrows(CosmosException.class, () -> executor.gett("id", key, String.class)));
            assertSame(failure, assertThrows(CosmosException.class, () -> executor.gett("id", key, options, String.class)));
            assertSame(failure, assertThrows(CosmosException.class, () -> executor.get("id", key, options, String.class)));
        }
    }
}
