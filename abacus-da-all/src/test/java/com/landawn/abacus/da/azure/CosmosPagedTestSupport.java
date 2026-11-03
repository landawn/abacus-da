package com.landawn.abacus.da.azure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import org.mockito.stubbing.Answer;

import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.ModelBridgeInternal;

/**
 * Stubs for a mocked {@code CosmosPagedIterable}: the executor's {@code streamItems}/{@code streamAllItems} read it through
 * {@code streamByPage()}. Each answer returns a fresh stream, so a stub can serve several calls.
 */
final class CosmosPagedTestSupport {

    private CosmosPagedTestSupport() {
    }

    /** One page holding {@code items}. */
    static <T> Answer<Stream<FeedResponse<T>>> onePage(final List<? extends T> items) {
        return invocation -> Stream.of(page(items));
    }

    /** No pages at all. */
    static <T> Answer<Stream<FeedResponse<T>>> noPages() {
        return invocation -> Stream.empty();
    }

    static <T> FeedResponse<T> page(final List<? extends T> items) {
        return ModelBridgeInternal.createFeedResponse(new ArrayList<T>(items), new HashMap<>());
    }
}
