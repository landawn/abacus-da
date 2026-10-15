package com.landawn.abacus.da.aws.dynamodb.v2;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/** Lets pagination tests await the client's dependent page chain after the public result is cancelled. */
final class ObservedPageFuture<T> extends CompletableFuture<T> {
    private final List<CompletableFuture<?>> dependents = new CopyOnWriteArrayList<>();

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        final CompletableFuture<U> dependent = new CompletableFuture<>();
        dependents.add(dependent);
        return dependent;
    }

    void awaitPageChain() throws Exception {
        awaitPageChain(5, TimeUnit.SECONDS);
    }

    void awaitPageChain(final long timeout, final TimeUnit unit) throws Exception {
        if (dependents.isEmpty()) {
            throw new AssertionError("The executor must attach a page-processing stage before the response is released");
        }
        CompletableFuture.allOf(dependents.toArray(CompletableFuture<?>[]::new)).get(timeout, unit);
    }
}
