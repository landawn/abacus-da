/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.da;
 
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.stream.Stream;

// TODO: Auto-generated Javadoc
/**
 * Asynchronous <code>HBaseExecutor</code>.
 *
 * @author Haiyang Li
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Table
 * @since 0.8
 */
public final class AsyncHBaseExecutor {

    /** The hbase executor. */
    private final HBaseExecutor hbaseExecutor;

    /** The async executor. */
    private final AsyncExecutor asyncExecutor;

    /**
     * Instantiates a new async H base executor.
     *
     * @param hbaseExecutor the hbase executor
     * @param asyncExecutor the async executor
     */
    AsyncHBaseExecutor(final HBaseExecutor hbaseExecutor, final AsyncExecutor asyncExecutor) {
        this.hbaseExecutor = hbaseExecutor;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Sync.
     *
     * @return the h base executor
     */
    public HBaseExecutor sync() {
        return hbaseExecutor;
    }

    /**
     * Exists.
     *
     * @param tableName the table name
     * @param get the get
     * @return the continuable future
     */
    public ContinuableFuture<Boolean> exists(final String tableName, final Get get) {
        return asyncExecutor.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return hbaseExecutor.exists(tableName, get);
            }
        });
    }

    /**
     * Exists.
     *
     * @param tableName the table name
     * @param gets the gets
     * @return the continuable future
     */
    public ContinuableFuture<List<Boolean>> exists(final String tableName, final List<Get> gets) {
        return asyncExecutor.execute(new Callable<List<Boolean>>() {
            @Override
            public List<Boolean> call() throws Exception {
                return hbaseExecutor.exists(tableName, gets);
            }
        });
    }

    /**
     * Test for the existence of columns in the table, as specified by the Gets.
     * This will return an array of booleans. Each value will be true if the related Get matches
     * one or more keys, false if not.
     * This is a server-side call so it prevents any data from being transferred to
     * the client.
     *
     * @param tableName the table name
     * @param gets the Gets
     * @return Array of boolean.  True if the specified Get matches one or more keys, false if not.
     * @deprecated since 2.0 version and will be removed in 3.0 version.
     *             use {@code exists(List)}
     */
    @Deprecated
    public ContinuableFuture<List<Boolean>> existsAll(final String tableName, final List<Get> gets) {
        return asyncExecutor.execute(new Callable<List<Boolean>>() {
            @Override
            public List<Boolean> call() throws Exception {
                return hbaseExecutor.existsAll(tableName, gets);
            }
        });
    }

    /**
     * Exists.
     *
     * @param tableName the table name
     * @param anyGet the any get
     * @return the continuable future
     */
    public ContinuableFuture<Boolean> exists(final String tableName, final AnyGet anyGet) {
        return asyncExecutor.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return hbaseExecutor.exists(tableName, anyGet);
            }
        });
    }

    /**
     * Exists.
     *
     * @param tableName the table name
     * @param anyGets the any gets
     * @return the continuable future
     */
    public ContinuableFuture<List<Boolean>> exists(final String tableName, final Collection<AnyGet> anyGets) {
        return asyncExecutor.execute(new Callable<List<Boolean>>() {
            @Override
            public List<Boolean> call() throws Exception {
                return hbaseExecutor.exists(tableName, anyGets);
            }
        });
    }

    /**
     * Exists all.
     *
     * @param tableName the table name
     * @param anyGets the any gets
     * @return the continuable future
     * @throws UncheckedIOException the unchecked IO exception
     * @deprecated  use {@code exists(String, Collection)}
     */
    @Deprecated
    public ContinuableFuture<List<Boolean>> existsAll(final String tableName, final Collection<AnyGet> anyGets) {
        return asyncExecutor.execute(new Callable<List<Boolean>>() {
            @Override
            public List<Boolean> call() throws Exception {
                return hbaseExecutor.existsAll(tableName, anyGets);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param get the get
     * @return the continuable future
     */
    public ContinuableFuture<Result> get(final String tableName, final Get get) {
        return asyncExecutor.execute(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                return hbaseExecutor.get(tableName, get);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param gets the gets
     * @return the continuable future
     */
    public ContinuableFuture<List<Result>> get(final String tableName, final List<Get> gets) {
        return asyncExecutor.execute(new Callable<List<Result>>() {
            @Override
            public List<Result> call() throws Exception {
                return hbaseExecutor.get(tableName, gets);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param anyGet the any get
     * @return the continuable future
     */
    public ContinuableFuture<Result> get(final String tableName, final AnyGet anyGet) {
        return asyncExecutor.execute(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                return hbaseExecutor.get(tableName, anyGet);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param anyGets the any gets
     * @return the continuable future
     */
    public ContinuableFuture<List<Result>> get(final String tableName, final Collection<AnyGet> anyGets) {
        return asyncExecutor.execute(new Callable<List<Result>>() {
            @Override
            public List<Result> call() throws Exception {
                return hbaseExecutor.get(tableName, anyGets);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param get the get
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> get(final Class<T> targetClass, final String tableName, final Get get) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return hbaseExecutor.get(targetClass, tableName, get);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param gets the gets
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> get(final Class<T> targetClass, final String tableName, final List<Get> gets) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return hbaseExecutor.get(targetClass, tableName, gets);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param anyGet the any get
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> get(final Class<T> targetClass, final String tableName, final AnyGet anyGet) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return hbaseExecutor.get(targetClass, tableName, anyGet);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param anyGets the any gets
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> get(final Class<T> targetClass, final String tableName, final Collection<AnyGet> anyGets) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return hbaseExecutor.get(targetClass, tableName, anyGets);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param scan the scan
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final Scan scan) {
        return asyncExecutor.execute(new Callable<Stream<Result>>() {
            @Override
            public Stream<Result> call() throws Exception {
                return hbaseExecutor.scan(tableName, scan);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param anyScan the any scan
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final AnyScan anyScan) {
        return asyncExecutor.execute(new Callable<Stream<Result>>() {
            @Override
            public Stream<Result> call() throws Exception {
                return hbaseExecutor.scan(tableName, anyScan);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param family the family
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final String family) {
        return asyncExecutor.execute(new Callable<Stream<Result>>() {
            @Override
            public Stream<Result> call() throws Exception {
                return hbaseExecutor.scan(tableName, family);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param family the family
     * @param qualifier the qualifier
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final String family, final String qualifier) {
        return asyncExecutor.execute(new Callable<Stream<Result>>() {
            @Override
            public Stream<Result> call() throws Exception {
                return hbaseExecutor.scan(tableName, family, qualifier);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param family the family
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final byte[] family) {
        return asyncExecutor.execute(new Callable<Stream<Result>>() {
            @Override
            public Stream<Result> call() throws Exception {
                return hbaseExecutor.scan(tableName, family);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param family the family
     * @param qualifier the qualifier
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final byte[] family, final byte[] qualifier) {
        return asyncExecutor.execute(new Callable<Stream<Result>>() {
            @Override
            public Stream<Result> call() throws Exception {
                return hbaseExecutor.scan(tableName, family, qualifier);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param scan the scan
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final Scan scan) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return hbaseExecutor.scan(targetClass, tableName, scan);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param anyScan the any scan
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final AnyScan anyScan) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return hbaseExecutor.scan(targetClass, tableName, anyScan);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param family the family
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final String family) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return hbaseExecutor.scan(targetClass, tableName, family);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param family the family
     * @param qualifier the qualifier
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final String family, final String qualifier) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return hbaseExecutor.scan(targetClass, tableName, family, qualifier);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param family the family
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final byte[] family) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return hbaseExecutor.scan(targetClass, tableName, family);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param family the family
     * @param qualifier the qualifier
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final byte[] family, final byte[] qualifier) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return hbaseExecutor.scan(targetClass, tableName, family, qualifier);
            }
        });
    }

    /**
     * Put.
     *
     * @param tableName the table name
     * @param put the put
     * @return the continuable future
     */
    public ContinuableFuture<Void> put(final String tableName, final Put put) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.put(tableName, put);

                return null;
            }
        });
    }

    /**
     * Put.
     *
     * @param tableName the table name
     * @param puts the puts
     * @return the continuable future
     */
    public ContinuableFuture<Void> put(final String tableName, final List<Put> puts) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.put(tableName, puts);

                return null;
            }
        });
    }

    /**
     * Put.
     *
     * @param tableName the table name
     * @param anyPut the any put
     * @return the continuable future
     */
    public ContinuableFuture<Void> put(final String tableName, final AnyPut anyPut) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.put(tableName, anyPut);

                return null;
            }
        });
    }

    /**
     * Put.
     *
     * @param tableName the table name
     * @param anyPuts the any puts
     * @return the continuable future
     */
    public ContinuableFuture<Void> put(final String tableName, final Collection<AnyPut> anyPuts) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.put(tableName, anyPuts);

                return null;
            }
        });
    }

    /**
     * Delete.
     *
     * @param tableName the table name
     * @param delete the delete
     * @return the continuable future
     */
    public ContinuableFuture<Void> delete(final String tableName, final Delete delete) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.delete(tableName, delete);

                return null;
            }
        });
    }

    /**
     * Delete.
     *
     * @param tableName the table name
     * @param deletes the deletes
     * @return the continuable future
     */
    public ContinuableFuture<Void> delete(final String tableName, final List<Delete> deletes) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.delete(tableName, deletes);

                return null;
            }
        });
    }

    /**
     * Delete.
     *
     * @param tableName the table name
     * @param anyDelete the any delete
     * @return the continuable future
     */
    public ContinuableFuture<Void> delete(final String tableName, final AnyDelete anyDelete) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.delete(tableName, anyDelete);

                return null;
            }
        });
    }

    /**
     * Delete.
     *
     * @param tableName the table name
     * @param anyDeletes the any deletes
     * @return the continuable future
     */
    public ContinuableFuture<Void> delete(final String tableName, final Collection<AnyDelete> anyDeletes) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.delete(tableName, anyDeletes);

                return null;
            }
        });
    }

    /**
     * Mutate row.
     *
     * @param tableName the table name
     * @param rm the rm
     * @return the continuable future
     */
    public ContinuableFuture<Void> mutateRow(final String tableName, final RowMutations rm) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.mutateRow(tableName, rm);

                return null;
            }
        });
    }

    /**
     * Append.
     *
     * @param tableName the table name
     * @param append the append
     * @return the continuable future
     */
    public ContinuableFuture<Result> append(final String tableName, final Append append) {
        return asyncExecutor.execute(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                return hbaseExecutor.append(tableName, append);
            }
        });
    }

    /**
     * Increment.
     *
     * @param tableName the table name
     * @param increment the increment
     * @return the continuable future
     */
    public ContinuableFuture<Result> increment(final String tableName, final Increment increment) {
        return asyncExecutor.execute(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                return hbaseExecutor.increment(tableName, increment);
            }
        });
    }

    /**
     * Increment column value.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @param family the family
     * @param qualifier the qualifier
     * @param amount the amount
     * @return the continuable future
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier,
            final long amount) {
        return asyncExecutor.execute(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount);
            }
        });
    }

    /**
     * Increment column value.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @param family the family
     * @param qualifier the qualifier
     * @param amount the amount
     * @param durability the durability
     * @return the continuable future
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier,
            final long amount, final Durability durability) {
        return asyncExecutor.execute(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability);
            }
        });
    }

    /**
     * Increment column value.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @param family the family
     * @param qualifier the qualifier
     * @param amount the amount
     * @return the continuable future
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier,
            final long amount) {
        return asyncExecutor.execute(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount);
            }
        });
    }

    /**
     * Increment column value.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @param family the family
     * @param qualifier the qualifier
     * @param amount the amount
     * @param durability the durability
     * @return the continuable future
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier,
            final long amount, final Durability durability) {
        return asyncExecutor.execute(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability);
            }
        });
    }

    /**
     * Coprocessor service.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @return the continuable future
     */
    public ContinuableFuture<CoprocessorRpcChannel> coprocessorService(final String tableName, final Object rowKey) {
        return asyncExecutor.execute(new Callable<CoprocessorRpcChannel>() {
            @Override
            public CoprocessorRpcChannel call() throws Exception {
                return hbaseExecutor.coprocessorService(tableName, rowKey);
            }
        });
    }

    /**
     * Coprocessor service.
     *
     * @param <T> the generic type
     * @param <R> the generic type
     * @param tableName the table name
     * @param service the service
     * @param startRowKey the start row key
     * @param endRowKey the end row key
     * @param callable the callable
     * @return the continuable future
     * @throws Exception the exception
     */
    public <T extends Service, R> ContinuableFuture<Map<byte[], R>> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable) throws Exception {
        return asyncExecutor.execute(new Callable<Map<byte[], R>>() {
            @Override
            public Map<byte[], R> call() throws Exception {
                return hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable);
            }
        });
    }

    /**
     * Coprocessor service.
     *
     * @param <T> the generic type
     * @param <R> the generic type
     * @param tableName the table name
     * @param service the service
     * @param startRowKey the start row key
     * @param endRowKey the end row key
     * @param callable the callable
     * @param callback the callback
     * @return the continuable future
     * @throws Exception the exception
     */
    public <T extends Service, R> ContinuableFuture<Void> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable, final Batch.Callback<R> callback) throws Exception {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable, callback);

                return null;
            }
        });
    }

    /**
     * Batch coprocessor service.
     *
     * @param <R> the generic type
     * @param tableName the table name
     * @param methodDescriptor the method descriptor
     * @param request the request
     * @param startRowKey the start row key
     * @param endRowKey the end row key
     * @param responsePrototype the response prototype
     * @return the continuable future
     * @throws Exception the exception
     */
    public <R extends Message> ContinuableFuture<Map<byte[], R>> batchCoprocessorService(final String tableName,
            final Descriptors.MethodDescriptor methodDescriptor, final Message request, final Object startRowKey, final Object endRowKey,
            final R responsePrototype) throws Exception {
        return asyncExecutor.execute(new Callable<Map<byte[], R>>() {
            @Override
            public Map<byte[], R> call() throws Exception {
                return hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype);
            }
        });
    }

    /**
     * Batch coprocessor service.
     *
     * @param <R> the generic type
     * @param tableName the table name
     * @param methodDescriptor the method descriptor
     * @param request the request
     * @param startRowKey the start row key
     * @param endRowKey the end row key
     * @param responsePrototype the response prototype
     * @param callback the callback
     * @return the continuable future
     * @throws Exception the exception
     */
    public <R extends Message> ContinuableFuture<Void> batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor,
            final Message request, final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback)
            throws Exception {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype, callback);

                return null;
            }
        });
    }
}
