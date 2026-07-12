/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.da.mongodb.reactivestreams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.da.mongodb.MongoDBBase;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.function.Function;
import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive MongoDB collection executor providing Publisher-based database operations with full reactive streams support.
 *
 * <p>This class provides a comprehensive reactive interface for MongoDB collection operations, returning
 * Publishers for all database interactions. It integrates seamlessly with reactive frameworks like Project Reactor,
 * RxJava, and supports the full reactive streams specification including backpressure, error handling,
 * and completion semantics.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li><strong>Publisher-Based Operations:</strong> All methods return Publishers (typically {@link Mono} or
 *       {@link Flux}) for reactive composition</li>
 *   <li><strong>Backpressure Support:</strong> Honours reactive-streams backpressure via the underlying driver</li>
 *   <li><strong>Error Propagation:</strong> Database failures surface as Publisher {@code onError} signals
 *       rather than thrown exceptions</li>
 *   <li><strong>Cursor Lifecycle:</strong> The driver's {@link FindPublisher} cursor is opened on subscription
 *       and closed automatically on completion, cancellation, or error</li>
 *   <li><strong>Integration Ready:</strong> Direct compatibility with Reactor, RxJava, and any other library
 *       that consumes {@code org.reactivestreams.Publisher}</li>
 * </ul>
 *
 * <h3>Reactive Patterns:</h3>
 * <ul>
 *   <li><strong>Cold Publishers:</strong> Every returned {@code Mono}/{@code Flux} is <i>cold</i> — no
 *       request is sent to MongoDB until the Publisher is subscribed to, and each new subscription executes
 *       the operation again (and opens a new cursor for find/aggregate). Re-subscribing a write operation
 *       therefore re-issues the write.</li>
 *   <li><strong>Hot Streams:</strong> {@link #watch()} returns a long-lived {@link ChangeStreamPublisher}
 *       that does not naturally complete; cancel the subscription to close the cursor.</li>
 *   <li><strong>Single-Value Operations:</strong> {@code Mono} for at-most-one result; empty Mono indicates
 *       "no match"</li>
 *   <li><strong>Multi-Value Streams:</strong> {@code Flux} for result sets, backed by a server-side cursor</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe. All operations can be called concurrently from multiple threads.
 * The underlying reactive MongoDB driver handles thread safety and resource management.</p>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 *   <li>Publishers are lazy — computation starts only on subscription</li>
 *   <li>Use appropriate schedulers to avoid blocking reactive threads</li>
 *   <li>Honour backpressure when consuming large result sets to avoid unbounded buffering</li>
 *   <li>Consider connection-pool settings for high-concurrency scenarios</li>
 *   <li>Use projection to reduce network bandwidth for large documents</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * MongoCollectionExecutor executor = reactiveMongoDB.collectionExecutor("users");
 * 
 * // Single value operations (single value, Mono):
 * Mono<Long> countMono = executor.count();
 * Mono<Document> firstMono = executor.findFirst(Filters.eq("status", "active"));
 *
 * // Multi-value operations (multi value, Flux):
 * Flux<Document> docs = executor.list(Filters.gte("age", 18));
 *
 * // With Project Reactor integration:
 * Mono<Long> totalCount = Mono.from(countMono);
 * Flux<Document> documents = Flux.from(docs)
 *     .filter(doc -> doc.getInteger("score") > 80)
 *     .take(100)
 *     .doOnNext(doc -> System.out.println(doc.toJson()));
 * 
 * // Reactive aggregation pipeline:
 * List<Bson> pipeline = Arrays.asList(
 *     Aggregates.match(Filters.eq("status", "active")),
 *     Aggregates.group("$department", Accumulators.sum("count", 1))
 * );
 * 
 * Flux<Document> aggregated = Flux.from(executor.aggregate(pipeline))
 *     .doOnComplete(() -> System.out.println("Aggregation complete"))
 *     .doOnError(error -> System.err.println("Error: " + error));
 * 
 * // Subscribe to execute:
 * aggregated.subscribe(
 *     result -> processResult(result),
 *     error -> handleError(error),
 *     () -> System.out.println("Processing complete")
 * );
 * }</pre>
 *
 * <p><b>Naming convention:</b> method names match the synchronous
 * {@link com.landawn.abacus.da.mongodb.MongoCollectionExecutor} — a hybrid vocabulary of abacus "house"
 * reads ({@code get}, {@code findFirst}, {@code list}, {@code query}, {@code exists},
 * {@code count}) and MongoDB-driver writes ({@code insertOne}/{@code insertMany},
 * {@code updateOne}/{@code updateMany}, {@code replaceOne}, {@code deleteOne}/{@code deleteMany}).
 * Each result is returned as a reactive {@code Publisher} ({@code Mono}/{@code Flux}).
 * Unlike the blocking API, there is deliberately no {@code gett} and no {@code queryForSingleNonNull}:
 * an empty {@code Mono} already signals absence, so the null-vs-{@code Optional} (and
 * {@code Nullable}-vs-{@code Optional}) distinction those blocking siblings exist for does not
 * apply here — {@code get} and {@link #queryForSingleValue(String, Bson, Class)} cover both.</p>
 *
 * @see com.mongodb.reactivestreams.client.MongoCollection
 * @see org.reactivestreams.Publisher
 * @see org.reactivestreams.Subscriber
 * @see reactor.core.publisher.Mono
 * @see reactor.core.publisher.Flux
 * @see com.mongodb.client.model.Filters
 * @see com.mongodb.client.model.Projections
 * @see com.mongodb.client.model.Sorts
 * @see com.mongodb.client.model.Updates
 * @see com.mongodb.client.model.Aggregates
 * @see com.mongodb.client.model.Indexes
 * @see <a href="https://www.mongodb.com/docs/drivers/java/reactive-streams/">MongoDB Reactive Streams Driver</a>
 * @see <a href="https://www.reactive-streams.org/">Reactive Streams Specification</a>
 */
public final class MongoCollectionExecutor {

    static final String _$ = "$";

    static final String _$SET = "$set";

    static final String _$EXPR = "$expr";

    static final String _$GROUP = "$group";

    static final String _$SUM = "$sum";

    static final String _COUNT = "count";

    private final MongoCollection<Document> coll;

    /**
     * Package-private constructor for creating a reactive MongoDB collection executor.
     *
     * @param coll the reactive MongoDB collection to wrap
     */
    MongoCollectionExecutor(final MongoCollection<Document> coll) {
        this.coll = coll;
    }

    /**
     * Returns the underlying reactive MongoDB collection for advanced operations.
     *
     * <p>This method provides direct access to the MongoDB reactive streams driver's
     * {@link com.mongodb.reactivestreams.client.MongoCollection} object, allowing for advanced
     * reactive operations not directly exposed by this executor. The returned collection is the
     * same instance passed to this executor at construction time and is pre-configured with the
     * framework's codec registry.</p>
     *
     * <p>Unlike most methods on this executor, {@code coll()} is a plain synchronous accessor — it
     * does not return a {@code Publisher} and performs no database I/O; it simply hands back the
     * wrapped collection reference.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<Document> raw = executor.coll();          // returns the wrapped collection (never null)
     *
     * // Same instance is returned on every call (no copy, no I/O):
     * boolean same = (executor.coll() == executor.coll());      // true
     *
     * // Reach driver features not surfaced by this executor, e.g. namespace / index management:
     * MongoNamespace ns = executor.coll().getNamespace();                                      // e.g. "mydb.users"
     * Publisher<String> indexCreate = executor.coll().createIndex(Indexes.ascending("email")); // cold; runs on subscription
     *
     * // Build your own reactive pipeline directly off the driver publisher:
     * Flux<Document> docs = Flux.from(executor.coll().find(Filters.eq("status", "active"))); // cold Flux
     * }</pre>
     *
     * @return the reactive MongoDB collection instance backing this executor (never {@code null})
     * @see com.mongodb.reactivestreams.client.MongoCollection
     */
    public MongoCollection<Document> coll() {
        return coll;
    }

    /**
     * Checks if a document exists with the specified ObjectId string in a reactive manner.
     *
     * <p>This method provides a reactive way to verify document existence without retrieving the full document,
     * making it efficient for existence checks. The operation completes when the existence check finishes.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionExecutor executor = reactiveMongoDB.collectionExecutor("users");
     *
     * // Typical: document present -> emits true.
     * Mono<Boolean> existsMono = executor.exists("507f1f77bcf86cd799439011");          // cold Mono
     * existsMono.subscribe(exists -> System.out.println(exists ? "found" : "absent")); // e.g. "found"
     *
     * // Typical: no such document -> emits false (never empty for this method).
     * boolean present = executor.exists("507f1f77bcf86cd799439099").block();          // false when absent
     *
     * // Edge/Negative: a null id throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because the id is parsed eagerly.
     * executor.exists((String) null);                            // throws IllegalArgumentException
     *
     * // Edge/Negative: a non-hex / wrong-length id also throws IllegalArgumentException at call time.
     * executor.exists("not-a-valid-objectid");                   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the ObjectId as a string to check for existence
     * @return a Mono that emits {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null or empty, or if it is not a valid ObjectId hex string
     * @see ObjectId
     */
    public Mono<Boolean> exists(final String objectId) {
        return exists(createObjectId(objectId));
    }

    /**
     * Checks if a document exists with the specified ObjectId in a reactive manner.
     *
     * <p>This method provides a reactive way to verify document existence using a typed ObjectId,
     * making it efficient for existence checks. The operation completes when the existence check finishes.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     *
     * // Typical: emits true when present, false when not (never empty).
     * Mono<Boolean> existsMono = executor.exists(userId);                      // cold Mono
     * existsMono.subscribe(exists -> System.out.println("exists: " + exists)); // e.g. "exists: true"
     *
     * // Typical: block for the boolean directly.
     * boolean present = executor.exists(new ObjectId()).block(); // false for a freshly minted id
     *
     * // Edge/Negative: a null ObjectId throws IllegalArgumentException synchronously (at call time),
     * // because the id-to-filter conversion runs eagerly before any Mono is returned.
     * executor.exists((ObjectId) null);                          // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the ObjectId to check for existence
     * @return a Mono that emits {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null
     * @see ObjectId
     */
    public Mono<Boolean> exists(final ObjectId objectId) {
        return exists(MongoDBBase.objectIdToFilter(objectId));
    }

    /**
     * Checks if any documents exist matching the specified filter in a reactive manner.
     *
     * <p>This method provides a reactive way to verify if documents matching the given filter exist
     * without retrieving the actual documents. It is implemented as a {@code countDocuments(filter,
     * limit=1)} query, so the server stops after locating the first match.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson activeUserFilter = Filters.eq("status", "active");
     * Mono<Boolean> existsMono = executor.exists(activeUserFilter);
     *
     * existsMono.subscribe(
     *     exists -> System.out.println("Active users exist: " + exists),
     *     error -> System.err.println("Error: " + error)
     * );
     * }</pre>
     *
     * @param filter the query filter to match documents against; must not be null
     * @return a {@code Mono} that, on subscription, emits a single {@code Boolean} ({@code true} when
     *         at least one document matches the filter, {@code false} otherwise), then completes
     * @throws IllegalArgumentException if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Boolean> exists(final Bson filter) {
        return count(filter, new CountOptions().limit(1)).map(c -> c > 0);
    }

    /**
     * Counts all documents in the collection in a reactive manner.
     *
     * <p>Delegates to the reactive driver's
     * {@link com.mongodb.reactivestreams.client.MongoCollection#countDocuments()
     * countDocuments()}, which is an accurate (full-scan) count rather than the metadata estimate
     * returned by {@link #estimatedDocumentCount()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: emits the exact total document count.
     * Mono<Long> countMono = executor.count();                             // cold Mono
     * countMono.subscribe(count -> System.out.println("Total: " + count)); // e.g. "Total: 100"
     *
     * // Typical: block for the value.
     * long total = executor.count().block();                     // e.g. 100L
     *
     * // Edge: an empty collection -> emits 0L (the Mono never completes empty).
     * Long zero = executor.count().block();                      // 0L when the collection has no documents
     *
     * // Edge: cold — calling count() issues no query until something subscribes.
     * Mono<Long> notRunYet = executor.count();                   // no countDocuments() call yet
     * }</pre>
     *
     * @return a {@code Mono} that, on subscription, emits exactly one {@code Long} with the total
     *         count of documents in the collection, then completes
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see com.mongodb.reactivestreams.client.MongoCollection#countDocuments()
     * @see #estimatedDocumentCount()
     */
    public Mono<Long> count() {
        return Mono.from(coll.countDocuments());
    }

    /**
     * Counts documents matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to count documents that match the given filter criteria.
     * The operation is performed asynchronously and the count is emitted when available.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson activeFilter = Filters.eq("status", "active");
     *
     * // Typical: emits the number of documents matching the filter.
     * Mono<Long> countMono = executor.count(activeFilter);                  // cold Mono
     * countMono.subscribe(count -> System.out.println("Active: " + count)); // e.g. "Active: 50"
     *
     * // Typical: block for the value.
     * long active = executor.count(activeFilter).block();        // e.g. 50L
     *
     * // Edge: a filter that matches nothing -> emits 0L (never completes empty).
     * Long none = executor.count(Filters.eq("status", "no-such")).block(); // 0L
     *
     * // Edge: cold — no countDocuments() call is issued until subscription.
     * Mono<Long> notRunYet = executor.count(activeFilter);       // nothing sent to the server yet
     * }</pre>
     *
     * @param filter the query filter to match documents against; must not be null
     * @return a {@code Mono} that, on subscription, emits a single {@code Long} count of documents
     *         matching the filter, then completes
     * @throws IllegalArgumentException if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Long> count(final Bson filter) {
        N.checkArgNotNull(filter, "filter");

        return Mono.from(coll.countDocuments(filter));
    }

    /**
     * Counts documents matching the specified filter with custom options reactively.
     *
     * <p>This method provides a reactive way to count documents with additional counting options
     * such as limit, skip, maxTime, and collation. The operation is performed asynchronously.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CountOptions options = new CountOptions().limit(1000).maxTime(30, TimeUnit.SECONDS);
     *
     * // Typical: count is capped by the options' limit (here at most 1000).
     * Mono<Long> countMono = executor.count(Filters.gte("age", 18), options);        // cold Mono
     * countMono.subscribe(count -> System.out.println("Adults (<=1000): " + count)); // e.g. 1000
     *
     * // Edge: a null options argument is allowed and falls back to the no-options overload.
     * long exact = executor.count(Filters.gte("age", 18), (CountOptions) null).block(); // unconstrained count
     *
     * // Edge: a skip option subtracts from the count.
     * long afterSkip = executor.count(Filters.gte("age", 18), new CountOptions().skip(10)).block();
     *
     * // Edge: no match -> emits 0L (never empty).
     * long none = executor.count(Filters.eq("age", -1), options).block();    // 0L
     * }</pre>
     *
     * @param filter the query filter to match documents against; must not be null
     * @param options the count options to apply (may be null to use driver defaults)
     * @return a {@code Mono} that, on subscription, emits a single {@code Long} count of documents
     *         matching the filter with the applied options, then completes
     * @throws IllegalArgumentException if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see Bson
     * @see CountOptions
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Long> count(final Bson filter, final CountOptions options) {
        N.checkArgNotNull(filter, "filter");

        if (options == null) {
            return Mono.from(coll.countDocuments(filter));
        } else {
            return Mono.from(coll.countDocuments(filter, options));
        }
    }

    /**
     * Returns an estimated count of documents in the collection reactively.
     *
     * <p>This method provides a reactive way to get an estimated document count, which is faster than
     * an exact count but may be less accurate. It uses collection metadata rather than scanning documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: fast, metadata-based estimate (may differ slightly from count()).
     * Mono<Long> estimatedCountMono = executor.estimatedDocumentCount();                // cold Mono
     * estimatedCountMono.subscribe(count -> System.out.println("Estimated: " + count)); // e.g. 1000
     *
     * // Typical: block for the value.
     * long approx = executor.estimatedDocumentCount().block();   // e.g. 1000L
     *
     * // Edge: an empty collection -> emits 0L (never completes empty).
     * Long zero = executor.estimatedDocumentCount().block();     // 0L for an empty collection
     *
     * // Edge: cold — no estimate is requested from the server until subscription.
     * Mono<Long> notRunYet = executor.estimatedDocumentCount();  // nothing sent yet
     * }</pre>
     *
     * @return a Mono that emits the estimated count of documents in the collection
     * @see com.mongodb.reactivestreams.client.MongoCollection#estimatedDocumentCount()
     */
    public Mono<Long> estimatedDocumentCount() {
        return Mono.from(coll.estimatedDocumentCount());
    }

    /**
     * Returns an estimated count of documents in the collection with custom options reactively.
     *
     * <p>This method provides a reactive way to get an estimated document count with additional options
     * such as maxTime. The estimate is faster than exact counting but may be less accurate.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions()
     *     .maxTime(10, TimeUnit.SECONDS);
     *
     * // Typical: estimate with a server-side time budget.
     * Mono<Long> estimatedCountMono = executor.estimatedDocumentCount(options);         // cold Mono
     * estimatedCountMono.subscribe(count -> System.out.println("Estimated: " + count)); // e.g. 2000
     *
     * // Typical: block for the value.
     * long approx = executor.estimatedDocumentCount(options).block(); // e.g. 2000L
     *
     * // Edge: empty collection -> emits 0L (never completes empty).
     * Long zero = executor.estimatedDocumentCount(options).block(); // 0L for an empty collection
     *
     * // Edge/Negative: exceeding the maxTime budget surfaces a MongoExecutionTimeoutException
     * // as a Mono error (onError), not a thrown exception at call time.
     * }</pre>
     *
     * @param options the estimation options to apply; may be null (default options)
     * @return a {@code Mono} that, on subscription, emits a single {@code Long} estimated count, then
     *         completes
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see EstimatedDocumentCountOptions
     */
    public Mono<Long> estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        if (options == null) {
            return Mono.from(coll.estimatedDocumentCount());
        } else {
            return Mono.from(coll.estimatedDocumentCount(options));
        }
    }

    /**
     * Retrieves a single document by its ObjectId string in a reactive manner.
     *
     * <p>This method provides a reactive way to retrieve a document using its string ObjectId
     * representation. The ObjectId string is parsed eagerly (before subscription), so an invalid
     * hex string surfaces as a thrown {@link IllegalArgumentException} at call time rather than
     * as a {@code Mono} error signal.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * matching {@link Document} and then completes, or completes <i>empty</i> if no document has
     * the specified ObjectId.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: subscribe with all three handlers; the completion handler fires when not found.
     * Mono<Document> docMono = executor.get("507f1f77bcf86cd799439011"); // cold Mono
     * docMono.subscribe(
     *     doc -> System.out.println("Found: " + doc.toJson()),
     *     error -> System.err.println("Error: " + error),
     *     () -> System.out.println("Document not found"));       // fires only when empty
     *
     * // Typical: block for the document (null if absent).
     * Document doc = executor.get("507f1f77bcf86cd799439011").block(); // null when no match
     *
     * // Edge: no document with that id -> Mono completes EMPTY (the onComplete handler runs, no onNext).
     *
     * // Edge/Negative: an invalid hex string throws IllegalArgumentException synchronously (at call
     * // time) — the id is parsed eagerly by the ObjectId constructor before any Mono is built.
     * executor.get("invalid");                                   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the ObjectId as a string to search for
     * @return a {@code Mono} that emits the found document on subscription, or completes empty
     *         when no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null or empty, or if it is not a valid
     *         ObjectId hex string (thrown synchronously by the {@link ObjectId} constructor)
     * @see Document
     * @see ObjectId
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#get(String)
     */
    public Mono<Document> get(final String objectId) {
        return get(createObjectId(objectId));
    }

    /**
     * Retrieves a single document by its ObjectId in a reactive manner.
     *
     * <p>This method provides a reactive way to retrieve a document using a typed ObjectId.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * matching {@link Document} and then completes, or completes <i>empty</i> if no document has
     * the specified ObjectId.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<Document> docMono = executor.get(userId);
     *
     * docMono.subscribe(
     *     doc -> processDocument(doc),
     *     error -> handleError(error)
     * );
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return a {@code Mono} that emits the found document on subscription, or completes empty
     *         when no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see Document
     * @see ObjectId
     */
    public Mono<Document> get(final ObjectId objectId) {
        return get(objectId, Document.class);
    }

    /**
     * Retrieves a single document by its ObjectId string and converts it to the specified type reactively.
     *
     * <p>This method provides a reactive way to retrieve a document and automatically convert it to
     * a Java object of the specified type using the configured codec registry.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<User> userMono = executor.get("507f1f77bcf86cd799439011", User.class);
     * 
     * userMono.subscribe(
     *     user -> System.out.println("User: " + user.getName()),
     *     error -> System.err.println("Error: " + error)
     * );
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param objectId the ObjectId as a string to search for
     * @param rowType the Class representing the target type for conversion
     * @return a {@code Mono} that emits the converted object on subscription, or completes empty
     *         when no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null/empty or not a valid ObjectId hex string
     * @see ObjectId
     */
    public <T> Mono<T> get(final String objectId, final Class<T> rowType) {
        return get(createObjectId(objectId), rowType);
    }

    /**
     * Retrieves a single document by its ObjectId and converts it to the specified type reactively.
     *
     * <p>This method provides a reactive way to retrieve a document and automatically convert it to
     * a Java object of the specified type using the configured codec registry.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<User> userMono = executor.get(userId, User.class);
     * 
     * userMono.subscribe(
     *     user -> processUser(user),
     *     error -> handleError(error)
     * );
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param objectId the ObjectId to search for
     * @param rowType the Class representing the target type for conversion
     * @return a {@code Mono} that emits the converted object on subscription, or completes empty
     *         when no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see ObjectId
     */
    public <T> Mono<T> get(final ObjectId objectId, final Class<T> rowType) {
        return get(objectId, null, rowType);
    }

    /**
     * Retrieves a single document by its ObjectId string with field projection and type conversion reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from a document identified by
     * its ObjectId string, then convert the result to the specified Java type. Field projection helps
     * reduce network traffic and memory usage by only retrieving necessary fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * Mono<User> userMono = executor.get("507f1f77bcf86cd799439011", fields, User.class);
     * 
     * userMono.subscribe(
     *     user -> System.out.println("User basic info: " + user.getName())
     * );
     * }</pre>
     *
     * @param <T> the type to convert the projected document to
     * @param objectId the ObjectId as a string to search for
     * @param selectPropNames the collection of field names to include in the projection
     *                        (null/empty selects all fields)
     * @param rowType the Class representing the target type for conversion
     * @return a {@code Mono} that emits the converted projected object on subscription, or completes
     *         empty when no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null, empty, or not a valid ObjectId hex string
     * @see ObjectId
     * @see com.mongodb.client.model.Projections
     */
    public <T> Mono<T> get(final String objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return get(createObjectId(objectId), selectPropNames, rowType);
    }

    /**
     * Retrieves a single document by its ObjectId with field projection and type conversion reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from a document identified by
     * its ObjectId, then convert the result to the specified Java type. Field projection reduces
     * network traffic and memory usage by only retrieving necessary fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Collection<String> fields = Arrays.asList("name", "email");
     * Mono<User> userMono = executor.get(userId, fields, User.class);
     * 
     * userMono.subscribe(
     *     user -> processUserBasicInfo(user)
     * );
     * }</pre>
     *
     * @param <T> the type to convert the projected document to
     * @param objectId the ObjectId to search for
     * @param selectPropNames the collection of field names to include in the projection
     *                        (null/empty selects all fields)
     * @param rowType the Class representing the target type for conversion
     * @return a {@code Mono} that emits the converted projected object on subscription, or completes
     *         empty when no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see ObjectId
     * @see com.mongodb.client.model.Projections
     */
    public <T> Mono<T> get(final ObjectId objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return findFirst(selectPropNames, MongoDBBase.objectIdToFilter(objectId), null, rowType);
    }

    /**
     * Finds the first document matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to retrieve the first document that matches the given
     * filter criteria. On subscription, the returned {@code Mono} emits the first matching document
     * and completes, or completes empty when no document matches.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     *
     * // Typical: emits the first matching document, then completes.
     * Mono<Document> docMono = executor.findFirst(filter);                    // cold Mono
     * docMono.subscribe(doc -> System.out.println("First: " + doc.toJson())); // prints once if a doc matched
     *
     * // Typical: block (null if no match).
     * Document first = executor.findFirst(filter).block();       // null when nothing matches
     *
     * // Edge: no document matches -> Mono completes EMPTY (no onNext).
     * executor.findFirst(Filters.eq("status", "no-such"))
     *         .subscribe(d -> {}, e -> {}, () -> System.out.println("none")); // prints "none"
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is returned).
     * executor.findFirst((Bson) null);                           // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the first matching document on subscription, or completes
     *         empty when no documents match the filter
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see Document
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Document> findFirst(final Bson filter) {
        return findFirst(filter, Document.class);
    }

    /**
     * Finds the first document matching the specified filter and converts it to the specified type reactively.
     *
     * <p>This method provides a reactive way to retrieve and convert the first document that matches
     * the given filter criteria. The document is automatically converted to the specified Java type
     * using the configured codec registry.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.and(
     *     Filters.eq("status", "active"),
     *     Filters.gte("age", 18)
     * );
     * Mono<User> userMono = executor.findFirst(filter, User.class);
     * 
     * userMono.subscribe(
     *     user -> System.out.println("First adult active user: " + user.getName())
     * );
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param filter the query filter to match documents against (must not be null)
     * @param rowType the Class representing the target type for conversion; must not be null
     * @return a {@code Mono} that emits the first matching document converted to {@code T} on
     *         subscription, or completes empty when no documents match
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public <T> Mono<T> findFirst(final Bson filter, final Class<T> rowType) {
        return findFirst(null, filter, rowType);
    }

    /**
     * Finds the first document matching the specified filter with field projection and type conversion reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from the first document
     * that matches the given filter criteria, then convert the result to the specified Java type.
     * Field projection reduces network traffic and memory usage.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "createdAt");
     * Bson filter = Filters.eq("department", "Engineering");
     * Mono<User> userMono = executor.findFirst(fields, filter, User.class);
     * 
     * userMono.subscribe(
     *     user -> System.out.println("Engineer: " + user.getName())
     * );
     * }</pre>
     *
     * @param <T> the type to convert the projected document to
     * @param selectPropNames the collection of field names to include in the projection
     *                        (null/empty selects all fields)
     * @param filter the query filter to match documents against (must not be null)
     * @param rowType the Class representing the target type for conversion; must not be null
     * @return a {@code Mono} that emits the first matching projected document converted to {@code T}
     *         on subscription, or completes empty when no documents match
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Projections
     */
    public <T> Mono<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return findFirst(selectPropNames, filter, null, rowType);
    }

    /**
     * Finds the first document matching the filter with projection, sorting, and type conversion.
     *
     * <p>Retrieves the first document that matches the filter criteria, with specified field projection
     * and sorting, then converts the result to the target type. The sort parameter determines
     * which document is considered "first" when multiple documents match.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score");
     * Bson sort = Sorts.descending("score");
     * Mono<User> topUser = executor.findFirst(fields, filter, sort, User.class);
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the first matching document converted to type T, or empty if no match
     * @throws IllegalArgumentException if filter or rowType is null (thrown synchronously at the call site)
     * @see com.mongodb.client.model.Sorts
     */
    public <T> Mono<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return query(selectPropNames, filter, sort, 0, 1).next().mapNotNull(toEntity(rowType));
    }

    /**
     * Finds the first document using explicit Bson projection, filter, sort, and type conversion.
     *
     * <p>Provides full control over the projection by accepting a Bson projection object directly,
     * useful for complex projections like exclusions or computed fields. Results are converted
     * to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name"), Projections.excludeId());
     * Bson sort = Sorts.ascending("createdAt");
     * Mono<User> firstUser = executor.findFirst(projection, filter, sort, User.class);
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the first matching document converted to type T, or empty if no match
     * @throws IllegalArgumentException if filter or rowType is null (thrown synchronously at the call site)
     */
    public <T> Mono<T> findFirst(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return executeQuery(projection, filter, sort, 0, 1).next().mapNotNull(toEntity(rowType));
    }

    /**
     * Lists all documents matching the specified filter as a reactive {@link Flux} of
     * {@link Document}.
     *
     * <p>Documents are returned as MongoDB {@code Document} objects without type conversion. The
     * underlying {@link com.mongodb.reactivestreams.client.FindPublisher} is wrapped in a
     * {@code Flux}; each subscriber gets an independent cursor.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     *
     * // Typical: stream every matching document.
     * Flux<Document> activeDocuments = executor.list(filter);             // cold Flux
     * activeDocuments.subscribe(doc -> System.out.println(doc.toJson())); // one call per document
     *
     * // Typical: collect into a list.
     * List<Document> all = executor.list(filter).collectList().block();
     *
     * // Edge: no document matches -> Flux completes EMPTY.
     * executor.list(Filters.eq("status", "no-such"))
     *         .subscribe(d -> {}, e -> {}, () -> System.out.println("empty")); // prints "empty"
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Flux is returned).
     * executor.list((Bson) null);                                // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Flux} that, on subscription, emits each matching document, then completes;
     *         completes empty when no documents match
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#list(Bson)
     */
    public Flux<Document> list(final Bson filter) {
        return list(filter, Document.class);
    }

    /**
     * Lists all documents matching the filter, converted to the specified type.
     *
     * <p>Returns a cold {@link Flux} of all documents that match the filter; each subscription opens
     * a new server-side cursor and converts every emitted document to {@code T} using the configured
     * codec registry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.gte("age", 18);
     * Flux<User> adultUsers = executor.list(filter, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param filter the query filter to match documents against (must not be null)
     * @param rowType the Class representing the target type for conversion; must not be null
     * @return a cold {@code Flux} that, on subscription, emits each matching document converted to
     *         {@code T}, then completes; completes empty when no documents match
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     */
    public <T> Flux<T> list(final Bson filter, final Class<T> rowType) {
        return list(null, filter, rowType);
    }

    /**
     * Lists documents with pagination support and type conversion.
     *
     * <p>Returns a paginated reactive stream of documents that match the filter,
     * starting from the specified offset and limited to the count. Useful for
     * implementing pagination in reactive applications.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * int pageSize = 20;
     * int pageNumber = 3;
     * Flux<User> page = executor.list(filter, pageNumber * pageSize, pageSize, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param filter the query filter to match documents against (must not be null)
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits matching documents within the specified range, converted to type T
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site), or if offset or count is negative
     */
    public <T> Flux<T> list(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return list(null, filter, offset, count, rowType);
    }

    /**
     * Lists documents with field projection and type conversion.
     *
     * <p>Returns a reactive stream of documents matching the filter with only
     * the specified fields included, then converts each to the target type.
     * Projection reduces network traffic and memory usage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email");
     * Flux<User> users = executor.list(fields, filter, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits all matching documents with projected fields, converted to type T
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     */
    public <T> Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return list(selectPropNames, filter, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Lists documents with field projection, pagination, and type conversion.
     *
     * <p>Combines field projection with pagination to return a limited set of documents
     * with only specified fields, starting from an offset. Each document is converted
     * to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("id", "name", "score");
     * Flux<User> topScorers = executor.list(fields, filter, 0, 10, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits matching documents within range with projected fields, converted to type T
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site), or if offset or count is negative
     */
    public <T> Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return list(selectPropNames, filter, null, offset, count, rowType);
    }

    /**
     * Lists documents with field projection, sorting, and type conversion.
     *
     * <p>Returns a sorted reactive stream of documents matching the filter with
     * only specified fields included. Each document is converted to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score");
     * Bson sort = Sorts.descending("score");
     * Flux<User> leaderboard = executor.list(fields, filter, sort, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits sorted matching documents with projected fields, converted to type T
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     */
    public <T> Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return list(selectPropNames, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Lists documents with all query options: projection, filter, sort, pagination, and type conversion.
     *
     * <p>Provides complete control over the query with field projection, filtering, sorting,
     * and pagination. This is the most comprehensive list method, suitable for complex
     * query requirements in reactive applications.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "department", "salary");
     * Bson sort = Sorts.orderBy(Sorts.ascending("department"), Sorts.descending("salary"));
     * Flux<Employee> topEarners = executor.list(fields, filter, sort, 0, 5, Employee.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType an entity class with getter/setter methods, Map.class, or basic single value type
     * @return a Flux that emits matching documents with all specified constraints, converted to type T
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site), or if offset or count is negative
     */
    @SuppressWarnings("unchecked")
    public <T> Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        // Same short-circuit as the sync executor (MongoDBBase.toList / sync stream): when the caller asks
        // for Document/Map/Object, return the raw documents untouched instead of rebuilding them through
        // readRow (which would throw "Unsupported target type" for Object.class).
        if (rowType.isAssignableFrom(Document.class)) {
            return (Flux<T>) query(selectPropNames, filter, sort, offset, count);
        }

        return query(selectPropNames, filter, sort, offset, count).mapNotNull(toEntity(rowType));
    }

    /**
     * Lists documents with Bson projection, filter, sort, and type conversion.
     *
     * <p>Uses a Bson projection object for complex field specifications,
     * combined with filtering and sorting. All results are converted to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name", "age"), Projections.excludeId());
     * Flux<User> users = executor.list(projection, filter, sort, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits all matching sorted documents with projection, converted to type T
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     */
    public <T> Flux<T> list(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return list(projection, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Lists documents with Bson projection, filter, sort, pagination, and type conversion.
     *
     * <p>Provides maximum flexibility with Bson projection, filtering, sorting, and pagination.
     * This method is ideal for complex queries requiring fine-grained control over all aspects
     * of the query execution.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.include("name", "status", "priority");
     * Bson sort = Sorts.descending("priority");
     * Flux<Task> urgentTasks = executor.list(projection, filter, sort, 0, 10, Task.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType an entity class with getter/setter methods, Map.class, or basic single value type
     * @return a Flux that emits matching documents with all specified constraints, converted to type T
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site), or if offset or count is negative
     */
    @SuppressWarnings("unchecked")
    public <T> Flux<T> list(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count, final Class<T> rowType) {
        // Same short-circuit as the sync executor: raw documents for Document/Map/Object targets.
        if (rowType.isAssignableFrom(Document.class)) {
            return (Flux<T>) executeQuery(projection, filter, sort, offset, count);
        }

        return executeQuery(projection, filter, sort, offset, count).mapNotNull(toEntity(rowType));
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@code Boolean} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Boolean} value and then completes. Because this overload
     * is driven by the wrapper type {@code Boolean.class}, missing/null fields surface as Mono
     * completion (not as the primitive default {@code false}) — there is no separate signal to
     * distinguish "no document matched" from "document matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its Boolean value.
     * Mono<Boolean> isActive = executor.queryForBoolean("isActive", Filters.eq("userId", "123")); // cold Mono
     * isActive.subscribe(active -> System.out.println("Active: " + active));                      // prints e.g. "Active: true"
     *
     * // Typical: combine with a default to turn "absent" into a concrete value.
     * Boolean active = executor.queryForBoolean("isActive", Filters.eq("userId", "123"))
     *                          .defaultIfEmpty(false)
     *                          .block();                          // false when no doc / no field
     *
     * // Edge: no document matches the filter -> Mono completes EMPTY (onComplete, no onNext).
     * executor.queryForBoolean("isActive", Filters.eq("userId", "missing")).subscribe(
     *     v -> handleValue(v),                     // never called: no value emitted
     *     err -> handleError(err),                 // never called: no error
     *     () -> System.out.println("no value"));   // prints "no value"
     *
     * // Edge: matching document but the "isActive" field is absent/null -> also completes EMPTY
     * // (the Boolean.class wrapper means there is NO bogus 'false' default).
     *
     * // Negative: a blank propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForBoolean("", Filters.eq("userId", "123")); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Boolean} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForBoolean(String, Bson)
     */
    @Beta
    public Mono<Boolean> queryForBoolean(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Boolean.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@code Character} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Character} value and then completes. Because this
     * overload is driven by the wrapper type {@code Character.class}, missing/null fields surface as
     * Mono completion (not as the primitive default {@code '\0'}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the Character value.
     * Mono<Character> grade = executor.queryForChar("grade", Filters.eq("studentId", "456")); // cold Mono
     * grade.subscribe(g -> System.out.println("Grade: " + g));                                // prints e.g. "Grade: A"
     *
     * // Typical: supply a fallback for the absent case.
     * char g = executor.queryForChar("grade", Filters.eq("studentId", "456"))
     *                  .defaultIfEmpty('?').block();             // '?' when no doc / no field
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY (no onNext).
     * executor.queryForChar("grade", Filters.eq("studentId", "nope"))
     *         .subscribe(g2 -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: a null propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForChar(null, Filters.eq("studentId", "456")); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Character} field value on subscription, or
     *         completes empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForChar(String, Bson)
     */
    @Beta
    public Mono<Character> queryForChar(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Character.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@code Byte} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Byte} value and then completes. Because this overload
     * is driven by the wrapper type {@code Byte.class}, missing/null fields surface as Mono
     * completion (not as the primitive default {@code 0}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the Byte value.
     * Mono<Byte> statusCode = executor.queryForByte("statusCode", Filters.eq("recordId", id)); // cold Mono
     * statusCode.subscribe(code -> process(code));                                             // process receives e.g. (byte) 5
     *
     * // Typical: default for the absent case (note: no bogus 0 from the wrapper type).
     * byte code = executor.queryForByte("statusCode", Filters.eq("recordId", id))
     *                     .defaultIfEmpty((byte) -1).block();    // -1 when no doc / no field
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY.
     * executor.queryForByte("statusCode", Filters.eq("recordId", "x"))
     *         .subscribe(b -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: an empty propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForByte("", Filters.eq("recordId", id)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Byte} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForByte(String, Bson)
     */
    @Beta
    public Mono<Byte> queryForByte(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Byte.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@code Short} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Short} value and then completes. Because this overload
     * is driven by the wrapper type {@code Short.class}, missing/null fields surface as Mono
     * completion (not as the primitive default {@code 0}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the Short value.
     * Mono<Short> quantity = executor.queryForShort("quantity", Filters.eq("productId", id)); // cold Mono
     * quantity.subscribe(q -> System.out.println("Qty: " + q));                               // prints e.g. "Qty: 100"
     *
     * // Typical: default for the absent case.
     * short q = executor.queryForShort("quantity", Filters.eq("productId", id))
     *                   .defaultIfEmpty((short) 0).block();      // 0 only because YOU chose it
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY.
     * executor.queryForShort("quantity", Filters.eq("productId", "x"))
     *         .subscribe(v -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: a null propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForShort(null, Filters.eq("productId", id)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Short} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForShort(String, Bson)
     */
    @Beta
    public Mono<Short> queryForShort(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Short.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as an {@code Integer} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Integer} value and then completes. Because this
     * overload is driven by the wrapper type {@code Integer.class}, missing/null fields surface as
     * Mono completion (not as the primitive default {@code 0}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the Integer value.
     * Mono<Integer> age = executor.queryForInt("age", Filters.eq("userId", "user123")); // cold Mono
     * age.subscribe(a -> System.out.println("Age: " + a));                              // prints e.g. "Age: 25"
     *
     * // Typical: default for the absent case.
     * int age2 = executor.queryForInt("age", Filters.eq("userId", "user123"))
     *                    .defaultIfEmpty(-1).block();            // -1 when no doc / no field
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY (no fabricated 0).
     * executor.queryForInt("age", Filters.eq("userId", "missing"))
     *         .subscribe(v -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: a blank propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForInt("", Filters.eq("userId", "user123")); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Integer} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForInt(String, Bson)
     */
    @Beta
    public Mono<Integer> queryForInt(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Integer.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@code Long} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Long} value and then completes. Because this overload
     * is driven by the wrapper type {@code Long.class}, missing/null fields surface as Mono
     * completion (not as the primitive default {@code 0L}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the Long value.
     * Mono<Long> timestamp = executor.queryForLong("timestamp", Filters.eq("eventId", "evt123")); // cold Mono
     * timestamp.subscribe(t -> System.out.println("Timestamp: " + t));                            // prints e.g. "Timestamp: 1234567890"
     *
     * // Typical: default for the absent case.
     * long ts = executor.queryForLong("timestamp", Filters.eq("eventId", "evt123"))
     *                   .defaultIfEmpty(0L).block();             // 0L only because YOU chose it
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY (no fabricated 0L).
     * executor.queryForLong("timestamp", Filters.eq("eventId", "nope"))
     *         .subscribe(v -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: a null propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForLong(null, Filters.eq("eventId", "evt123")); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Long} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForLong(String, Bson)
     */
    @Beta
    public Mono<Long> queryForLong(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Long.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@code Float} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Float} value and then completes. Because this overload
     * is driven by the wrapper type {@code Float.class}, missing/null fields surface as Mono
     * completion (not as the primitive default {@code 0.0f}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the Float value.
     * Mono<Float> rating = executor.queryForFloat("rating", Filters.eq("productId", id)); // cold Mono
     * rating.subscribe(r -> System.out.println("Rating: " + r));                          // prints e.g. "Rating: 98.5"
     *
     * // Typical: default for the absent case.
     * float r = executor.queryForFloat("rating", Filters.eq("productId", id))
     *                   .defaultIfEmpty(0.0f).block();           // 0.0f only because YOU chose it
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY (no fabricated 0.0f).
     * executor.queryForFloat("rating", Filters.eq("productId", "x"))
     *         .subscribe(v -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: an empty propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForFloat("", Filters.eq("productId", id)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Float} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForDouble(String, Bson)
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForFloat(String, Bson)
     */
    @Beta
    public Mono<Float> queryForFloat(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Float.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@code Double} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Double} value and then completes. Because this overload
     * is driven by the wrapper type {@code Double.class}, missing/null fields surface as Mono
     * completion (not as the primitive default {@code 0.0d}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the Double value.
     * Mono<Double> price = executor.queryForDouble("price", Filters.eq("itemId", id)); // cold Mono
     * price.subscribe(p -> System.out.println("Price: $" + p));                        // prints e.g. "Price: $19.99"
     *
     * // Typical: default for the absent case.
     * double p = executor.queryForDouble("price", Filters.eq("itemId", id))
     *                    .defaultIfEmpty(0.0d).block();          // 0.0d only because YOU chose it
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY (no fabricated 0.0d).
     * executor.queryForDouble("price", Filters.eq("itemId", "x"))
     *         .subscribe(v -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: a null propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForDouble(null, Filters.eq("itemId", id)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Double} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForFloat(String, Bson)
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForDouble(String, Bson)
     */
    @Beta
    public Mono<Double> queryForDouble(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Double.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@code String} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code String} value and then completes. Subscribers that need
     * to distinguish "no document matched" from "document matched but value is null" should use the
     * blocking sync API or supply a default via {@code defaultIfEmpty(...)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the String value.
     * Mono<String> username = executor.queryForString("username", Filters.eq("userId", "u123")); // cold Mono
     * username.subscribe(n -> System.out.println("Username: " + n));                             // prints e.g. "Username: John Doe"
     *
     * // Typical: default for the absent case.
     * String name = executor.queryForString("username", Filters.eq("userId", "u123"))
     *                       .defaultIfEmpty("(unknown)").block(); // "(unknown)" when no doc / no field
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY (no onNext).
     * executor.queryForString("username", Filters.eq("userId", "missing"))
     *         .subscribe(v -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: a blank propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForString("", Filters.eq("userId", "u123")); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code String} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForString(String, Bson)
     */
    @Beta
    public Mono<String> queryForString(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, String.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a {@link Date} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Commonly used for timestamp fields such as {@code createdAt},
     * {@code updatedAt}, or {@code lastLogin}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Date} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the Date value.
     * Mono<Date> createdAt = executor.queryForDate("createdAt", Filters.eq("recordId", id)); // cold Mono
     * createdAt.subscribe(d -> System.out.println("Created: " + d));                         // prints the stored java.util.Date
     *
     * // Typical: default for the absent case.
     * Date d = executor.queryForDate("createdAt", Filters.eq("recordId", id))
     *                  .defaultIfEmpty(new Date(0L)).block();    // epoch when no doc / no field
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY (no onNext).
     * executor.queryForDate("createdAt", Filters.eq("recordId", "x"))
     *         .subscribe(v -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: a null propName throws IllegalArgumentException synchronously (at call time,
     * // before any Mono is returned), because propName is validated eagerly.
     * executor.queryForDate(null, Filters.eq("recordId", id)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Date} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForDate(String, Bson, Class)
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForDate(String, Bson)
     */
    @Beta
    public Mono<Date> queryForDate(final String propName, final Bson filter) {
        return queryForSingleValue(propName, filter, Date.class);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document as a specific {@link Date} subclass value (e.g. {@link java.sql.Timestamp}).
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. The value is converted to the requested {@code Date} subclass.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted typed value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the value as the requested Date subclass.
     * Mono<Timestamp> lastModified = executor.queryForDate(
     *     "lastModified", Filters.eq("docId", id), Timestamp.class); // cold Mono
     * lastModified.subscribe(ts -> log(ts));                         // log receives a java.sql.Timestamp
     *
     * // Typical: default for the absent case.
     * Timestamp ts = executor.queryForDate("lastModified", Filters.eq("docId", id), Timestamp.class)
     *                        .defaultIfEmpty(new Timestamp(0L)).block(); // epoch when absent
     *
     * // Edge: no match or missing/null field -> Mono completes EMPTY (no onNext).
     * executor.queryForDate("lastModified", Filters.eq("docId", "x"), Timestamp.class)
     *         .subscribe(v -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: null valueType -> IllegalArgumentException is thrown synchronously at the call site
     * // (before any Mono is returned), because valueType is validated eagerly.
     * executor.queryForDate("lastModified", Filters.eq("docId", id), (Class<Timestamp>) null); // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the specific Date subtype to return
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @param valueType the specific Date subclass to convert to
     * @return a {@code Mono} that emits the typed {@code Date} value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, if {@code filter} is null, or if {@code valueType} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForDate(String, Bson)
     * @see #queryForSingleValue(String, Bson, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForDate(String, Bson, Class)
     */
    public <T extends Date> Mono<T> queryForDate(final String propName, final Bson filter, final Class<T> valueType) {
        return queryForSingleValue(propName, filter, valueType);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document, converted to the specified type.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. The value is converted to {@code valueType} using the configured codec
     * registry and the {@code N.convert} helper. This is the underlying method delegated to by the
     * primitive-wrapper convenience overloads ({@link #queryForBoolean}, {@link #queryForInt}, etc.).</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted value and then completes. Subscribers cannot distinguish "no
     * document matched" from "document matched but value is null" purely from the reactive signal —
     * use the blocking sync API ({@link com.landawn.abacus.da.mongodb.MongoCollectionExecutor}) when
     * that distinction is required.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the value converted to the requested type.
     * Mono<BigDecimal> balance = executor.queryForSingleValue(
     *     "balance", Filters.eq("accountId", id), BigDecimal.class); // cold Mono
     * balance.subscribe(b -> processBalance(b));                     // processBalance receives a BigDecimal
     *
     * // Typical: a primitive-wrapper type behaves exactly like the queryForInt convenience overload.
     * Integer v = executor.queryForSingleValue("value", Filters.eq("id", 1), Integer.class)
     *                     .block();                              // e.g. 42, or null if absent
     *
     * // Edge: no match, or matched document whose "balance" field is absent/null
     * // -> Mono completes EMPTY (no onNext); there is no separate "matched but null" signal.
     * executor.queryForSingleValue("balance", Filters.eq("accountId", "x"), BigDecimal.class)
     *         .subscribe(b -> {}, e -> {}, () -> System.out.println("absent")); // prints "absent"
     *
     * // Negative: a blank propName or a null valueType throws IllegalArgumentException synchronously
     * // (at call time, before any Mono is returned), because both are validated eagerly.
     * executor.queryForSingleValue("", Filters.eq("accountId", id), BigDecimal.class); // throws IllegalArgumentException
     * }</pre>
     *
     * @param <V> the type of value to retrieve
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @param valueType the Class representing the target type for conversion
     * @return a {@code Mono} that emits the converted field value on subscription, or completes empty
     *         when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, if {@code filter} is null, or if {@code valueType} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#queryForSingleValue(String, Bson, Class)
     */
    public <V> Mono<V> queryForSingleValue(final String propName, final Bson filter, final Class<V> valueType) {
        N.checkArgNotEmpty(propName, "propName");
        N.checkArgNotNull(valueType, "valueType");

        return query(N.asList(propName), filter, null, 0, 1).next().flatMap(doc -> convert(doc, propName, valueType));
    }

    private static <V> Mono<V> convert(final Document doc, final String propName, final Class<V> targetType) {
        return N.isEmpty(doc) ? Mono.empty() : Mono.justOrEmpty(N.convert(getPropValueByPath(doc, propName), targetType));
    }

    // Document.get does a flat key lookup, but a dotted path like "address.city" is a valid projection:
    // the server returns {address: {city: ...}}, so the nested value must be resolved via getEmbedded.
    private static Object getPropValueByPath(final Document doc, final String propName) {
        return propName.indexOf('.') < 0 ? doc.get(propName) : doc.getEmbedded(java.util.Arrays.asList(propName.split("\\.")), Object.class);
    }

    /**
     * Executes a query and returns the results as a {@link Dataset} of {@link Document} rows.
     *
     * <p>Performs a find operation with the specified filter; on subscription, the matching
     * documents are collected into a single {@code Dataset} (which provides tabular data
     * manipulation capabilities) that is emitted once and then the {@code Mono} completes. Even
     * when no documents match, the {@code Mono} emits an empty {@code Dataset} rather than
     * completing empty.</p>
     *
     * <p><b>Note:</b> all matching documents are materialised into memory before the {@code
     * Dataset} is emitted, so this is not suitable for very large result sets — use
     * {@link #list(Bson)} (streaming {@code Flux}) instead in that case.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: materialise all matches into a tabular Dataset.
     * Mono<Dataset> dataset = executor.query(Filters.eq("status", "active")); // cold Mono
     * dataset.subscribe(ds -> System.out.println("rows: " + ds.size()));      // e.g. "rows: 50"
     *
     * // Typical: block and inspect.
     * Dataset ds = executor.query(Filters.eq("status", "active")).block();
     *
     * // Edge: no match -> emits an EMPTY Dataset (size 0); the Mono does NOT complete empty.
     * int rows = executor.query(Filters.eq("status", "no-such")).block().size(); // 0
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is returned).
     * executor.query((Bson) null);                               // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that, on subscription, emits exactly one {@code Dataset} containing
     *         all matching documents (possibly empty), then completes
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#query(Bson)
     */
    public Mono<Dataset> query(final Bson filter) {
        return query(filter, Document.class);
    }

    /**
     * Executes a query and returns results as a typed {@link Dataset}.
     *
     * <p>Materialises every matching document into memory, then emits a single {@code Dataset}
     * whose row schema is derived from {@code rowType}. Even when no documents match, an empty
     * {@code Dataset} is emitted (the {@code Mono} does not complete empty).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Dataset> userDataset = executor.query(filter, User.class);
     * }</pre>
     *
     * @param filter the query filter to match documents against (must not be null)
     * @param rowType the Class representing the row type for the Dataset; must not be null
     * @return a {@code Mono} that, on subscription, emits exactly one {@code Dataset} (possibly
     *         empty) and then completes
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     */
    public Mono<Dataset> query(final Bson filter, final Class<?> rowType) {
        return query(null, filter, rowType);
    }

    /**
     * Executes a paginated query and returns results as a typed Dataset.
     *
     * <p>Performs a paginated query with the specified filter, offset, and count,
     * returning results in a Dataset structure with typed rows.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Dataset> pageData = executor.query(filter, 20, 10, User.class);
     * }</pre>
     *
     * @param filter the query filter to match documents against (must not be null)
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with typed rows within the specified range
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site), or if offset or count is negative
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     */
    public Mono<Dataset> query(final Bson filter, final int offset, final int count, final Class<?> rowType) {
        return query(null, filter, offset, count, rowType);
    }

    /**
     * Executes a query with field projection and returns results as a typed Dataset.
     *
     * <p>Performs a query with field projection, returning only specified fields
     * in a Dataset structure with typed rows for efficient data processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "department");
     * Mono<Dataset> employeeData = executor.query(fields, filter, Employee.class);
     * }</pre>
     *
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with projected fields and typed rows
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Class<?> rowType) {
        return query(selectPropNames, filter, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a paginated query with field projection and returns results as a typed Dataset.
     *
     * <p>Combines field projection with pagination to return a limited set of documents
     * with only specified fields in a Dataset structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("id", "name", "email");
     * Mono<Dataset> userData = executor.query(fields, filter, 0, 50, User.class);
     * }</pre>
     *
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with projected fields and typed rows within range
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site), or if offset or count is negative
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count, final Class<?> rowType) {
        return query(selectPropNames, filter, null, offset, count, rowType);
    }

    /**
     * Executes a sorted query with field projection and returns results as a typed Dataset.
     *
     * <p>Performs a sorted query with field projection, returning ordered results
     * with only specified fields in a Dataset structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score");
     * Bson sort = Sorts.descending("score");
     * Mono<Dataset> leaderboard = executor.query(fields, filter, sort, Player.class);
     * }</pre>
     *
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with projected fields and sorted typed rows
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<?> rowType) {
        return query(selectPropNames, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a fully customized query and returns results as a typed Dataset.
     *
     * <p>Performs a query with complete control over projection, filtering, sorting,
     * and pagination, returning results in a Dataset structure for advanced data processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("id", "name", "salary");
     * Bson sort = Sorts.descending("salary");
     * Mono<Dataset> topEarners = executor.query(fields, filter, sort, 0, 10, Employee.class);
     * }</pre>
     *
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with all specified constraints applied
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site), or if offset or count is negative
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<?> rowType) {
        checkResultClass(rowType);

        if (N.isEmpty(selectPropNames)) {
            return query(selectPropNames, filter, sort, offset, count).collectList().map(rowList -> MongoDB.extractData(rowList, rowType));
        } else {
            return query(selectPropNames, filter, sort, offset, count).collectList().map(rowList -> MongoDB.extractData(selectPropNames, rowList, rowType));
        }
    }

    /**
     * Executes a query with Bson projection and returns results as a typed Dataset.
     *
     * <p>Uses a Bson projection object for complex field specifications combined
     * with filtering and sorting, returning results in a Dataset structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.include("name", "status");
     * Mono<Dataset> statusReport = executor.query(projection, filter, sort, Task.class);
     * }</pre>
     *
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with projected and sorted typed rows
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     */
    public Mono<Dataset> query(final Bson projection, final Bson filter, final Bson sort, final Class<?> rowType) {
        return query(projection, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a fully customized query with Bson projection and returns results as a typed Dataset.
     *
     * <p>Provides maximum query flexibility with Bson projection, filtering, sorting,
     * and pagination, returning results in a Dataset for complex data analysis.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name"), Projections.computed("total", "$sum"));
     * Mono<Dataset> analysis = executor.query(projection, filter, sort, 0, 100, Report.class);
     * }</pre>
     *
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against (must not be null)
     * @param sort the sort criteria to determine document order
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with all specified constraints applied
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site), or if offset or count is negative
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     */
    public Mono<Dataset> query(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count, final Class<?> rowType) {
        checkResultClass(rowType);

        return executeQuery(projection, filter, sort, offset, count).collectList().map(rowList -> MongoDB.extractData(rowList, rowType));
    }

    // Mirrors the validation MongoDBBase.extractData(Collection, MongoIterable, Class) applies on the sync
    // side, so an invalid rowType fails fast with the same IllegalArgumentException instead of producing a
    // malformed Dataset on subscription.
    private static void checkResultClass(final Class<?> rowType) {
        if (!(Beans.isBeanClass(rowType) || Map.class.isAssignableFrom(rowType))) {
            throw new IllegalArgumentException("The target class must be an entity class with getter/setter methods or Map.class/Document.class. But it is: "
                    + ClassUtil.getCanonicalClassName(rowType));
        }
    }

    private static <T> Function<Document, T> toEntity(final Class<T> rowType) {
        return doc -> shouldReturnNullForEmptyDocument(doc, rowType) ? null : MongoDB.readRow(doc, rowType);
    }

    private static boolean shouldReturnNullForEmptyDocument(final Document doc, final Class<?> rowType) {
        if (doc == null) {
            return true;
        }

        if (doc.isEmpty() == false) {
            return false;
        }

        final Type<?> targetType = rowType == null ? null : N.typeOf(rowType);

        return targetType != null && targetType.isObjectArray() == false && targetType.isCollection() == false && targetType.isMap() == false
                && targetType.isBean() == false;
    }

    private Flux<Document> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        if (N.isEmpty(selectPropNames)) {
            return executeQuery(null, filter, sort, offset, count);
        } else if (selectPropNames instanceof List) {
            return executeQuery(Projections.include((List<String>) selectPropNames), filter, sort, offset, count);
        } else {
            return executeQuery(Projections.include(new ArrayList<>(selectPropNames)), filter, sort, offset, count);
        }
    }

    private Flux<Document> executeQuery(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        N.checkArgNotNull(filter, "filter");

        N.checkArgNotNegative(offset, "offset");
        N.checkArgNotNegative(count, "count");

        if (count == 0) {
            // The MongoDB driver treats limit(0) as "no limit" (return all matching documents).
            // A request for zero documents must yield zero documents, so use an always-false filter.
            return Flux.from(coll.find(new Document(_$EXPR, false)));
        }

        FindPublisher<Document> findIterable = coll.find(filter);

        if (projection != null) {
            findIterable = findIterable.projection(projection);
        }

        if (sort != null) {
            findIterable = findIterable.sort(sort);
        }

        if (offset > 0) {
            findIterable = findIterable.skip(offset);
        }

        if (count < Integer.MAX_VALUE) {
            findIterable = findIterable.limit(count);
        }

        return Flux.from(findIterable);
    }

    /**
     * Opens a change stream watching for changes in the collection.
     *
     * <p>Returns a hot, long-lived {@link ChangeStreamPublisher} that emits a
     * {@link com.mongodb.client.model.changestream.ChangeStreamDocument}-wrapped {@link Document}
     * for each change event as it occurs in the collection. Unlike the {@code Mono}/{@code Flux}
     * returned by other reactive methods, the change-stream {@code Publisher} does not naturally
     * complete — it stays open until the subscription is cancelled or the cursor is invalidated.
     * Use {@code Flux.from(...)} to integrate with Reactor.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: subscribe and process change events as they arrive (hot, never completes on its own).
     * ChangeStreamPublisher<Document> changeStream = executor.watch(); // returned synchronously; never null
     * Disposable sub = Flux.from(changeStream)
     *                      .subscribe(change -> processChange(change)); // runs until cancelled / invalidated
     *
     * // Edge: nothing happens until subscribed — merely calling watch() opens no cursor.
     * ChangeStreamPublisher<Document> cold = executor.watch();        // no server-side cursor yet
     *
     * // Edge: cancel the subscription to close the server-side cursor (it will not complete by itself).
     * sub.dispose();                                                  // cursor closed
     * }</pre>
     *
     * @return a {@link ChangeStreamPublisher} that emits each change event as it occurs
     * @see com.mongodb.reactivestreams.client.MongoCollection#watch()
     */
    public ChangeStreamPublisher<Document> watch() {
        return coll.watch();
    }

    /**
     * Creates a typed change stream to watch for changes in the collection.
     *
     * <p>Returns a hot, long-lived {@link ChangeStreamPublisher} whose
     * {@code ChangeStreamDocument.fullDocument} is decoded as {@code rowType} using the configured
     * codec registry. Like {@link #watch()}, the stream does not naturally complete — cancel the
     * subscription to close the cursor.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: each event's fullDocument is decoded as the given type.
     * ChangeStreamPublisher<UserChangeEvent> userChanges = executor.watch(UserChangeEvent.class); // never null
     * Disposable sub = Flux.from(userChanges)
     *                      .subscribe(event -> handleUserChange(event)); // hot; runs until cancelled
     *
     * // Edge: nothing is opened until subscription — the call itself performs no I/O.
     * ChangeStreamPublisher<UserChangeEvent> cold = executor.watch(UserChangeEvent.class); // cold until subscribed
     *
     * // Edge: dispose to close the cursor (the stream does not complete on its own).
     * sub.dispose();                                                  // cursor closed
     * }</pre>
     *
     * @param <T> the type to decode each change event's full document into
     * @param rowType the Class representing the target type for change events; must not be null
     * @return a {@link ChangeStreamPublisher} that emits typed change events as they occur
     * @see com.mongodb.reactivestreams.client.MongoCollection#watch(Class)
     */
    public <T> ChangeStreamPublisher<T> watch(final Class<T> rowType) {
        return coll.watch(rowType);
    }

    /**
     * Creates a change stream with an aggregation pipeline to watch filtered changes.
     *
     * <p>Returns a hot, long-lived {@link ChangeStreamPublisher} that runs each change event through
     * the supplied aggregation pipeline before publishing it. As with {@link #watch()}, the stream
     * does not naturally complete — cancel the subscription to close the cursor.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.in("operationType", "insert", "update"))
     * );
     * ChangeStreamPublisher<Document> filteredChanges = executor.watch(pipeline);
     * }</pre>
     *
     * @param pipeline the aggregation pipeline applied to change events; must not be null
     * @return a {@link ChangeStreamPublisher} emitting filtered change events as {@link Document}
     * @see com.mongodb.reactivestreams.client.MongoCollection#watch(List)
     */
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return coll.watch(pipeline);
    }

    /**
     * Creates a typed change stream with an aggregation pipeline to watch filtered changes.
     *
     * <p>Combines pipeline filtering with type conversion for change events, providing both
     * selectivity and type-safe full-document decoding. The returned hot publisher does not
     * naturally complete — cancel the subscription to close the server-side cursor.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.eq("fullDocument.status", "critical"))
     * );
     * ChangeStreamPublisher<Alert> alerts = executor.watch(pipeline, Alert.class);
     * }</pre>
     *
     * @param <T> the type to decode each change event's full document into
     * @param pipeline the aggregation pipeline applied to change events; must not be null
     * @param rowType the Class representing the target type for change events; must not be null
     * @return a {@link ChangeStreamPublisher} emitting filtered and typed change events
     * @see com.mongodb.reactivestreams.client.MongoCollection#watch(List, Class)
     */
    public <T> ChangeStreamPublisher<T> watch(final List<? extends Bson> pipeline, final Class<T> rowType) {
        return coll.watch(pipeline, rowType);
    }

    /**
     * Inserts a single document into the collection in a reactive manner.
     *
     * <p>Inserts the provided object as a document in the collection. The object can be a
     * {@link Document}, {@link java.util.Map}, or an entity class with getter/setter methods
     * (non-Document inputs are converted via {@code MongoDBBase.toDocument}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: insert an entity; the result carries the generated _id.
     * User newUser = new User("John", "john@example.com");
     * Mono<InsertOneResult> result = executor.insertOne(newUser);   // cold; insert runs on subscription
     * result.subscribe(r -> System.out.println(r.getInsertedId())); // e.g. a BsonObjectId value
     *
     * // Typical: a raw Document is inserted as-is (no conversion).
     * executor.insertOne(new Document("name", "Jane")).block();   // emits one InsertOneResult
     *
     * // Edge: cold publisher — re-subscribing re-issues the insert (a second document is written).
     * Mono<InsertOneResult> twice = executor.insertOne(new Document("name", "Dup"));
     * twice.block();                                              // insert #1
     * twice.block();                                              // insert #2 (separate write)
     *
     * // Edge/Negative: a duplicate unique key surfaces as a MongoWriteException via onError,
     * // not as a thrown exception at call time.
     * }</pre>
     *
     * @param obj the object to insert (Document/Map/entity class); must not be null
     * @return a {@code Mono} that, on subscription, emits exactly one {@link InsertOneResult}
     *         describing the operation, then completes
     * @throws IllegalArgumentException if obj is null (thrown synchronously at call time, before
     *         the {@code Mono} is built)
     * @throws com.mongodb.MongoWriteException if the insert violates a unique constraint or
     *         document validation (signalled via {@code Mono})
     * @see #insertOne(Object, InsertOneOptions)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#insertOne(Object)
     */
    public Mono<InsertOneResult> insertOne(final Object obj) {
        return insertOne(obj, null);
    }

    /**
     * Inserts a single document with options into the collection reactively.
     *
     * <p>Inserts the provided object with custom insert options, such as bypass document
     * validation. Non-{@link Document} input is converted via {@code MongoDBBase.toDocument}
     * before insertion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
     * Mono<InsertOneResult> result = executor.insertOne(entity, options);
     * }</pre>
     *
     * @param obj the object to insert (Document/Map/entity class); must not be null
     * @param options the options to apply to the insert operation; may be null for driver defaults
     * @return a {@code Mono} that, on subscription, emits exactly one {@link InsertOneResult}, then
     *         completes
     * @throws IllegalArgumentException if obj is null (thrown synchronously at call time, before the
     *         {@code Mono} is built)
     * @throws com.mongodb.MongoWriteException if the insert violates a unique constraint or
     *         document validation (signalled via {@code Mono})
     */
    public Mono<InsertOneResult> insertOne(final Object obj, final InsertOneOptions options) {
        N.checkArgNotNull(obj, "obj");

        if (options == null) {
            return Mono.from(coll.insertOne(toDocument(obj)));
        } else {
            return Mono.from(coll.insertOne(toDocument(obj), options));
        }
    }

    /**
     * Inserts multiple documents into the collection reactively.
     *
     * <p>Batch inserts a collection of objects as documents. Each element may be a
     * {@link Document}, {@link java.util.Map}, or an entity class — non-Document elements are
     * converted via {@code MongoDBBase.toDocument}. A bulk insert is more efficient than multiple
     * single inserts.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: batch-insert a list of entities.
     * List<User> users = Arrays.asList(new User("Alice"), new User("Bob"));
     * Mono<InsertManyResult> result = executor.insertMany(users);           // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getInsertedIds().size())); // e.g. 2
     *
     * // Typical: a mix of Documents/Maps/entities is accepted (non-Documents are converted).
     * executor.insertMany(Arrays.asList(new Document("k", 1), Map.of("k", 2))).block(); // one InsertManyResult
     *
     * // Edge/Negative: a null collection is rejected synchronously (at call time).
     * executor.insertMany(null);                                  // throws IllegalArgumentException
     *
     * // Edge/Negative: an empty collection is also rejected synchronously.
     * executor.insertMany(Collections.emptyList());              // throws IllegalArgumentException
     * }</pre>
     *
     * @param objList the collection of objects to insert (Document/Map/entity classes);
     *                must not be null or empty
     * @return a {@code Mono} that, on subscription, emits exactly one {@link InsertManyResult}, then
     *         completes
     * @throws IllegalArgumentException if {@code objList} is null or empty
     * @throws com.mongodb.MongoBulkWriteException if any insert violates a unique constraint or
     *         document validation (signalled via {@code Mono})
     * @see #insertMany(Collection, InsertManyOptions)
     */
    public Mono<InsertManyResult> insertMany(final Collection<?> objList) {
        return insertMany(objList, null);
    }

    /**
     * Inserts multiple documents with options into the collection reactively.
     *
     * <p>Batch inserts a collection of objects with custom insert options, such as
     * ordered/unordered insertion. With {@code ordered(false)}, the driver continues after
     * per-document failures and reports them in the resulting bulk-write exception.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * InsertManyOptions options = new InsertManyOptions().ordered(false);
     * Mono<InsertManyResult> result = executor.insertMany(documents, options);
     * }</pre>
     *
     * @param objList the collection of objects to insert (Document/Map/entity classes);
     *                must not be null or empty
     * @param options the options to apply to the insert operation; may be null for driver defaults
     * @return a {@code Mono} that, on subscription, emits exactly one {@link InsertManyResult}, then
     *         completes
     * @throws IllegalArgumentException if {@code objList} is null or empty
     * @throws com.mongodb.MongoBulkWriteException if any insert fails (signalled via {@code Mono})
     */
    public Mono<InsertManyResult> insertMany(final Collection<?> objList, final InsertManyOptions options) {
        N.checkArgNotEmpty(objList, "objList");

        final List<Document> docs = toDocument(objList);

        if (options == null) {
            return Mono.from(coll.insertMany(docs));
        } else {
            return Mono.from(coll.insertMany(docs, options));
        }
    }

    private static Document toDocument(final Object obj) {
        return obj instanceof Document ? (Document) obj : MongoDBBase.toDocument(obj);
    }

    private List<Document> toDocument(final Collection<?> objList) {
        List<Document> docs = null;
        boolean allDocuments = true;

        for (final Object obj : objList) {
            if (!(obj instanceof Document)) {
                allDocuments = false;
                break;
            }
        }

        if (allDocuments) {
            if (objList instanceof List) {
                docs = (List<Document>) objList;
            } else {
                docs = new ArrayList<>((Collection<Document>) objList);
            }
        } else {
            docs = new ArrayList<>(objList.size());

            for (final Object entity : objList) {
                docs.add(MongoDBBase.toDocument(entity));
            }
        }

        return docs;
    }

    /**
     * Updates a single document by string ObjectId reactively.
     *
     * <p>Updates the document with the specified ObjectId using the provided update object. The update
     * object (Bson/Document/Map/entity class) is converted to a Bson update; if it does not already
     * begin with an update operator, it is automatically wrapped in a {@code $set} operator.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: a plain object is auto-wrapped in {$set: ...}.
     * String objectId = "507f1f77bcf86cd799439011";
     * User updates = new User(); updates.setStatus("active");
     * Mono<UpdateResult> result = executor.updateOne(objectId, updates); // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getModifiedCount()));   // e.g. 1 (0 if no doc matched)
     *
     * // Typical: a Document that already starts with an operator is passed through unchanged.
     * executor.updateOne(objectId, new Document("$inc", new Document("loginCount", 1))).block(); // returns an UpdateResult
     *
     * // Edge: no document has that id -> emits an UpdateResult with matchedCount/modifiedCount == 0
     * // (not an error, not empty).
     *
     * // Edge/Negative: an invalid ObjectId hex string throws IllegalArgumentException synchronously
     * // (at call time), because the id is parsed eagerly.
     * executor.updateOne("not-a-valid-id", updates);            // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the document's ObjectId
     * @param update the update specification (Bson/Document/Map/entity class)
     * @return a Mono that emits the UpdateResult containing details about the operation including
     *         matched count, modified count, and upserted id if applicable
     * @throws IllegalArgumentException if objectId is null, empty, or not a valid ObjectId hex string,
     *         or if update is null (thrown synchronously at the call site)
     */
    public Mono<UpdateResult> updateOne(final String objectId, final Object update) {
        return updateOne(createObjectId(objectId), update);
    }

    /**
     * Updates a single document by ObjectId reactively.
     *
     * <p>Updates the document with the specified ObjectId using the provided update object.
     * The update is automatically wrapped in a $set operator unless it already contains operators.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Document updates = new Document("status", "completed");
     * Mono<UpdateResult> result = executor.updateOne(id, updates);
     * }</pre>
     *
     * @param objectId the ObjectId of the document to update
     * @param update the update specification (Bson/Document/Map/entity class)
     * @return a Mono that emits the UpdateResult containing details about the operation including
     *         matched count, modified count, and upserted id if applicable
     * @throws IllegalArgumentException if objectId or update is null (thrown synchronously at the call site)
     */
    public Mono<UpdateResult> updateOne(final ObjectId objectId, final Object update) {
        return updateOne(MongoDBBase.objectIdToFilter(objectId), update);
    }

    /**
     * Updates the first document matching the filter reactively.
     *
     * <p>Updates the first document that matches the filter criteria with the provided
     * update specification. The update is automatically wrapped in appropriate update operators.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("email", "user@example.com");
     *
     * // Typical: a non-operator Document is auto-wrapped in {$set: {lastLogin: ...}}.
     * Document update = new Document("lastLogin", new Date());
     * Mono<UpdateResult> result = executor.updateOne(filter, update);  // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getModifiedCount())); // e.g. 1
     *
     * // Typical: a driver-built update (Updates.xxx) is used as-is (NOT re-wrapped in $set).
     * executor.updateOne(filter, Updates.set("verified", true)).block(); // emits one UpdateResult
     *
     * // Edge: only the FIRST matching document is updated, even if several match.
     *
     * // Edge: no match -> emits an UpdateResult with matchedCount == 0 (not empty, not an error).
     * long matched = executor.updateOne(Filters.eq("email", "nobody"), update).block().getMatchedCount(); // 0
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param update the update specification (Bson/Document/Map/entity class)
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Object update) {
        return updateOne(filter, update, null);
    }

    /**
     * Updates the first document matching the filter with options reactively.
     *
     * <p>Updates the first matching document with custom update options, such as
     * upsert (insert if not exists) or array filters for nested array updates.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * Bson filter = Filters.eq("userId", 123);
     * Mono<UpdateResult> result = executor.updateOne(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param update the update specification (Bson/Document/Map/entity class)
     * @param options the options to apply to the update operation
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Object update, final UpdateOptions options) {
        N.checkArgNotNull(filter, "filter");

        if (options == null) {
            return Mono.from(coll.updateOne(filter, toBson(update)));
        } else {
            return Mono.from(coll.updateOne(filter, toBson(update), options));
        }
    }

    /**
     * Updates the first document matching the filter using a pipeline of update stages reactively.
     *
     * <p>Applies a list of update operations as a MongoDB <i>pipeline-style</i> update
     * (server requires MongoDB 4.2+). Each element is converted via {@link #toBson(Object)}; any
     * non-operator object is wrapped in a {@code $set}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: a pipeline-style update (MongoDB 4.2+).
     * List<Bson> pipeline = Arrays.asList(
     *     Updates.set("total", new Document("$sum", Arrays.asList("$price", "$tax")))
     * );
     * Mono<UpdateResult> result = executor.updateOne(filter, pipeline); // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getModifiedCount()));  // e.g. 1
     *
     * // Edge: no match -> emits an UpdateResult with matchedCount == 0 (not empty, not an error).
     *
     * // Edge/Negative: an empty pipeline is rejected synchronously (at call time).
     * executor.updateOne(filter, Collections.emptyList());      // throws IllegalArgumentException
     *
     * // Edge/Negative: a null pipeline is likewise rejected synchronously.
     * executor.updateOne(filter, (Collection<?>) null);         // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to identify the document to update; must not be null
     * @param objList the pipeline of update stages to apply; must not be null or empty
     * @return a {@code Mono} that emits the {@link UpdateResult} containing operation details
     * @throws IllegalArgumentException if {@code objList} is null or empty
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Collection<?> objList) {
        return updateOne(filter, objList, null);
    }

    /**
     * Updates the first document matching the filter using a collection of update operations with options reactively.
     *
     * <p>Applies multiple update operations from a collection to the first matching document with custom options.
     * This corresponds to MongoDB's pipeline-style update form, enabling complex field transformations
     * with upsert and other update behaviors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * List<Bson> pipeline = Arrays.asList(Updates.addFields(new Field("modified", "$$NOW")));
     * Mono<UpdateResult> result = executor.updateOne(filter, pipeline, options);
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param objList the collection of update operations to apply
     * @param options the options to apply to the update operation
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Collection<?> objList, final UpdateOptions options) {
        N.checkArgNotNull(filter, "filter");

        final List<Bson> updateToUse = toBson(objList);

        if (options == null) {
            return Mono.from(coll.updateOne(filter, updateToUse));
        } else {
            return Mono.from(coll.updateOne(filter, updateToUse, options));
        }
    }

    private static ObjectId createObjectId(final String objectId) {
        N.checkArgNotEmpty(objectId, "objectId");

        return new ObjectId(objectId);
    }

    private static Bson toBson(final Object update) {
        N.checkArgNotNull(update, "update");

        // Note: the second argument (isForUpdate) on MongoDBBase.toDocument is a dead flag, AND
        // MongoCollectionExecutor cannot see the protected (Object, boolean) overload from this
        // package — passing it silently resolved to the public Object... varargs and treated
        // (update, true) as a name/value pair, casting update to String -> ClassCastException
        // for Map/entity updates. Call the public single-arg overload directly.
        final Bson bson = update instanceof Bson ? (Bson) update : MongoDBBase.toDocument(update);

        final Bson bsonToUse;

        if (bson instanceof final Document doc) {
            if (!doc.isEmpty() && doc.keySet().iterator().next().startsWith(_$)) {
                return doc;
            }

            // A caller-supplied Document must not be mutated: strip _id from a copy.
            final Document docToUse = doc == update ? new Document(doc) : doc;
            docToUse.remove(MongoDBBase._ID);
            bsonToUse = docToUse;
        } else if (bson instanceof final BasicDBObject dbObject) { //NOSONAR
            if (!dbObject.isEmpty() && dbObject.keySet().iterator().next().startsWith(_$)) {
                return dbObject;
            }

            // Always caller-supplied (toDocument never returns BasicDBObject): strip _id from a copy.
            final BasicDBObject dbObjectToUse = new BasicDBObject(dbObject);
            dbObjectToUse.remove(MongoDBBase._ID);
            bsonToUse = dbObjectToUse;
        } else {
            // A driver-built Bson (e.g. Updates.set(...)/Updates.combine(...)) is already a
            // complete update expression. It is neither Document nor BasicDBObject, so it must
            // be returned as-is rather than (incorrectly) re-wrapped in a {$set: ...} document.
            return bson;
        }

        // _id was stripped above: it is immutable server-side, and {$set: {_id: ...}} fails for every
        // matched document whose _id differs — breaking the common "fetch entity, modify a field,
        // update by filter" pattern. The server also rejects an empty $set; fail fast instead.
        if (bsonToUse instanceof final Document doc && doc.isEmpty() || bsonToUse instanceof final BasicDBObject dbObject && dbObject.isEmpty()) {
            throw new IllegalArgumentException(
                    "The update payload is empty (no non-null updatable properties besides the immutable _id). MongoDB rejects an empty $set");
        }

        return new Document(_$SET, bsonToUse);
    }

    private List<Bson> toBson(final Collection<?> objList) {
        N.checkArgNotEmpty(objList, "objList");

        List<Bson> docs = null;
        boolean allBson = true;

        for (final Object obj : objList) {
            if (!(obj instanceof Bson)) {
                allBson = false;
                break;
            }
        }

        if (allBson) {
            if (objList instanceof List) {
                docs = (List<Bson>) objList;
            } else {
                docs = new ArrayList<>((Collection<Bson>) objList);
            }
        } else {
            docs = new ArrayList<>(objList.size());

            for (final Object entity : objList) {
                docs.add(toBson(entity));
            }
        }

        return docs;
    }

    /**
     * Updates all documents matching the filter reactively.
     *
     * <p>Updates all documents that match the filter criteria with the provided update
     * specification. The update (Bson/Document/Map/entity class) is converted to a Bson update; if it
     * does not already begin with an update operator, it is automatically wrapped in a {@code $set}
     * operator. This method is useful for bulk updates where you need to modify multiple documents
     * with the same update operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "pending");
     *
     * // Typical: updates EVERY matching document.
     * Document update = new Document("$set", new Document("status", "processed"));
     * Mono<UpdateResult> result = executor.updateMany(filter, update); // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getModifiedCount())); // e.g. 42
     *
     * // Typical: a non-operator object is auto-wrapped in {$set: ...}.
     * executor.updateMany(filter, new Document("processedAt", new Date())).block(); // one UpdateResult
     *
     * // Edge: no match -> emits an UpdateResult with matchedCount/modifiedCount == 0 (not empty).
     * long matched = executor.updateMany(Filters.eq("status", "no-such"), update).block().getMatchedCount(); // 0
     *
     * // Edge: cold publisher — re-subscribing re-issues the bulk update.
     * }</pre>
     *
     * @param filter the query filter to identify documents to update; must not be null
     * @param update the update specification which can be a Bson document, Document, Map, or entity class;
     *               must not be null
     * @return a Mono that emits the UpdateResult containing details about the operation including
     *         matched count, modified count, and upserted id if applicable
     * @throws IllegalArgumentException if filter or update is null
     * @see #updateMany(Bson, Object, UpdateOptions)
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Object update) {
        return updateMany(filter, update, null);
    }

    /**
     * Updates all documents matching the filter with custom options reactively.
     *
     * <p>Updates all matching documents with custom update options. This method provides full control
     * over the update operation including upsert behavior, validation bypass, array filters, and
     * collation settings. It's particularly useful for bulk updates with specific requirements
     * such as creating documents if they don't exist (upsert) or bypassing schema validation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions()
     *     .upsert(true)
     *     .bypassDocumentValidation(true);
     * Bson filter = Filters.lt("version", 2);
     * Document update = new Document("$set", new Document("version", 2));
     * Mono<UpdateResult> result = executor.updateMany(filter, update, options);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update; must not be null
     * @param update the update specification which can be a Bson document, Document, Map, or entity class;
     *               must not be null
     * @param options the options to apply to the update operation such as upsert, validation bypass,
     *                array filters, or collation; may be null to use default options
     * @return a Mono that emits the UpdateResult containing details about the operation including
     *         matched count, modified count, and upserted id if applicable
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Object update, final UpdateOptions options) {
        N.checkArgNotNull(filter, "filter");

        if (options == null) {
            return Mono.from(coll.updateMany(filter, toBson(update)));
        } else {
            return Mono.from(coll.updateMany(filter, toBson(update), options));
        }
    }

    /**
     * Updates all documents matching the filter using a collection of update operations.
     *
     * <p>Applies multiple update operations from a collection to all matching documents.
     * This method is useful when you need to apply a pipeline of update operations or
     * multiple field updates in a specific order.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.currentDate("lastModified")
     * );
     * Mono<UpdateResult> result = executor.updateMany(filter, updates);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update; must not be null
     * @param objList collection of update operations to apply; must not be null or empty
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @see #updateMany(Bson, Collection, UpdateOptions)
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Collection<?> objList) {
        return updateMany(filter, objList, null);
    }

    /**
     * Updates all documents matching the filter using a collection of update operations with options.
     *
     * <p>Applies multiple update operations from a collection to all matching documents with
     * custom update options. This provides maximum flexibility for complex bulk update scenarios
     * where multiple operations need to be applied in sequence with specific behaviors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * List<Bson> updates = Arrays.asList(
     *     Updates.inc("count", 1),
     *     Updates.addToSet("tags", "processed")
     * );
     * Mono<UpdateResult> result = executor.updateMany(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update; must not be null
     * @param objList collection of update operations to apply; must not be null or empty
     * @param options the options to apply to the update operation; may be null for defaults
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Collection<?> objList, final UpdateOptions options) {
        N.checkArgNotNull(filter, "filter");
        N.checkArgNotEmpty(objList, "objList");

        final List<Bson> updateToUse = toBson(objList);

        if (options == null) {
            return Mono.from(coll.updateMany(filter, updateToUse));
        } else {
            return Mono.from(coll.updateMany(filter, updateToUse, options));
        }
    }

    /**
     * Replaces a single document identified by its ObjectId string reactively.
     *
     * <p>Completely replaces the document with the specified ObjectId with a new document in a reactive manner.
     * Unlike update operations, replace operations replace the entire document except for
     * the _id field. This is useful when you need to completely overwrite a document's content. Returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: completely overwrite the document (the _id is preserved).
     * String id = "507f1f77bcf86cd799439011";
     * Document newDoc = new Document("name", "John").append("age", 30);
     * Mono<UpdateResult> result = executor.replaceOne(id, newDoc);     // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getModifiedCount())); // 1 if replaced, else 0
     *
     * // Typical: a non-Document replacement (Map/entity) is converted before replacing.
     * executor.replaceOne(id, Map.of("name", "Jane", "age", 31)).block(); // emits one UpdateResult
     *
     * // Edge: no document has that id -> emits an UpdateResult with matchedCount == 0.
     *
     * // Edge/Negative: an invalid ObjectId hex string throws IllegalArgumentException synchronously
     * // (at call time), because the id is parsed eagerly.
     * executor.replaceOne("xyz", newDoc);                       // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId string representation of the ObjectId; must be a valid 24-character hex string
     * @param replacement the replacement document which can be Document, Map, or entity class with
     *                    getter/setter methods; must not be null
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if objectId is invalid or replacement is null
     * @see #replaceOne(ObjectId, Object)
     */
    public Mono<UpdateResult> replaceOne(final String objectId, final Object replacement) {
        return replaceOne(createObjectId(objectId), replacement);
    }

    /**
     * Replaces a single document identified by its ObjectId reactively.
     *
     * <p>Completely replaces the document with the specified ObjectId with a new document in a reactive manner.
     * This method provides type-safe ObjectId handling for document replacement operations and returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * MyEntity entity = new MyEntity("John", 30);
     * Mono<UpdateResult> result = executor.replaceOne(id, entity);
     * }</pre>
     *
     * @param objectId the ObjectId of the document to replace; must not be null
     * @param replacement the replacement document which can be Document, Map, or entity class with
     *                    getter/setter methods; must not be null
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if objectId or replacement is null
     * @see #replaceOne(Bson, Object)
     */
    public Mono<UpdateResult> replaceOne(final ObjectId objectId, final Object replacement) {
        return replaceOne(MongoDBBase.objectIdToFilter(objectId), replacement);
    }

    /**
     * Replaces a single document matching the filter reactively.
     *
     * <p>Replaces the first document that matches the filter with the replacement document in a reactive manner.
     * If multiple documents match the filter, only the first one encountered is replaced.
     * The _id field is preserved from the original document. Returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "olduser");
     * Document replacement = new Document("username", "newuser").append("updated", true);
     * Mono<UpdateResult> result = executor.replaceOne(filter, replacement);
     * }</pre>
     *
     * @param filter the query filter to identify the document to replace; must not be null
     * @param replacement the replacement document which can be Document, Map, or entity class with
     *                    getter/setter methods; must not be null
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or replacement is null
     * @see #replaceOne(Bson, Object, ReplaceOptions)
     */
    public Mono<UpdateResult> replaceOne(final Bson filter, final Object replacement) {
        return replaceOne(filter, replacement, null);
    }

    /**
     * Replaces a single document matching the filter with custom options reactively.
     *
     * <p>Replaces the first matching document with custom replace options in a reactive manner. This method provides
     * full control over the replace operation including upsert behavior, validation bypass,
     * and collation settings. It's particularly useful when you need to create a document
     * if it doesn't exist (upsert). Returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ReplaceOptions options = new ReplaceOptions().upsert(true);
     * Bson filter = Filters.eq("_id", documentId);
     * Document replacement = new Document("name", "John").append("version", 2);
     * Mono<UpdateResult> result = executor.replaceOne(filter, replacement, options);
     * }</pre>
     *
     * @param filter the query filter to identify the document to replace; must not be null
     * @param replacement the replacement document which can be Document, Map, or entity class with
     *                    getter/setter methods; must not be null
     * @param options the options to apply to the replace operation such as upsert or validation bypass;
     *                may be null to use default options
     * @return a Mono that emits the UpdateResult containing operation details including matched count,
     *         modified count, and upserted id if applicable
     * @throws IllegalArgumentException if filter or replacement is null
     */
    public Mono<UpdateResult> replaceOne(final Bson filter, final Object replacement, final ReplaceOptions options) {
        N.checkArgNotNull(filter, "filter");
        N.checkArgNotNull(replacement, "replacement");

        if (options == null) {
            return Mono.from(coll.replaceOne(filter, toDocument(replacement)));
        } else {
            return Mono.from(coll.replaceOne(filter, toDocument(replacement), options));
        }
    }

    /**
     * Deletes a single document by its ObjectId string reactively.
     *
     * <p>Deletes the document with the specified ObjectId in a reactive manner. This is a convenience method
     * for the common case of deleting a document by its primary key, returning a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String documentId = "507f1f77bcf86cd799439011";
     * Mono<DeleteResult> result = executor.deleteOne(documentId);
     * }</pre>
     *
     * @param objectId string representation of the ObjectId; must be a valid 24-character hex string
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if objectId is invalid
     * @see #deleteOne(ObjectId)
     */
    public Mono<DeleteResult> deleteOne(final String objectId) {
        return deleteOne(createObjectId(objectId));
    }

    /**
     * Deletes a single document by its ObjectId reactively.
     *
     * <p>Deletes the document with the specified ObjectId in a reactive manner. This method provides type-safe
     * ObjectId handling for deletion operations and returns a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<DeleteResult> result = executor.deleteOne(id);
     * }</pre>
     *
     * @param objectId the ObjectId of the document to delete; must not be null
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if objectId is null
     * @see #deleteOne(Bson)
     */
    public Mono<DeleteResult> deleteOne(final ObjectId objectId) {
        return deleteOne(MongoDBBase.objectIdToFilter(objectId));
    }

    /**
     * Deletes a single document matching the filter reactively.
     *
     * <p>Deletes the first document that matches the filter in a reactive manner. If multiple documents match,
     * only the first one encountered is deleted. This method is useful for precise
     * deletion operations based on custom criteria and returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.and(
     *     Filters.eq("status", "expired"),
     *     Filters.lt("created", oldDate)
     * );
     *
     * // Typical: deletes at most ONE document (the first match).
     * Mono<DeleteResult> result = executor.deleteOne(filter);         // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getDeletedCount())); // 1 if a doc matched, else 0
     *
     * // Edge: no document matches -> emits a DeleteResult with deletedCount == 0 (not empty, not an error).
     * long deleted = executor.deleteOne(Filters.eq("status", "no-such")).block().getDeletedCount(); // 0
     *
     * // Edge: even if several documents match, only one is removed.
     *
     * // Edge: cold publisher — re-subscribing re-issues the delete.
     * Mono<DeleteResult> again = executor.deleteOne(filter);  // nothing deleted until subscribed
     * }</pre>
     *
     * @param filter the query filter to identify the document to delete; must not be null
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if filter is null
     * @see #deleteOne(Bson, DeleteOptions)
     */
    public Mono<DeleteResult> deleteOne(final Bson filter) {
        N.checkArgNotNull(filter, "filter");

        return Mono.from(coll.deleteOne(filter));
    }

    /**
     * Deletes a single document matching the filter with custom options reactively.
     *
     * <p>Deletes the first matching document with custom delete options in a reactive manner. This method provides
     * control over the delete operation including collation settings and hint specifications
     * for index usage, returning a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions()
     *     .collation(Collation.builder().locale("en").build());
     * Bson filter = Filters.eq("username", "JohnDoe");
     * Mono<DeleteResult> result = executor.deleteOne(filter, options);
     * }</pre>
     *
     * @param filter the query filter to identify the document to delete; must not be null
     * @param options the options to apply to the delete operation such as collation or hint;
     *                may be null to use default options
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<DeleteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        N.checkArgNotNull(filter, "filter");

        return Mono.from(options == null ? coll.deleteOne(filter) : coll.deleteOne(filter, options));
    }

    /**
     * Deletes all documents matching the filter reactively.
     *
     * <p>Deletes all documents that match the filter criteria in a reactive manner. This method is useful for
     * bulk deletion operations where multiple documents need to be removed based on
     * common criteria. Use with caution as it can delete multiple documents. Returns a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "archived");
     *
     * // Typical: removes ALL matching documents.
     * Mono<DeleteResult> result = executor.deleteMany(filter);        // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getDeletedCount())); // e.g. 42
     *
     * // Edge: no match -> emits a DeleteResult with deletedCount == 0 (not empty, not an error).
     * long deleted = executor.deleteMany(Filters.eq("status", "no-such")).block().getDeletedCount(); // 0
     *
     * // Edge/Caution: an empty filter deletes the ENTIRE collection.
     * // executor.deleteMany(new Document()).block();          // deletes every document
     *
     * // Edge: cold publisher — re-subscribing re-issues the bulk delete.
     * }</pre>
     *
     * @param filter the query filter to identify documents to delete; must not be null
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if filter is null
     * @see #deleteMany(Bson, DeleteOptions)
     */
    public Mono<DeleteResult> deleteMany(final Bson filter) {
        N.checkArgNotNull(filter, "filter");

        return Mono.from(coll.deleteMany(filter));
    }

    /**
     * Deletes all documents matching the filter with custom options.
     *
     * <p>Deletes all matching documents with custom delete options. This method provides
     * full control over bulk deletion operations including collation settings and
     * index hints for optimized performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions()
     *     .hint(Indexes.ascending("status", "created"));
     * Bson filter = Filters.and(
     *     Filters.eq("status", "expired"),
     *     Filters.lt("created", cutoffDate)
     * );
     * Mono<DeleteResult> result = executor.deleteMany(filter, options);
     * }</pre>
     *
     * @param filter the query filter to identify documents to delete; must not be null
     * @param options the options to apply to the delete operation such as collation or hint;
     *                may be null to use default options
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<DeleteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        N.checkArgNotNull(filter, "filter");

        return Mono.from(options == null ? coll.deleteMany(filter) : coll.deleteMany(filter, options));
    }

    /**
     * Performs a bulk insert of multiple documents in a reactive manner.
     *
     * <p>Inserts each entity as an {@link InsertOneModel} via {@link #bulkWrite(List)} and emits the
     * resulting {@link BulkWriteResult} (use {@link BulkWriteResult#getInsertedCount()} for the
     * inserted count). The operation is atomic at the document level but not for the entire batch —
     * by default (ordered=true) the driver stops at the first failing document; pass an unordered
     * {@link BulkWriteOptions} via {@link #bulkInsert(Collection, BulkWriteOptions)} to continue
     * after errors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: emits the BulkWriteResult.
     * List<Document> documents = Arrays.asList(
     *     new Document("name", "Alice").append("age", 25),
     *     new Document("name", "Bob").append("age", 30)
     * );
     * Mono<BulkWriteResult> result = executor.bulkInsert(documents);                 // cold; runs on subscription
     * result.subscribe(r -> System.out.println("inserted " + r.getInsertedCount())); // "inserted 2"
     *
     * // Typical: block for the result directly.
     * BulkWriteResult r = executor.bulkInsert(documents).block();   // r.getInsertedCount() == 2
     *
     * // Edge/Negative: a null collection is rejected synchronously (at call time).
     * executor.bulkInsert(null);                                 // throws IllegalArgumentException
     *
     * // Edge/Negative: an empty collection is also rejected synchronously.
     * executor.bulkInsert(Collections.emptyList());             // throws IllegalArgumentException
     * }</pre>
     *
     * @param entities collection of documents or entities to insert; must not be null or empty
     * @return a {@code Mono} that, on subscription, emits exactly one {@link BulkWriteResult}
     *         (use {@link BulkWriteResult#getInsertedCount()} for the inserted count), then completes
     * @throws IllegalArgumentException if entities is null or empty
     * @throws com.mongodb.MongoBulkWriteException if the bulk write reports any per-document
     *         failures (signalled via {@code Mono})
     * @see #bulkInsert(Collection, BulkWriteOptions)
     * @see #bulkWrite(List)
     */
    public Mono<BulkWriteResult> bulkInsert(final Collection<?> entities) {
        return bulkInsert(entities, null);
    }

    /**
     * Performs bulk insert of multiple documents with custom options.
     *
     * <p>Inserts multiple documents with custom bulk write options. This method provides
     * control over the bulk operation including ordered/unordered execution, validation
     * bypass, and write concern settings. Unordered operations can continue after errors
     * and may perform better.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions()
     *     .ordered(false)
     *     .bypassDocumentValidation(true);
     * List<MyEntity> entities = loadEntities();
     * Mono<BulkWriteResult> result = executor.bulkInsert(entities, options);
     * }</pre>
     *
     * @param entities collection of documents or entities to insert; must not be null or empty
     * @param options the options to apply to the bulk write operation such as ordered execution
     *                or validation bypass; may be null to use default options
     * @return a Mono that emits the {@link BulkWriteResult} (use {@link BulkWriteResult#getInsertedCount()}
     *         for the inserted count)
     * @throws IllegalArgumentException if entities is null or empty
     */
    public Mono<BulkWriteResult> bulkInsert(final Collection<?> entities, final BulkWriteOptions options) {
        N.checkArgNotEmpty(entities, "entities");

        final List<InsertOneModel<Document>> list = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            if (entity instanceof final Document doc) {
                list.add(new InsertOneModel<>(doc));
            } else {
                list.add(new InsertOneModel<>(MongoDBBase.toDocument(entity)));
            }
        }

        return bulkWrite(list, options);
    }

    /**
     * Executes a bulk write operation with multiple write models.
     *
     * <p>Performs a bulk write operation consisting of multiple insert, update, replace,
     * or delete operations. This method provides maximum flexibility for complex bulk
     * operations where different types of operations need to be executed together. The
     * default driver behaviour is <i>ordered</i>: execution stops at the first failing model.
     * Use {@link #bulkWrite(List, BulkWriteOptions)} with {@code ordered(false)} to continue
     * after errors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: mix insert/update/delete in one round-trip.
     * List<WriteModel<Document>> operations = Arrays.asList(
     *     new InsertOneModel<>(new Document("type", "insert")),
     *     new UpdateOneModel<>(filter, update),
     *     new DeleteOneModel<>(deleteFilter)
     * );
     * Mono<BulkWriteResult> result = executor.bulkWrite(operations);                               // cold; runs on subscription
     * result.subscribe(r -> System.out.println(r.getInsertedCount() + "/" + r.getDeletedCount())); // e.g. "1/1"
     *
     * // Typical: block for the aggregate result.
     * BulkWriteResult r = executor.bulkWrite(operations).block();
     *
     * // Edge/Negative: a null list is rejected synchronously (at call time).
     * executor.bulkWrite(null);                                  // throws IllegalArgumentException
     *
     * // Edge/Negative: an empty list is also rejected synchronously.
     * executor.bulkWrite(Collections.emptyList());              // throws IllegalArgumentException
     * }</pre>
     *
     * @param requests list of write models representing the operations to perform; must not be null or empty
     * @return a {@code Mono} that, on subscription, emits a single {@link BulkWriteResult} and then
     *         completes
     * @throws IllegalArgumentException if {@code requests} is null or empty
     * @throws com.mongodb.MongoBulkWriteException if any write fails (signalled via {@code Mono})
     * @see #bulkWrite(List, BulkWriteOptions)
     */
    public Mono<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return bulkWrite(requests, null);
    }

    /**
     * Executes a bulk write operation with multiple write models and custom options.
     *
     * <p>Performs a bulk write operation with custom options. This method provides complete
     * control over mixed bulk operations including ordering, validation, and error handling.
     * Unordered operations may provide better performance as they can be executed in parallel.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions().ordered(false);
     * List<WriteModel<Document>> operations = buildOperations();
     * Mono<BulkWriteResult> result = executor.bulkWrite(operations, options);
     * }</pre>
     *
     * @param requests list of write models representing the operations to perform; must not be null or empty
     * @param options the options to apply to the bulk write operation such as ordered execution;
     *                may be null to use default options
     * @return a Mono that emits the BulkWriteResult containing detailed operation results including
     *         counts for inserted, updated, and deleted documents
     * @throws IllegalArgumentException if requests is null or empty
     */
    public Mono<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        N.checkArgNotEmpty(requests, "requests");

        if (options == null) {
            return Mono.from(coll.bulkWrite(requests));
        } else {
            return Mono.from(coll.bulkWrite(requests, options));
        }
    }

    /**
     * Atomically finds and updates a single document in a reactive manner.
     *
     * <p>Finds a document matching the filter and updates it atomically. The operation is atomic,
     * preventing race conditions in concurrent environments.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * document <i>before</i> the update (the default for this overload) and then completes; if no
     * document matches the filter and the operation is not configured to upsert, the {@code Mono}
     * completes <i>empty</i> without emitting.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("_id", documentId);
     * Document update = new Document("$inc", new Document("counter", 1));
     *
     * // Typical: emits the document AS IT WAS BEFORE the update (this overload's default).
     * Mono<Document> result = executor.findOneAndUpdate(filter, update);            // cold; runs on subscription
     * result.subscribe(before -> System.out.println(before.getInteger("counter"))); // pre-increment value
     *
     * // Typical: a non-operator object is auto-wrapped in {$set: ...}.
     * executor.findOneAndUpdate(filter, new Document("status", "seen")).block(); // emits the prior doc
     *
     * // Edge: no document matches (and no upsert) -> Mono completes EMPTY (no onNext).
     * executor.findOneAndUpdate(Filters.eq("_id", "missing"), update)
     *         .subscribe(d -> {}, e -> {}, () -> System.out.println("no match")); // prints "no match"
     *
     * // Edge: only the FIRST matching document is updated and returned.
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param update the update specification (Bson/Document/Map/entity class); must not be null.
     *               If it does not already begin with an update operator, it is wrapped in {@code $set}.
     * @return a {@code Mono} that emits the matched document (before update) on subscription, or
     *         completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter or update is null
     * @see #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#findOneAndUpdate(Bson, Object)
     */
    public Mono<Document> findOneAndUpdate(final Bson filter, final Object update) {
        return findOneAndUpdate(filter, update, (FindOneAndUpdateOptions) null);
    }

    /**
     * Atomically finds and updates a single document, returning it as a specific type.
     *
     * <p>Finds a document matching the filter, updates it atomically, and returns the result
     * mapped to the specified type. This method combines atomic update with type-safe
     * deserialization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "john");
     * Document update = new Document("$set", new Document("lastLogin", new Date()));
     * Mono<User> user = executor.findOneAndUpdate(filter, update, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param update the update operations to apply; must not be null
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if any parameter is null
     * @see #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions, Class)
     */
    public <T> Mono<T> findOneAndUpdate(final Bson filter, final Object update, final Class<T> rowType) {
        return findOneAndUpdate(filter, update, null, rowType);
    }

    /**
     * Atomically finds and updates a single document with custom options.
     *
     * <p>Finds and updates a document with full control over the operation. Options include
     * returning the document before or after update, upsert behavior, projection, sort order,
     * and more. This is the most flexible find-and-update method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * Bson filter = Filters.eq("_id", id);
     * Document update = new Document("$setOnInsert", defaults);
     * Mono<Document> result = executor.findOneAndUpdate(filter, update, options);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param update the update operations to apply; must not be null
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @return a Mono that emits the found document (before or after update based on options), or
     *         completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<Document> findOneAndUpdate(final Bson filter, final Object update, final FindOneAndUpdateOptions options) {
        N.checkArgNotNull(filter, "filter");

        if (options == null) {
            return Mono.from(coll.findOneAndUpdate(filter, toBson(update)));
        } else {
            return Mono.from(coll.findOneAndUpdate(filter, toBson(update), options));
        }
    }

    /**
     * Atomically finds and updates a single document with options, returning it as a specific type.
     *
     * <p>Combines atomic find-and-update with custom options and type-safe deserialization.
     * This method provides maximum flexibility for atomic update operations with typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER);
     * Bson filter = Filters.eq("_id", userId);
     * Document update = new Document("$inc", new Document("loginCount", 1));
     * Mono<User> user = executor.findOneAndUpdate(filter, update, options, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param update the update operations to apply; must not be null
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if filter, update, or rowType is null
     */
    public <T> Mono<T> findOneAndUpdate(final Bson filter, final Object update, final FindOneAndUpdateOptions options, final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return findOneAndUpdate(filter, update, options).mapNotNull(toEntity(rowType));
    }

    /**
     * Atomically finds and updates a document using multiple update operations.
     *
     * <p>Applies a collection of update operations atomically to a single document.
     * This is useful when multiple update operations need to be applied in a specific
     * order as an atomic operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: apply a pipeline-style atomic update; emits the prior document by default.
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processing"),
     *     Updates.currentDate("lastModified")
     * );
     * Mono<Document> result = executor.findOneAndUpdate(filter, updates);         // cold; runs on subscription
     * result.subscribe(before -> System.out.println(before.getString("status"))); // pre-update status
     *
     * // Edge: no match (and no upsert) -> Mono completes EMPTY (no onNext).
     * executor.findOneAndUpdate(Filters.eq("_id", "missing"), updates)
     *         .subscribe(d -> {}, e -> {}, () -> System.out.println("no match")); // prints "no match"
     *
     * // Edge/Negative: an empty pipeline is rejected synchronously (at call time).
     * executor.findOneAndUpdate(filter, Collections.emptyList()); // throws IllegalArgumentException
     *
     * // Edge/Negative: a null pipeline is likewise rejected synchronously.
     * executor.findOneAndUpdate(filter, (Collection<?>) null);    // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param objList collection of update operations to apply; must not be null or empty
     * @return a Mono that emits the found document, or completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @see #findOneAndUpdate(Bson, Collection, FindOneAndUpdateOptions)
     */
    public Mono<Document> findOneAndUpdate(final Bson filter, final Collection<?> objList) {
        return findOneAndUpdate(filter, objList, (FindOneAndUpdateOptions) null);
    }

    /**
     * Atomically finds and updates a document using multiple operations, returning it as a specific type.
     *
     * <p>Applies multiple update operations atomically and returns the result as the specified type.
     * Combines pipeline updates with type-safe deserialization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = buildUpdatePipeline();
     * Mono<User> user = executor.findOneAndUpdate(filter, updates, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param objList collection of update operations to apply; must not be null or empty
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if any parameter is null or objList is empty
     * @see #findOneAndUpdate(Bson, Collection, FindOneAndUpdateOptions, Class)
     */
    public <T> Mono<T> findOneAndUpdate(final Bson filter, final Collection<?> objList, final Class<T> rowType) {
        return findOneAndUpdate(filter, objList, null, rowType);
    }

    /**
     * Atomically finds and updates a document using multiple operations with custom options.
     *
     * <p>Applies a pipeline of update operations atomically with full control over the operation.
     * This method is useful for complex atomic updates that require multiple operations
     * to be applied in sequence.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER);
     * List<Bson> pipeline = Arrays.asList(
     *     new Document("$set", new Document("status", "processed")),
     *     new Document("$inc", new Document("processCount", 1))
     * );
     * Mono<Document> result = executor.findOneAndUpdate(filter, pipeline, options);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param objList collection of update operations to apply as a pipeline; must not be null or empty
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @return a Mono that emits the found document (before or after update based on options), or
     *         completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<Document> findOneAndUpdate(final Bson filter, final Collection<?> objList, final FindOneAndUpdateOptions options) {
        N.checkArgNotNull(filter, "filter");

        final List<Bson> updateToUse = toBson(objList);

        if (options == null) {
            return Mono.from(coll.findOneAndUpdate(filter, updateToUse));
        } else {
            return Mono.from(coll.findOneAndUpdate(filter, updateToUse, options));
        }
    }

    /**
     * Atomically finds and updates using multiple operations with options, returning as a specific type.
     *
     * <p>The most flexible find-and-update method combining pipeline updates, custom options,
     * and type-safe deserialization. Ideal for complex atomic operations with typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .projection(Projections.exclude("internalField"));
     * List<Bson> pipeline = buildComplexPipeline();
     * Mono<User> user = executor.findOneAndUpdate(filter, pipeline, options, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param objList collection of update operations to apply as a pipeline; must not be null or empty
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if filter, objList, or rowType is null, or objList is empty
     */
    public <T> Mono<T> findOneAndUpdate(final Bson filter, final Collection<?> objList, final FindOneAndUpdateOptions options, final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return findOneAndUpdate(filter, objList, options).mapNotNull(toEntity(rowType));
    }

    /**
     * Atomically finds and replaces a single document in a reactive manner.
     *
     * <p>Finds a document matching the filter and replaces it entirely with the replacement
     * document. The operation is atomic, preventing race conditions. The {@code _id} field is
     * preserved from the original document.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * matched document <i>before</i> replacement (the default for this overload) and then
     * completes; if no document matches the filter and the operation is not configured to upsert,
     * the {@code Mono} completes <i>empty</i> without emitting.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("_id", documentId);
     * Document replacement = new Document("name", "New Name").append("version", 2);
     *
     * // Typical: emits the document AS IT WAS BEFORE replacement (this overload's default).
     * Mono<Document> result = executor.findOneAndReplace(filter, replacement);  // cold; runs on subscription
     * result.subscribe(before -> System.out.println(before.getString("name"))); // old name
     *
     * // Edge: no document matches (and no upsert) -> Mono completes EMPTY (no onNext).
     * executor.findOneAndReplace(Filters.eq("_id", "missing"), replacement)
     *         .subscribe(d -> {}, e -> {}, () -> System.out.println("no match")); // prints "no match"
     *
     * // Edge: the _id of the original document is preserved across the replacement.
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param replacement the replacement document (Document/Map/entity class); must not be null
     * @return a {@code Mono} that emits the matched document (before replacement) on subscription,
     *         or completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter or replacement is null
     * @see #findOneAndReplace(Bson, Object, FindOneAndReplaceOptions)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#findOneAndReplace(Bson, Object)
     */
    public Mono<Document> findOneAndReplace(final Bson filter, final Object replacement) {
        return findOneAndReplace(filter, replacement, (FindOneAndReplaceOptions) null);
    }

    /**
     * Atomically finds and replaces a document, returning it as a specific type.
     *
     * <p>Finds and replaces a document atomically, returning the result as the specified type.
     * Combines atomic replacement with type-safe deserialization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "olduser");
     * User newUser = new User("newuser", "email@example.com");
     * Mono<User> oldUser = executor.findOneAndReplace(filter, newUser, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param replacement the replacement document; must not be null
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if any parameter is null
     * @see #findOneAndReplace(Bson, Object, FindOneAndReplaceOptions, Class)
     */
    public <T> Mono<T> findOneAndReplace(final Bson filter, final Object replacement, final Class<T> rowType) {
        return findOneAndReplace(filter, replacement, null, rowType);
    }

    /**
     * Atomically finds and replaces a document with custom options.
     *
     * <p>Finds and replaces a document with full control over the operation. Options include
     * returning the document before or after replacement, upsert behavior, projection,
     * sort order, and validation bypass.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * Bson filter = Filters.eq("_id", id);
     * Document replacement = buildReplacementDocument();
     * Mono<Document> result = executor.findOneAndReplace(filter, replacement, options);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param replacement the replacement document; must not be null
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @return a Mono that emits the found document (before or after replacement based on options),
     *         or completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter or replacement is null
     */
    public Mono<Document> findOneAndReplace(final Bson filter, final Object replacement, final FindOneAndReplaceOptions options) {
        N.checkArgNotNull(filter, "filter");
        N.checkArgNotNull(replacement, "replacement");

        if (options == null) {
            return Mono.from(coll.findOneAndReplace(filter, toDocument(replacement)));
        } else {
            return Mono.from(coll.findOneAndReplace(filter, toDocument(replacement), options));
        }
    }

    /**
     * Atomically finds and replaces with options, returning as a specific type.
     *
     * <p>The most flexible find-and-replace method combining custom options with type-safe
     * deserialization. Provides complete control over atomic replacement operations with
     * typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .projection(Projections.include("name", "email"));
     * User newUser = createUser();
     * Mono<User> result = executor.findOneAndReplace(filter, newUser, options, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param replacement the replacement document; must not be null
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if filter, replacement, or rowType is null
     */
    public <T> Mono<T> findOneAndReplace(final Bson filter, final Object replacement, final FindOneAndReplaceOptions options, final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return findOneAndReplace(filter, replacement, options).mapNotNull(toEntity(rowType));
    }

    /**
     * Atomically finds and deletes a single document in a reactive manner.
     *
     * <p>Finds a document matching the filter and deletes it atomically. This operation is atomic,
     * ensuring the document is retrieved and deleted in a single operation without race conditions.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * just-deleted document and then completes, or completes <i>empty</i> when no document matches
     * the filter (nothing is deleted in that case).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "expired");
     *
     * // Typical: emits the just-deleted document, then completes.
     * Mono<Document> deletedDoc = executor.findOneAndDelete(filter);                // cold; runs on subscription
     * deletedDoc.subscribe(doc -> System.out.println("removed " + doc.get("_id"))); // prints once if a doc was deleted
     *
     * // Edge: no document matches -> nothing is deleted and the Mono completes EMPTY (no onNext).
     * executor.findOneAndDelete(Filters.eq("status", "no-such"))
     *         .subscribe(d -> {}, e -> {}, () -> System.out.println("no match")); // prints "no match"
     *
     * // Edge: only ONE document is removed even if several match.
     * }</pre>
     *
     * @param filter the query filter to find the document to delete; must not be null
     * @return a {@code Mono} that emits the deleted document on subscription, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if filter is null
     * @see #findOneAndDelete(Bson, FindOneAndDeleteOptions)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#findOneAndDelete(Bson)
     */
    public Mono<Document> findOneAndDelete(final Bson filter) {
        return findOneAndDelete(filter, (FindOneAndDeleteOptions) null);
    }

    /**
     * Atomically finds and deletes a document, returning it as a specific type.
     *
     * <p>Finds and deletes a document atomically, returning the deleted document as the
     * specified type. Combines atomic deletion with type-safe deserialization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("_id", userId);
     * Mono<User> deletedUser = executor.findOneAndDelete(filter, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document to delete; must not be null
     * @param rowType the class to deserialize the deleted document into; must not be null
     * @return a Mono that emits the deleted document mapped to the specified type, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if filter or rowType is null
     * @see #findOneAndDelete(Bson, FindOneAndDeleteOptions, Class)
     */
    public <T> Mono<T> findOneAndDelete(final Bson filter, final Class<T> rowType) {
        return findOneAndDelete(filter, null, rowType);
    }

    /**
     * Atomically finds and deletes a document with custom options.
     *
     * <p>Finds and deletes a document with control over the operation including projection,
     * sort order, and collation. This method is useful when you need to delete specific
     * documents based on sort criteria or with field projections.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .sort(Sorts.ascending("priority"))
     *     .projection(Projections.include("_id", "name"));
     * Bson filter = Filters.eq("status", "pending");
     * Mono<Document> result = executor.findOneAndDelete(filter, options);
     * }</pre>
     *
     * @param filter the query filter to find the document to delete; must not be null
     * @param options the options such as projection, sort, or collation;
     *                may be null to use default options
     * @return a Mono that emits the deleted document, or completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<Document> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        N.checkArgNotNull(filter, "filter");

        if (options == null) {
            return Mono.from(coll.findOneAndDelete(filter));
        } else {
            return Mono.from(coll.findOneAndDelete(filter, options));
        }
    }

    /**
     * Atomically finds and deletes with options, returning as a specific type.
     *
     * <p>The most flexible find-and-delete method combining custom options with type-safe
     * deserialization. Provides complete control over atomic deletion operations with
     * typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .sort(Sorts.descending("created"))
     *     .projection(Projections.exclude("internalData"));
     * Mono<Order> oldestOrder = executor.findOneAndDelete(filter, options, Order.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document to delete; must not be null
     * @param options the options such as projection, sort, or collation;
     *                may be null to use default options
     * @param rowType the class to deserialize the deleted document into; must not be null
     * @return a Mono that emits the deleted document mapped to the specified type, or completes empty
     *         when no document matches the filter
     * @throws IllegalArgumentException if filter or rowType is null
     */
    public <T> Mono<T> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options, final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return findOneAndDelete(filter, options).mapNotNull(toEntity(rowType));
    }

    /**
     * Finds distinct values for a specified field.
     *
     * <p>Returns distinct values for the specified field across all documents in the collection.
     * Useful for getting unique values such as categories, tags, or statuses.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: emit each unique value of the field, deduplicated server-side.
     * Flux<String> categories = executor.distinct("category", String.class); // cold Flux
     * categories.subscribe(System.out::println);                             // e.g. "books", "electronics", ...
     *
     * // Typical: collect into a list.
     * List<String> all = executor.distinct("category", String.class).collectList().block();
     *
     * // Edge: an empty collection (or a field present on no document) -> Flux completes EMPTY.
     *
     * // Edge: cold — no query is issued until subscription; re-subscribing re-runs it.
     * Flux<String> notRunYet = executor.distinct("category", String.class); // nothing sent yet
     * }</pre>
     *
     * @param <T> the type of the distinct values
     * @param fieldName the field name to get distinct values for; must not be null
     * @param rowType the class type of the distinct values; must not be null
     * @return a cold {@code Flux} that, on subscription, emits each distinct value of the field
     *         (deduplicated by the server), then completes
     * @throws IllegalArgumentException if fieldName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     * @see #distinct(String, Bson, Class)
     */
    public <T> Flux<T> distinct(final String fieldName, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldName, "fieldName");

        return Flux.from(coll.distinct(fieldName, rowType));
    }

    /**
     * Finds distinct values for a field with a filter.
     *
     * <p>Returns distinct values for the specified field among documents matching the filter.
     * Useful for getting unique values within a subset of documents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     * Flux<String> activeUsernames = executor.distinct("username", filter, String.class);
     * }</pre>
     *
     * @param <T> the type of the distinct values
     * @param fieldName the field name to get distinct values for; must not be null
     * @param filter the query filter to apply before finding distinct values; must not be null
     * @param rowType the class type of the distinct values; must not be null
     * @return a cold {@code Flux} that, on subscription, emits each distinct value found among the
     *         filtered documents, then completes
     * @throws IllegalArgumentException if fieldName is null or empty, or filter is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     */
    public <T> Flux<T> distinct(final String fieldName, final Bson filter, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldName, "fieldName");
        N.checkArgNotNull(filter, "filter");

        return Flux.from(coll.distinct(fieldName, filter, rowType));
    }

    /**
     * Executes an aggregation pipeline in a reactive manner.
     *
     * <p>Processes documents through an aggregation pipeline consisting of multiple stages.
     * Aggregation pipelines enable complex data processing including filtering, grouping,
     * sorting, reshaping, and computing derived values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: group-and-sum; each output stage document is emitted.
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.eq("status", "completed")),
     *     Aggregates.group("$category", Accumulators.sum("total", "$amount"))
     * );
     * Flux<Document> results = executor.aggregate(pipeline);      // cold Flux
     * results.subscribe(doc -> System.out.println(doc.toJson())); // one doc per group
     *
     * // Typical: collect the output.
     * List<Document> grouped = executor.aggregate(pipeline).collectList().block();
     *
     * // Edge: a pipeline that matches nothing -> Flux completes EMPTY.
     * executor.aggregate(Arrays.asList(Aggregates.match(Filters.eq("status", "no-such"))))
     *         .subscribe(d -> {}, e -> {}, () -> System.out.println("empty")); // prints "empty"
     *
     * // Edge: cold — no aggregation runs until subscription; re-subscribing re-runs the pipeline.
     * }</pre>
     *
     * @param pipeline the aggregation pipeline stages; must not be null
     * @return a {@code Flux} that, on subscription, emits each output document produced by the
     *         pipeline, then completes; completes empty if the pipeline yields no documents
     * @throws IllegalArgumentException if pipeline is null
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     * @see #aggregate(List, Class)
     * @see com.landawn.abacus.da.mongodb.MongoCollectionExecutor#aggregate(List)
     */
    public Flux<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    /**
     * Executes an aggregation pipeline, decoding each output document as the specified type.
     *
     * <p>The pipeline is always executed against {@link Document} (so the framework codec
     * registry is honoured) and each output document is then converted to {@code rowType} via
     * the same row-conversion path used by {@code list(...)}/{@code findFirst(...)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.gte("date", startDate)),
     *     Aggregates.group("$userId",
     *         Accumulators.sum("totalSpent", "$amount"),
     *         Accumulators.avg("avgSpent", "$amount"))
     * );
     * Flux<UserStats> stats = executor.aggregate(pipeline, UserStats.class);
     * }</pre>
     *
     * @param <T> the type of the aggregation results
     * @param pipeline the aggregation pipeline stages; must not be null
     * @param rowType an entity class with getter/setter methods, {@code Map.class}, or a basic
     *                single-value type; must not be null
     * @return a {@code Flux} that, on subscription, emits each pipeline output document decoded
     *         as {@code T}, then completes; completes empty when the pipeline yields no documents
     * @throws IllegalArgumentException if pipeline or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     */
    public <T> Flux<T> aggregate(final List<? extends Bson> pipeline, final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return Flux.from(coll.aggregate(pipeline, Document.class)).mapNotNull(toEntity(rowType));
    }

    /**
     * Groups documents by a single field.
     *
     * <p>Groups all documents in the collection by the specified field. This is a convenience
     * method for simple grouping operations. For more complex grouping, use the aggregate method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: one output document per distinct value, with {_id: <value>}.
     * Flux<Document> groupedByCategory = executor.groupBy("category");        // cold Flux
     * groupedByCategory.subscribe(doc -> System.out.println(doc.get("_id"))); // e.g. "books", "toys"
     *
     * // Typical: collect the group keys.
     * List<Document> groups = executor.groupBy("category").collectList().block();
     *
     * // Edge: an empty collection -> Flux completes EMPTY (no groups).
     *
     * // Edge: cold — no aggregation runs until subscription; re-subscribing re-runs it.
     * Flux<Document> notRunYet = executor.groupBy("category"); // nothing sent yet
     * }</pre>
     *
     * @param fieldName the field name to group by; must not be null
     * @return a Flux that emits documents grouped by the specified field
     * @throws IllegalArgumentException if fieldName is null or empty
     * @see #groupBy(String, Class)
     */
    @Beta
    public Flux<Document> groupBy(final String fieldName) {
        return groupBy(fieldName, Document.class);
    }

    /**
     * Groups documents by a single field returning results as a specific type.
     *
     * <p>Groups documents by the specified field and maps results to the specified type.
     * This convenience method simplifies common grouping operations with typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<CategoryGroup> groups = executor.groupBy("category", CategoryGroup.class);
     * }</pre>
     *
     * @param <T> the type of the grouped results
     * @param fieldName the field name to group by; must not be null
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits grouped documents mapped to the specified type
     * @throws IllegalArgumentException if fieldName is null or empty, or rowType is null
     * @see #groupBy(Collection, Class)
     */
    @Beta
    public <T> Flux<T> groupBy(final String fieldName, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldName, "fieldName");

        return aggregate(groupByPipeline(fieldName, false, rowType), rowType);
    }

    /**
     * Groups documents by multiple fields.
     *
     * <p>Groups documents by multiple fields creating composite grouping keys. This is useful
     * for multi-dimensional analysis and reporting.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<Document> grouped = executor.groupBy(Arrays.asList("category", "status"));
     * }</pre>
     *
     * @param fieldNames collection of field names to group by; must not be null or empty
     * @return a Flux that emits documents grouped by the specified fields
     * @throws IllegalArgumentException if fieldNames is null or empty
     * @see #groupBy(Collection, Class)
     */
    @Beta
    public Flux<Document> groupBy(final Collection<String> fieldNames) {
        return groupBy(fieldNames, Document.class);
    }

    /**
     * Groups documents by multiple fields returning results as a specific type.
     *
     * <p>Groups documents by multiple fields and maps results to the specified type.
     * Enables multi-dimensional grouping with type-safe results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("department", "role");
     * Flux<EmployeeGroup> groups = executor.groupBy(fields, EmployeeGroup.class);
     * }</pre>
     *
     * @param <T> the type of the grouped results
     * @param fieldNames collection of field names to group by; must not be null or empty
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits grouped documents mapped to the specified type
     * @throws IllegalArgumentException if fieldNames is null/empty or rowType is null
     */
    @Beta
    public <T> Flux<T> groupBy(final Collection<String> fieldNames, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldNames, "fieldNames");
        N.checkArgNotNull(rowType, "rowType");

        return aggregate(groupByPipeline(fieldNames, false, rowType), rowType);
    }

    /**
     * Groups documents by a field and counts frequency.
     *
     * <p>Groups documents by the specified field and includes a count of documents in each group.
     * This is a common pattern for generating frequency distributions and summaries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: each output doc is {_id: <value>, count: <frequency>}.
     * Flux<Document> categoryCounts = executor.groupByAndCount("category"); // cold Flux
     * categoryCounts.subscribe(doc ->
     *     System.out.println(doc.get("_id") + " = " + doc.getInteger("count"))); // e.g. "books = 10"
     *
     * // Typical: collect the frequency distribution.
     * List<Document> counts = executor.groupByAndCount("category").collectList().block();
     *
     * // Edge: an empty collection -> Flux completes EMPTY (no counts emitted).
     *
     * // Edge: cold — re-subscribing re-runs the aggregation.
     * }</pre>
     *
     * @param fieldName the field name to group by; must not be null
     * @return a Flux that emits documents with _id (group key) and count fields
     * @throws IllegalArgumentException if fieldName is null or empty
     * @see #groupByAndCount(String, Class)
     */
    @Beta
    public Flux<Document> groupByAndCount(final String fieldName) {
        return groupByAndCount(fieldName, Document.class);
    }

    /**
     * Groups documents by a field and counts frequency, returning as a specific type.
     *
     * <p>Groups documents by field, counts frequency, and maps results to the specified type.
     * Useful for generating typed frequency distributions and statistical summaries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<CategoryCount> counts = executor.groupByAndCount("category", CategoryCount.class);
     * }</pre>
     *
     * @param <T> the type of the grouped and counted results
     * @param fieldName the field name to group by; must not be null
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits grouped and counted documents mapped to the specified type
     * @throws IllegalArgumentException if fieldName is null or empty, or rowType is null
     */
    @Beta
    public <T> Flux<T> groupByAndCount(final String fieldName, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldName, "fieldName");

        return aggregate(groupByPipeline(fieldName, true, rowType), rowType);
    }

    /**
     * Groups documents by multiple fields and counts frequency.
     *
     * <p>Groups documents by multiple fields and includes a count of documents in each group.
     * Useful for multi-dimensional frequency analysis and cross-tabulation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<Document> counts = executor.groupByAndCount(Arrays.asList("status", "priority"));
     * }</pre>
     *
     * @param fieldNames collection of field names to group by; must not be null or empty
     * @return a Flux that emits documents with composite _id and count fields
     * @throws IllegalArgumentException if fieldNames is null or empty
     * @see #groupByAndCount(Collection, Class)
     */
    @Beta
    public Flux<Document> groupByAndCount(final Collection<String> fieldNames) {
        return groupByAndCount(fieldNames, Document.class);
    }

    /**
     * Groups by multiple fields with counts, returning as a specific type.
     *
     * <p>Groups documents by multiple fields, counts frequency, and maps results to the
     * specified type. Enables complex multi-dimensional analysis with type-safe results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> dimensions = Arrays.asList("region", "product");
     * Flux<SalesCount> counts = executor.groupByAndCount(dimensions, SalesCount.class);
     * }</pre>
     *
     * @param <T> the type of the grouped and counted results
     * @param fieldNames collection of field names to group by; must not be null or empty
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits grouped and counted documents mapped to the specified type
     * @throws IllegalArgumentException if fieldNames is null/empty or rowType is null
     */
    @Beta
    public <T> Flux<T> groupByAndCount(final Collection<String> fieldNames, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldNames, "fieldNames");
        N.checkArgNotNull(rowType, "rowType");

        return aggregate(groupByPipeline(fieldNames, true, rowType), rowType);
    }

    private static List<Document> groupByPipeline(final String fieldName, final boolean count, final Class<?> rowType) {
        final Document group = new Document(MongoDBBase._ID, _$ + fieldName);

        if (count) {
            group.append(_COUNT, new Document(_$SUM, 1));
        }

        if (Document.class.equals(rowType)) {
            return N.asList(new Document(_$GROUP, group));
        }

        final Document project = new Document(MongoDBBase._ID, 0).append(fieldName, "$" + MongoDBBase._ID);

        if (count) {
            project.append(_COUNT, 1);
        }

        return N.asList(new Document(_$GROUP, group), new Document("$project", project));
    }

    private static List<Document> groupByPipeline(final Collection<String> fieldNames, final boolean count, final Class<?> rowType) {
        N.checkArgNotEmpty(fieldNames, "fieldNames");

        final Document groupFields = new Document();

        for (final String fieldName : fieldNames) {
            groupFields.put(fieldName, _$ + fieldName);
        }

        final Document group = new Document(MongoDBBase._ID, groupFields);

        if (count) {
            group.append(_COUNT, new Document(_$SUM, 1));
        }

        if (Document.class.equals(rowType)) {
            return N.asList(new Document(_$GROUP, group));
        }

        final Document project = new Document(MongoDBBase._ID, 0);

        for (final String fieldName : fieldNames) {
            project.append(fieldName, "$" + MongoDBBase._ID + "." + fieldName);
        }

        if (count) {
            project.append(_COUNT, 1);
        }

        return N.asList(new Document(_$GROUP, group), new Document("$project", project));
    }

    /**
     * Executes a map-reduce operation.
     *
     * <p>Performs a server-side map-reduce using the supplied JavaScript functions. Note that
     * server-side map-reduce has been deprecated by MongoDB itself starting with version 5.0 —
     * aggregation pipelines are preferred for both performance and forward compatibility.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: server-side map-reduce; each output doc is {_id: <key>, value: <reduced>}.
     * String mapFunction = "function() { emit(this.category, this.amount); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * Flux<Document> results = executor.mapReduce(mapFunction, reduceFunction);               // cold Flux
     * results.subscribe(doc -> System.out.println(doc.get("_id") + ": " + doc.get("value"))); // one line per key
     *
     * // Typical: collect the results.
     * List<Document> totals = executor.mapReduce(mapFunction, reduceFunction).collectList().block();
     *
     * // Edge: an empty collection -> Flux completes EMPTY.
     *
     * // Edge: cold — no map-reduce runs until subscription; re-subscribing re-runs it.
     * // Note: prefer the aggregate pipeline — server-side map-reduce is deprecated in MongoDB 5.0+.
     * }</pre>
     *
     * @param mapFunction the JavaScript map function; must not be null
     * @param reduceFunction the JavaScript reduce function; must not be null
     * @return a cold {@code Flux} that, on subscription, emits each result document, then completes
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     * @deprecated Map-reduce is deprecated in MongoDB 5.0+. Use {@link #aggregate(List)} instead.
     * @see #aggregate(List)
     */
    @Deprecated
    public Flux<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, Document.class);
    }

    /**
     * Executes a map-reduce operation returning results as a specific type.
     *
     * <p>Performs a server-side map-reduce using the supplied JavaScript functions, decoding each
     * output document to {@code rowType} via the framework's row-conversion path. See
     * {@link #mapReduce(String, String)} for deprecation status.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String mapFunction = "function() { emit(this.category, this.amount); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * Flux<CategoryTotal> totals = executor.mapReduce(mapFunction, reduceFunction, CategoryTotal.class);
     * }</pre>
     *
     * @param <T> the type of the map-reduce results
     * @param mapFunction the JavaScript map function; must not be null
     * @param reduceFunction the JavaScript reduce function; must not be null
     * @param rowType the class to deserialize results into; must not be null
     * @return a cold {@code Flux} that, on subscription, emits each result document decoded as
     *         {@code T}, then completes
     * @throws IllegalArgumentException if {@code rowType} is null
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     * @deprecated Map-reduce is deprecated in MongoDB 5.0+. Use {@link #aggregate(List, Class)} instead.
     */
    @Deprecated
    public <T> Flux<T> mapReduce(final String mapFunction, final String reduceFunction, final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return Flux.from(coll.mapReduce(mapFunction, reduceFunction, Document.class)).mapNotNull(toEntity(rowType));
    }

    //
    //    private String getCollectionName(final Class<?> cls) {
    //        final String collectionName = classCollectionMapper.get(cls);
    //
    //        if (N.isEmpty(collectionName)) {
    //            throw new IllegalArgumentException("No collection is mapped to class: " + cls);
    //        }
    //        return collectionName;
    //    }

    //
    //    private String getObjectId(Object obj) {
    //        String objectId = null;
    //
    //        try {
    //            objectId = N.convert(String.class, Beans.getPropValue(obj, "id"));
    //        } catch (Exception e) {
    //            // ignore
    //
    //            try {
    //                objectId = N.convert(String.class, Beans.getPropValue(obj, "objectId"));
    //            } catch (Exception e2) {
    //                // ignore
    //            }
    //        }
    //
    //        if (N.isEmpty(objectId)) {
    //            throw new IllegalArgumentException("Property value of 'id' or 'objectId' can't be null or empty for update or delete");
    //        }
    //
    //        return objectId;
    //    }

}
