/*
 * Copyright (C) 2016 HaiYang Li
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

package com.landawn.abacus.da.neo4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.ogm.cypher.Filter;
import org.neo4j.ogm.cypher.Filters;
import org.neo4j.ogm.cypher.query.Pagination;
import org.neo4j.ogm.cypher.query.SortOrder;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.stream.Stream;

/**
 * A thin executor that wraps the <a href="http://neo4j.com/docs/ogm/java/stable/">Neo4j OGM</a>
 * {@link Session} API, adding lightweight session pooling and {@link Stream}-based result handling
 * on top of it. Each public operation borrows a {@link Session} from an internal pool, delegates to
 * the equivalent method on the OGM session, and either returns the session immediately (for eager
 * methods) or transfers ownership of session release to the returned {@link Stream}'s
 * {@code onClose} handler (for the streaming {@code query} overloads).
 *
 * <h2>Session and Transaction Semantics</h2>
 * <ul>
 *   <li><b>Pool:</b> Sessions are held in a bounded {@link LinkedBlockingQueue} (capacity 8192).
 *       {@link #getSession()} polls the queue with a 100&nbsp;ms timeout and opens a fresh OGM
 *       session via {@link SessionFactory#openSession()} if the queue is empty or the wait is
 *       interrupted (the thread's interrupt status is preserved).</li>
 *   <li><b>Release:</b> {@link Session#clear()} is invoked before a session is offered back to the
 *       queue. If the queue is full or the offer times out the session is silently dropped (no
 *       {@code close()} call is made; the OGM {@link Session} has no {@code close} operation).</li>
 *   <li><b>Transactions:</b> Each operation runs against an auto-commit OGM session unless callers
 *       use {@link #run(Consumer)} or {@link #call(Function)} to open a multi-statement transaction
 *       explicitly via {@link Session#beginTransaction()}. Read/write routing is the OGM/driver's
 *       responsibility; the {@code readOnly} flag accepted by
 *       {@link #query(String, Map, boolean)} is forwarded to the underlying driver as a routing
 *       hint.</li>
 *   <li><b>Threading:</b> All methods are blocking and synchronous. The executor itself is
 *       thread-safe (the session pool is concurrent), but individual {@link Session} instances
 *       borrowed from the pool are <i>not</i> safe for concurrent use across threads; do not share
 *       a session obtained via {@link #openSession()} between threads.</li>
 * </ul>
 *
 * <h2>Cypher Parameter Binding</h2>
 * <p>Cypher parameters are passed as a {@code Map<String, ?>} of named parameters. Use the
 * {@code $name} placeholder syntax in your Cypher (e.g. {@code MATCH (p:Person {name:$name})}).
 * Parameter values are forwarded verbatim to the OGM session, which converts them through the
 * standard OGM type system.</p>
 *
 * <h2>Result Streaming</h2>
 * <p>The {@link #query(String, Map)}, {@link #query(String, Map, boolean)}, and
 * {@link #query(Class, String, Map)} overloads return lazily-consumed {@link Stream}s that own a
 * pooled session for their lifetime. The session is returned to the pool when the stream is closed
 * (either explicitly via {@link Stream#close()} or via try-with-resources). Failing to close such a
 * stream will leak a session from the pool.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize executor
 * SessionFactory sessionFactory = new SessionFactory(configuration, "com.example.model");
 * Neo4jExecutor executor = new Neo4jExecutor(sessionFactory);
 *
 * // Node operations
 * Person person = new Person("John Doe", 30);
 * executor.save(person);
 *
 * Person loaded = executor.load(Person.class, person.getId());
 * Collection<Person> people = executor.loadAll(Person.class);
 *
 * // Relationship operations
 * Company company = new Company("Tech Corp");
 * person.worksFor(company);
 * executor.save(person, 1);   // Save with depth 1 to include relationships
 *
 * // Cypher queries
 * Map<String, Object> params = Map.of("age", 25);
 * try (Stream<Person> adults = executor.query(Person.class,
 *         "MATCH (p:Person) WHERE p.age > $age RETURN p", params)) {
 *     adults.forEach(System.out::println);
 * }
 *
 * // Filtering and pagination
 * Filter ageFilter = new Filter("age", ComparisonOperator.GREATER_THAN, 25);
 * Pagination pagination = new Pagination(0, 10);
 * Collection<Person> pagedResults = executor.loadAll(Person.class, ageFilter, pagination);
 * }</pre>
 *
 * @see org.neo4j.ogm.session.SessionFactory
 * @see org.neo4j.ogm.session.Session
 * @see <a href="http://neo4j.com/docs/ogm/java/stable/">Neo4j OGM Documentation</a>
 */
public final class Neo4jExecutor {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jExecutor.class);

    private final LinkedBlockingQueue<Session> sessionPool = new LinkedBlockingQueue<>(8192);

    private final SessionFactory sessionFactory;

    /**
     * Constructs a {@code Neo4jExecutor} that borrows sessions from the supplied
     * {@link SessionFactory}.
     * <p>
     * The factory owns the connection to the Neo4j database and the OGM mapping configuration; this
     * executor only manages a bounded internal queue (capacity 8192) of {@link Session} instances
     * that are reused across calls. The session pool is populated lazily on first use; no sessions
     * are opened by this constructor.
     *
     * @param sessionFactory the Neo4j {@link SessionFactory} used to open new sessions on demand
     * @throws IllegalArgumentException if {@code sessionFactory} is {@code null}
     */
    public Neo4jExecutor(final SessionFactory sessionFactory) {
        if (sessionFactory == null) {
            throw new IllegalArgumentException("sessionFactory cannot be null");
        }
        this.sessionFactory = sessionFactory;
    }

    /**
     * Returns the {@link SessionFactory} this executor was constructed with.
     * <p>
     * Use this to access OGM features not surfaced by the executor (for example, direct
     * configuration introspection or opening sessions that are not pooled by this executor).
     *
     * @return the {@link SessionFactory} supplied at construction time; never {@code null}
     * @see org.neo4j.ogm.session.SessionFactory
     */
    public SessionFactory sessionFactory() {
        return sessionFactory;
    }

    /**
     * Opens a new {@link Session} directly via the underlying {@link SessionFactory}.
     * <p>
     * Sessions returned by this method are <i>not</i> placed into the executor's pool. The OGM
     * {@link Session} does not expose a {@code close()} method, but the caller should call
     * {@link Session#clear()} when finished to release the session's mapping context (the
     * accumulated identity map of loaded entities). Prefer {@link #run(Consumer)} or
     * {@link #call(Function)}, which acquire a pooled session, clear it on the way back to the
     * pool, and propagate the result automatically.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Open a session for manual management
     * Session session = executor.openSession();
     * try {
     *     Person person = session.load(Person.class, 123L);
     *     person.setName("Updated Name");
     *     session.save(person);
     * } finally {
     *     // Release the session's mapping context (no close() to call)
     *     session.clear();
     * }
     *
     * // Prefer using run() or call() for automatic session management
     * executor.run(session -> {
     *     Person person = session.load(Person.class, 123L);
     *     person.setName("Updated Name");
     *     session.save(person);
     * });
     * }</pre>
     *
     * @return a fresh, unpooled OGM {@link Session}
     * @see org.neo4j.ogm.session.Session
     * @see #run(Consumer)
     * @see #call(Function)
     */
    public Session openSession() {
        return sessionFactory.openSession();
    }

    /**
     * Borrows a pooled {@link Session}, hands it to {@code action}, and returns it to the pool when
     * {@code action} completes (whether normally or exceptionally).
     * <p>
     * Use this for multi-statement work that needs to share a single session (for example, when
     * opening an explicit transaction via {@link Session#beginTransaction()} so that several saves
     * commit atomically). For operations that return a value, use {@link #call(Function)} instead.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute operations without return value
     * executor.run(session -> {
     *     Person person = session.load(Person.class, 123L);
     *     person.setName("John Doe");
     *     session.save(person);
     * });
     *
     * // Multi-statement transaction
     * executor.run(session -> {
     *     try (org.neo4j.ogm.transaction.Transaction tx = session.beginTransaction()) {
     *         session.save(personA);
     *         session.save(personB);
     *         tx.commit();
     *     }
     * });
     * }</pre>
     *
     * @param action the callback invoked with the pooled session; must not be {@code null}
     * @see #call(Function)
     */
    @Beta
    public void run(final Consumer<? super Session> action) {
        final Session session = getSession();

        try {
            action.accept(session);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Borrows a pooled {@link Session}, applies {@code action} to it, returns the result, and
     * releases the session back to the pool when {@code action} completes (whether normally or
     * exceptionally).
     * <p>
     * <b>Important:</b> {@code action} must not leak references to entities or to the {@link Session}
     * itself outside the lambda, because the session is cleared (see {@link Session#clear()}) before
     * it is returned to the pool, which severs the identity map of any loaded entities. Convert
     * results to detached forms (e.g. collect into a {@code List}, copy fields, or convert to
     * {@code String}) inside the lambda.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute and return a result
     * Person person = executor.call(session -> session.load(Person.class, 123L));
     *
     * // Load and transform data inside the callback so nothing escapes the session
     * List<String> names = executor.call(session ->
     *     session.loadAll(Person.class).stream()
     *         .map(Person::getName)
     *         .collect(Collectors.toList()));
     * }</pre>
     *
     * @param <T> the type returned by {@code action}
     * @param action the function invoked with the pooled session; must not be {@code null}
     * @return the value produced by {@code action}
     * @see #run(Consumer)
     */
    @Beta
    public <T> T call(final Function<? super Session, ? extends T> action) {
        final Session session = getSession();

        try {
            return action.apply(session);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a single node by its native Neo4j ID using the OGM session's default depth.
     * <p>
     * Delegates to {@link Session#load(Class, java.io.Serializable)}. The session's default load
     * depth is&nbsp;1, so immediate relationships are typically also loaded; use
     * {@link #load(Class, Long, int)} to control the depth explicitly.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Person person = executor.load(Person.class, 123L);
     * if (person != null) {
     *     System.out.println("Person: " + person.getName());
     * }
     * }</pre>
     *
     * @param <T> the entity type mapped by OGM
     * @param targetClass the OGM-mapped class to load
     * @param id the native Neo4j node ID
     * @return the loaded entity, or {@code null} if no node with that ID exists
     * @see #load(Class, Long, int)
     * @see #loadAll(Class, Collection)
     */
    public <T> T load(final Class<T> targetClass, final Long id) {
        final Session session = getSession();

        try {
            return session.load(targetClass, id);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a single node by its native Neo4j ID, populating related entities to the given depth.
     * <p>
     * Delegates to {@link Session#load(Class, java.io.Serializable, int)}. {@code depth&nbsp;==&nbsp;0}
     * loads only the node's scalar properties; {@code depth&nbsp;==&nbsp;1} loads it together with
     * immediate relationships; larger depths recurse further; {@code -1} loads the entire connected
     * sub-graph reachable from this node and should be used with care on large graphs.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Person personOnly = executor.load(Person.class, 123L, 0);   // properties only
     * Person withCompany = executor.load(Person.class, 123L, 1);  // immediate relationships
     * Person fullGraph = executor.load(Person.class, 123L, -1);   // full reachable sub-graph
     * }</pre>
     *
     * @param <T> the entity type mapped by OGM
     * @param targetClass the OGM-mapped class to load
     * @param id the native Neo4j node ID
     * @param depth the depth of relationships to traverse: {@code 0} for the node only, a positive
     *              integer for that many hops, or {@code -1} for unlimited
     * @return the loaded entity, or {@code null} if no node with that ID exists
     * @see #load(Class, Long)
     * @see #loadAll(Class, Collection, int)
     */
    public <T> T load(final Class<T> targetClass, final Long id, final int depth) {
        final Session session = getSession();

        try {
            return session.load(targetClass, id, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads multiple nodes from Neo4j by their unique IDs.
     * <p>
     * This method efficiently retrieves multiple nodes in a single database operation.
     * Only the node properties are loaded; relationships are not included. For loading
     * relationships, use {@link #loadAll(Class, Collection, int)} with depth > 0.
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids collection of unique Neo4j node IDs to load
     * @return collection of loaded node entities, empty collection if no nodes found
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Class, Collection, int)
     * @see #load(Class, Long)
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<Long> ids) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads multiple nodes from Neo4j by their unique IDs with specified relationship depth.
     * <p>
     * This method efficiently retrieves multiple nodes and their relationships in a single
     * database operation. The depth parameter controls how deeply connected nodes are loaded.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids collection of unique Neo4j node IDs to load
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of loaded node entities with relationships, may be empty if no nodes found
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Class, Collection)
     * @see #load(Class, Long, int)
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<Long> ids, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads multiple nodes from Neo4j by their unique IDs with specified sort order.
     * <p>
     * This method retrieves multiple nodes and applies the specified sorting to the results.
     * Only the node properties are loaded; relationships are not included.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids collection of unique Neo4j node IDs to load
     * @param sortOrder the sort order specification for results
     * @return collection of loaded node entities sorted as specified, may be empty if no nodes found
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Class, Collection, SortOrder, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<Long> ids, final SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads multiple nodes from Neo4j by their unique IDs with specified sort order and relationship depth.
     * <p>
     * This method retrieves multiple nodes and their relationships, applying the specified sorting
     * to the results. The depth parameter controls how deeply connected nodes are loaded.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids collection of unique Neo4j node IDs to load
     * @param sortOrder the sort order specification for results
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of loaded node entities with relationships, sorted as specified
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Class, Collection, SortOrder)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<Long> ids, final SortOrder sortOrder, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads multiple nodes from Neo4j by their unique IDs with pagination.
     * <p>
     * This method retrieves a subset of nodes based on pagination settings, allowing
     * efficient handling of large result sets. Only node properties are loaded.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids collection of unique Neo4j node IDs to load
     * @param pagination pagination settings (page offset and size)
     * @return collection of loaded node entities for the specified page, may be empty
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Class, Collection, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<Long> ids, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads multiple nodes from Neo4j by their unique IDs with pagination and relationship depth.
     * <p>
     * This method retrieves a subset of nodes and their relationships based on pagination settings,
     * allowing efficient handling of large result sets with related data.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids collection of unique Neo4j node IDs to load
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of loaded node entities with relationships for the specified page
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Class, Collection, Pagination)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<Long> ids, final Pagination pagination, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads multiple nodes from Neo4j by their unique IDs with sort order and pagination.
     * <p>
     * This method retrieves a subset of sorted nodes based on pagination settings.
     * The combination of sorting and pagination provides efficient navigation through
     * large, ordered result sets.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids collection of unique Neo4j node IDs to load
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @return collection of loaded, sorted node entities for the specified page
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Class, Collection, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<Long> ids, final SortOrder sortOrder, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads multiple nodes from Neo4j by their unique IDs with sort order, pagination, and relationship depth.
     * <p>
     * This is the most comprehensive loadAll method, providing full control over sorting,
     * pagination, and relationship loading. Ideal for building complex data views with
     * navigation and detailed relationship information.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids collection of unique Neo4j node IDs to load
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of loaded, sorted node entities with relationships for the specified page
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Class, Collection, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<Long> ids, final SortOrder sortOrder, final Pagination pagination,
            final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Reloads a collection of existing node entities from Neo4j.
     * <p>
     * This method refreshes the provided entities from the database, updating their
     * properties to the current state in Neo4j. The entities must already have IDs.
     * Only node properties are reloaded; relationships are not included.
     * 
     * @param <T> the node type
     * @param objects collection of node entities to reload
     * @return collection of reloaded node entities with current database state
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Collection, int)
     */
    public <T> Collection<T> loadAll(final Collection<T> objects) {
        final Session session = getSession();

        try {
            return session.loadAll(objects);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with specified relationship depth.
     * <p>
     * This method refreshes the provided entities from the database and loads their
     * relationships up to the specified depth. Useful for ensuring entities have
     * the most current data and relationship information.
     * 
     * @param <T> the node type
     * @param objects collection of node entities to reload
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of reloaded node entities with relationships and current database state
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Collection)
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with specified sort order.
     * <p>
     * This method refreshes the provided entities from the database and returns them
     * sorted according to the specified order. Useful for maintaining consistent
     * ordering when refreshing entity collections.
     * 
     * @param <T> the node type
     * @param objects collection of node entities to reload
     * @param sortOrder the sort order specification for results
     * @return collection of reloaded, sorted node entities with current database state
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Collection, SortOrder, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with sort order and relationship depth.
     * <p>
     * This method refreshes the provided entities, loads their relationships to the specified
     * depth, and returns them sorted according to the specified order. Provides comprehensive
     * entity refresh with related data and ordering.
     * 
     * @param <T> the node type
     * @param objects collection of node entities to reload
     * @param sortOrder the sort order specification for results
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of reloaded, sorted node entities with relationships
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Collection, SortOrder)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final SortOrder sortOrder, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with pagination.
     * <p>
     * This method refreshes the provided entities and returns a paginated subset.
     * Useful for refreshing large collections of entities while managing memory usage
     * and response times.
     * 
     * @param <T> the node type
     * @param objects collection of node entities to reload
     * @param pagination pagination settings (page offset and size)
     * @return paginated collection of reloaded node entities with current database state
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Collection, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with pagination and relationship depth.
     * <p>
     * This method refreshes the provided entities with their relationships and returns a
     * paginated subset. Combines entity refresh, relationship loading, and pagination
     * for comprehensive data management.
     * 
     * @param <T> the node type
     * @param objects collection of node entities to reload
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return paginated collection of reloaded node entities with relationships
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Collection, Pagination)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final Pagination pagination, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with sort order and pagination.
     * <p>
     * This method refreshes the provided entities, sorts them according to the specified
     * order, and returns a paginated subset. Ideal for building sorted, paginated views
     * of refreshed entity data.
     * 
     * @param <T> the node type
     * @param objects collection of node entities to reload
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @return paginated collection of reloaded, sorted node entities
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Collection, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final SortOrder sortOrder, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with sort order, pagination, and relationship depth.
     * <p>
     * This is the most comprehensive reload method for existing entities, providing full control
     * over sorting, pagination, and relationship loading. Perfect for building complex data views
     * that require refreshed entity data with complete relationship information.
     * 
     * @param <T> the node type
     * @param objects collection of node entities to reload
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return paginated collection of reloaded, sorted node entities with relationships
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #loadAll(Collection, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final SortOrder sortOrder, final Pagination pagination, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads all nodes of the specified type from Neo4j.
     * <p>
     * This method retrieves all nodes of the given type from the database.
     * Only node properties are loaded; relationships are not included.
     * Use with caution on large datasets as it loads all matching nodes.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @return collection of all nodes of the specified type, may be empty
     * @throws IllegalArgumentException if targetClass is null
     * @see #loadAll(Class, int)
     * @see #loadAll(Class, Pagination)
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads all nodes of the specified type from Neo4j with relationship depth.
     * <p>
     * This method retrieves all nodes of the given type and their relationships
     * up to the specified depth. Use with caution on large datasets as it loads
     * all matching nodes and their relationships.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of all nodes of the specified type with relationships, may be empty
     * @throws IllegalArgumentException if targetClass is null
     * @see #loadAll(Class)
     * @see #loadAll(Class, Pagination, int)
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads all nodes of the specified type from Neo4j with sort order.
     * <p>
     * This method retrieves all nodes of the given type and sorts them according
     * to the specified order. Only node properties are loaded. Use with caution
     * on large datasets as it loads and sorts all matching nodes.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param sortOrder the sort order specification for results
     * @return collection of all sorted nodes of the specified type, may be empty
     * @throws IllegalArgumentException if targetClass is null
     * @see #loadAll(Class, SortOrder, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads all nodes of the specified type from Neo4j with sort order and relationship depth.
     * <p>
     * This method retrieves all nodes of the given type, loads their relationships to
     * the specified depth, and sorts the results. Use with caution on large datasets
     * as it loads all matching nodes with their relationships.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param sortOrder the sort order specification for results
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of all sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass is null
     * @see #loadAll(Class, SortOrder)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final SortOrder sortOrder, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated subset of nodes of the specified type from Neo4j.
     * <p>
     * This method retrieves a specific page of nodes of the given type, allowing
     * efficient navigation through large datasets. Only node properties are loaded.
     * Recommended approach for handling large node collections.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param pagination pagination settings (page offset and size)
     * @return paginated collection of nodes of the specified type, may be empty
     * @throws IllegalArgumentException if targetClass is null
     * @see #loadAll(Class, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated subset of nodes of the specified type from Neo4j with relationship depth.
     * <p>
     * This method retrieves a specific page of nodes and their relationships, providing
     * efficient navigation through large datasets with related data. Combines pagination
     * with relationship loading for optimal memory usage.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return paginated collection of nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass is null
     * @see #loadAll(Class, Pagination)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Pagination pagination, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated, sorted subset of nodes of the specified type from Neo4j.
     * <p>
     * This method retrieves a specific page of nodes, sorted according to the specified
     * order. Ideal for building paginated, sorted views of large node collections.
     * Only node properties are loaded.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @return paginated collection of sorted nodes of the specified type, may be empty
     * @throws IllegalArgumentException if targetClass is null
     * @see #loadAll(Class, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final SortOrder sortOrder, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated, sorted subset of nodes of the specified type from Neo4j with relationship depth.
     * <p>
     * This is the most comprehensive loadAll method for class-based loading, providing full control
     * over sorting, pagination, and relationship loading. Perfect for building complex, navigable
     * data views with complete relationship information.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return paginated collection of sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass is null
     * @see #loadAll(Class, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final SortOrder sortOrder, final Pagination pagination, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads nodes of the specified type from Neo4j that match the given filter criteria.
     * <p>
     * This method retrieves nodes that satisfy the specified filter conditions.
     * Only node properties are loaded; relationships are not included.
     * This is the primary method for filtered node retrieval.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes
     * @return collection of nodes matching the filter criteria, may be empty
     * @throws IllegalArgumentException if targetClass is null or filter is null
     * @see #loadAll(Class, Filter, int)
     * @see org.neo4j.ogm.cypher.Filter
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads nodes of the specified type from Neo4j that match the filter criteria with relationship depth.
     * <p>
     * This method retrieves nodes that satisfy the filter conditions and loads their
     * relationships up to the specified depth. Combines filtering with relationship
     * loading for comprehensive data retrieval.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of filtered nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass is null or filter is null
     * @see #loadAll(Class, Filter)
     * @see org.neo4j.ogm.cypher.Filter
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads nodes of the specified type that match the filter criteria with sort order.
     * <p>
     * This method retrieves nodes that satisfy the filter conditions and sorts them
     * according to the specified order. Combines filtering with sorting for organized
     * data retrieval. Only node properties are loaded.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes
     * @param sortOrder the sort order specification for results
     * @return collection of filtered, sorted nodes, may be empty
     * @throws IllegalArgumentException if targetClass, filter, or sortOrder is null
     * @see #loadAll(Class, Filter, SortOrder, int)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads nodes of the specified type that match the filter criteria with sort order and relationship depth.
     * <p>
     * This method retrieves nodes that satisfy the filter conditions, loads their relationships
     * to the specified depth, and sorts the results. Provides comprehensive filtered data
     * retrieval with complete relationship information.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes
     * @param sortOrder the sort order specification for results
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of filtered, sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass, filter, or sortOrder is null
     * @see #loadAll(Class, Filter, SortOrder)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final SortOrder sortOrder, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated subset of nodes that match the filter criteria.
     * <p>
     * This method retrieves a specific page of nodes that satisfy the filter conditions.
     * Combines filtering with pagination for efficient handling of large filtered datasets.
     * Only node properties are loaded.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes
     * @param pagination pagination settings (page offset and size)
     * @return paginated collection of filtered nodes, may be empty
     * @throws IllegalArgumentException if targetClass, filter, or pagination is null
     * @see #loadAll(Class, Filter, Pagination, int)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated subset of nodes that match the filter criteria with relationship depth.
     * <p>
     * This method retrieves a specific page of nodes that satisfy the filter conditions
     * and loads their relationships to the specified depth. Combines filtering, pagination,
     * and relationship loading for comprehensive data management.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return paginated collection of filtered nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass, filter, or pagination is null
     * @see #loadAll(Class, Filter, Pagination)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final Pagination pagination, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated, sorted subset of nodes that match the filter criteria.
     * <p>
     * This method retrieves a specific page of nodes that satisfy the filter conditions
     * and sorts them according to the specified order. Combines filtering, sorting,
     * and pagination for building organized, navigable data views.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @return paginated collection of filtered, sorted nodes, may be empty
     * @throws IllegalArgumentException if targetClass, filter, sortOrder, or pagination is null
     * @see #loadAll(Class, Filter, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final SortOrder sortOrder, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated, sorted subset of nodes that match the filter criteria with relationship depth.
     * <p>
     * This is the most comprehensive filtered loadAll method, providing full control over
     * filtering, sorting, pagination, and relationship loading. Perfect for building
     * complex, navigable, filtered data views with complete relationship information.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return paginated collection of filtered, sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if any required parameter is null
     * @see #loadAll(Class, Filter, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final SortOrder sortOrder, final Pagination pagination, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads nodes of the specified type from Neo4j that match the given multiple filter criteria.
     * <p>
     * This method retrieves nodes that satisfy all the specified filter conditions.
     * The Filters object allows combining multiple filter criteria with logical operators.
     * Only node properties are loaded; relationships are not included.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes
     * @return collection of nodes matching all filter criteria, may be empty
     * @throws IllegalArgumentException if targetClass is null or filters is null
     * @see #loadAll(Class, Filters, int)
     * @see #loadAll(Class, Filter)
     * @see org.neo4j.ogm.cypher.Filters
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads nodes of the specified type that match the multiple filter criteria with relationship depth.
     * <p>
     * This method retrieves nodes that satisfy all the filter conditions and loads their
     * relationships up to the specified depth. Combines complex filtering with relationship
     * loading for comprehensive data retrieval with multiple criteria.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of filtered nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass is null or filters is null
     * @see #loadAll(Class, Filters)
     * @see org.neo4j.ogm.cypher.Filters
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads nodes of the specified type that match multiple filter criteria with sort order.
     * <p>
     * This method retrieves nodes that satisfy all the filter conditions and sorts them
     * according to the specified order. Combines complex filtering with sorting for
     * organized data retrieval with multiple criteria. Only node properties are loaded.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes
     * @param sortOrder the sort order specification for results
     * @return collection of filtered, sorted nodes, may be empty
     * @throws IllegalArgumentException if targetClass, filters, or sortOrder is null
     * @see #loadAll(Class, Filters, SortOrder, int)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads nodes of the specified type that match multiple filter criteria with sort order and relationship depth.
     * <p>
     * This method retrieves nodes that satisfy all the filter conditions, loads their
     * relationships to the specified depth, and sorts the results. Provides comprehensive
     * filtered data retrieval with complete relationship information and multiple criteria.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes
     * @param sortOrder the sort order specification for results
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return collection of filtered, sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass, filters, or sortOrder is null
     * @see #loadAll(Class, Filters, SortOrder)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final SortOrder sortOrder, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated subset of nodes that match multiple filter criteria.
     * <p>
     * This method retrieves a specific page of nodes that satisfy all the filter conditions.
     * Combines complex filtering with pagination for efficient handling of large filtered
     * datasets with multiple criteria. Only node properties are loaded.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes
     * @param pagination pagination settings (page offset and size)
     * @return paginated collection of filtered nodes, may be empty
     * @throws IllegalArgumentException if targetClass, filters, or pagination is null
     * @see #loadAll(Class, Filters, Pagination, int)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated subset of nodes that match multiple filter criteria with relationship depth.
     * <p>
     * This method retrieves a specific page of nodes that satisfy all the filter conditions
     * and loads their relationships to the specified depth. Combines complex filtering,
     * pagination, and relationship loading for comprehensive data management.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return paginated collection of filtered nodes with relationships, may be empty
     * @throws IllegalArgumentException if targetClass, filters, or pagination is null
     * @see #loadAll(Class, Filters, Pagination)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final Pagination pagination, final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated, sorted subset of nodes that match multiple filter criteria.
     * <p>
     * This method retrieves a specific page of nodes that satisfy all the filter conditions
     * and sorts them according to the specified order. Combines complex filtering, sorting,
     * and pagination for building sophisticated, navigable data views with multiple criteria.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @return paginated collection of filtered, sorted nodes, may be empty
     * @throws IllegalArgumentException if any required parameter is null
     * @see #loadAll(Class, Filters, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final SortOrder sortOrder, final Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Loads a paginated, sorted subset of nodes that match multiple filter criteria with relationship depth.
     * <p>
     * This is the most comprehensive filtered loadAll method using Filters, providing complete control
     * over complex filtering, sorting, pagination, and relationship loading. Perfect for building
     * sophisticated, navigable data views with multiple filter criteria and full relationship information.
     * 
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes
     * @param sortOrder the sort order specification for results
     * @param pagination pagination settings (page offset and size)
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return paginated collection of filtered, sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if any required parameter is null
     * @see #loadAll(Class, Filters, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final SortOrder sortOrder, final Pagination pagination,
            final int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Persists an OGM-mapped entity to Neo4j using the session's default save depth.
     * <p>
     * Delegates to {@link Session#save(Object)}, which by default traverses the object graph at
     * unlimited depth ({@code -1}), saving the supplied entity together with every reachable
     * related entity. New nodes are created and assigned a generated ID (written back onto the
     * entity); existing nodes (those with an ID) are updated in place. Use {@link #save(Object, int)}
     * to bound the traversal explicitly.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create and save a new node
     * Person person = new Person("John Doe", 30);
     * executor.save(person);
     * System.out.println("Saved with ID: " + person.getId());
     *
     * // Update an existing node together with its (potentially deep) relationships
     * Person existing = executor.load(Person.class, 123L, 1);
     * existing.setAge(31);
     * executor.save(existing);
     * }</pre>
     *
     * @param <T> the entity type mapped by OGM
     * @param object the entity to save; must be an instance of an OGM-mapped class
     * @see #save(Object, int)
     */
    public <T> void save(final T object) {
        final Session session = getSession();

        try {
            session.save(object);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Persists an OGM-mapped entity to Neo4j, traversing related entities to the given depth.
     * <p>
     * Delegates to {@link Session#save(Object, int)}. {@code depth&nbsp;==&nbsp;0} saves only the
     * node's scalar properties (no relationships are written); a positive integer recursively
     * saves related entities to that many hops; {@code -1} saves the entire reachable sub-graph.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Person person = new Person("John Doe");
     * executor.save(person, 0);   // node only - no relationships written
     *
     * Person alice = new Person("Alice");
     * Company company = new Company("Tech Corp");
     * alice.worksFor(company);
     * executor.save(alice, 1);    // alice and the WORKS_FOR -> company edge
     *
     * executor.save(rootNode, -1); // entire reachable sub-graph (use with care)
     * }</pre>
     *
     * @param <T> the entity type mapped by OGM
     * @param object the entity to save
     * @param depth the depth of related entities to traverse and persist: {@code 0} for the node
     *              only, a positive integer for that many hops, or {@code -1} for unlimited
     * @see #save(Object)
     */
    public <T> void save(final T object, final int depth) {
        final Session session = getSession();

        try {
            session.save(object, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Deletes an OGM-managed entity (and any relationships incident to its underlying node) from
     * Neo4j.
     * <p>
     * Delegates to {@link Session#delete(Object)}. The entity must carry a populated graph ID
     * (i.e. it has previously been loaded or saved); otherwise OGM has nothing to delete. The
     * delete is irreversible.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Person person = executor.load(Person.class, 123L);
     * if (person != null) {
     *     executor.delete(person);
     * }
     *
     * Collection<Person> inactive = executor.loadAll(Person.class,
     *     new Filter("status", ComparisonOperator.EQUALS, "inactive"));
     * inactive.forEach(executor::delete);
     * }</pre>
     *
     * @param <T> the entity type mapped by OGM
     * @param object the entity to delete
     * @see #deleteAll(Class)
     */
    public <T> void delete(final T object) {
        final Session session = getSession();

        try {
            session.delete(object);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Deletes every node of the supplied OGM-mapped class (and all relationships incident to those
     * nodes) from Neo4j.
     * <p>
     * Delegates to {@link Session#deleteAll(Class)}. <strong>WARNING:</strong> this is a destructive
     * bulk operation that cannot be undone; verify the target class before invoking it.
     *
     * @param <T> the entity type mapped by OGM
     * @param targetClass the OGM-mapped class whose nodes are to be deleted
     * @see #delete(Object)
     */
    public <T> void deleteAll(final Class<T> targetClass) {
        final Session session = getSession();

        try {
            session.deleteAll(targetClass);
        } finally {
            closeSession(session);
        }

    }

    //    public <T> List<T> queryDto(final Class<T> type, final String cypher, final Map<String, ?> parameters) {
    //        final Session session = getSession();
    //
    //        try {
    //            return session.queryDto(cypher, parameters, type);
    //        } finally {
    //            closeSession(session);
    //        }
    //    }

    /**
     * Executes a Cypher query and maps the single result row to an instance of {@code objectType}.
     * <p>
     * Delegates to {@link Session#queryForObject(Class, String, Map)}. The query must return
     * exactly zero or one row; if multiple rows are returned the underlying OGM session raises a
     * {@link RuntimeException}. Add a {@code LIMIT 1} clause or a uniqueness constraint to the
     * Cypher to guarantee that.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query for a single person by name
     * Map<String, Object> params = Map.of("name", "John Doe");
     * Person person = executor.queryForObject(Person.class,
     *     "MATCH (p:Person {name: $name}) RETURN p", params);
     *
     * // Query for single aggregation result
     * Long count = executor.queryForObject(Long.class,
     *     "MATCH (p:Person) WHERE p.age > $minAge RETURN count(p) as count",
     *     Map.of("minAge", 25));
     *
     * // Query with LIMIT 1 to guarantee a single row
     * Person oldest = executor.queryForObject(Person.class,
     *     "MATCH (p:Person) RETURN p ORDER BY p.age DESC LIMIT 1",
     *     Collections.emptyMap());
     * }</pre>
     *
     * @param <T> the result type
     * @param objectType the target class &mdash; either a mapped OGM entity class or a basic value
     *                   type (e.g. {@code Long.class} for an aggregate result)
     * @param cypher the Cypher query string with {@code $name} parameter placeholders
     * @param parameters named parameters bound by the OGM session; may be empty but should not be
     *                   {@code null}
     * @return the single mapped result, or {@code null} if the query returns no rows
     * @throws RuntimeException if the query returns more than one row or the underlying OGM session
     *                          rejects the query
     * @see #query(Class, String, Map)
     * @see #query(String, Map)
     */
    public <T> T queryForObject(final Class<T> objectType, final String cypher, final Map<String, ?> parameters) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing Cypher: {}", cypher);
        }

        final Session session = getSession();

        try {
            return session.queryForObject(objectType, cypher, parameters);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Executes a Cypher query and returns each result row as a {@code Map<String, Object>} keyed
     * by the column alias declared in the {@code RETURN} clause.
     * <p>
     * The returned stream is lazy: the underlying OGM session is borrowed from the pool when this
     * method is invoked, and is returned to the pool when the stream is closed. <b>Callers must
     * close the stream</b> (preferably with try-with-resources); leaving it open leaks a session
     * for the lifetime of the executor.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String cypher = "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) " +
     *                 "WHERE c.name = $companyName " +
     *                 "RETURN p.name as personName, c.name as companyName";
     * Map<String, Object> params = Map.of("companyName", "Tech Corp");
     *
     * try (Stream<Map<String, Object>> rows = executor.query(cypher, params)) {
     *     rows.forEach(row -> System.out.println(
     *             row.get("personName") + " works at " + row.get("companyName")));
     * }
     * }</pre>
     *
     * @param cypher the Cypher query string with {@code $name} parameter placeholders
     * @param parameters named parameters bound by the OGM session
     * @return a lazily-evaluated {@link Stream} of result rows, each row a {@code Map} keyed by the
     *         {@code RETURN}-clause aliases; close the stream to release the underlying session
     * @throws RuntimeException if the underlying OGM session rejects the query
     * @see #query(Class, String, Map)
     * @see #query(String, Map, boolean)
     * @see #queryForObject(Class, String, Map)
     */
    public Stream<Map<String, Object>> query(final String cypher, final Map<String, ?> parameters) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing Cypher: {}", cypher);
        }

        final Session session = getSession();
        try {
            return Stream.of(session.query(cypher, parameters).iterator()).onClose(newCloseHandle(session));
        } catch (RuntimeException | Error e) {
            closeSession(session);
            throw e;
        }
    }

    /**
     * Executes a Cypher query with an explicit read-only hint and returns each row as a
     * {@code Map<String, Object>}.
     * <p>
     * The {@code readOnly} flag is forwarded to {@link Session#query(String, Map, boolean)} so the
     * Neo4j driver can route the request to a read replica in a clustered deployment or otherwise
     * optimise execution. Pass {@code false} for any query that mutates the graph; passing
     * {@code true} for a write query will result in a runtime failure from the driver.
     * <p>
     * The returned stream is lazy and borrows a pooled session for its lifetime; close the stream
     * (try-with-resources) to release the session back to the pool.
     *
     * @param cypher the Cypher query string with {@code $name} parameter placeholders
     * @param parameters named parameters bound by the OGM session
     * @param readOnly {@code true} to mark the query as read-only (eligible for read-replica
     *                 routing); must be {@code false} for queries that write to the graph
     * @return a lazily-evaluated {@link Stream} of result rows; close the stream to release the
     *         underlying session
     * @throws RuntimeException if the underlying OGM session rejects the query
     * @see #query(String, Map)
     * @see #query(Class, String, Map)
     */
    public Stream<Map<String, Object>> query(final String cypher, final Map<String, ?> parameters, final boolean readOnly) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing Cypher: {}", cypher);
        }

        final Session session = getSession();
        try {
            return Stream.of(session.query(cypher, parameters, readOnly).iterator()).onClose(newCloseHandle(session));
        } catch (RuntimeException | Error e) {
            closeSession(session);
            throw e;
        }
    }

    /**
     * Executes a Cypher query and maps each row to an instance of {@code objectType}.
     * <p>
     * Delegates to {@link Session#query(Class, String, Map)}. {@code objectType} is typically an
     * OGM-mapped entity class for queries that return whole nodes, but may also be a basic value
     * type when the {@code RETURN} clause produces a single scalar per row.
     * <p>
     * The returned stream is lazy and owns a pooled session for its lifetime; close the stream
     * (try-with-resources recommended) to release the session back to the pool.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String cypher = "MATCH (p:Person) WHERE p.age > $minAge RETURN p";
     * Map<String, Object> params = Map.of("minAge", 25);
     *
     * try (Stream<Person> adults = executor.query(Person.class, cypher, params)) {
     *     adults.filter(p -> p.getName().startsWith("A"))
     *           .forEach(p -> System.out.println(p.getName()));
     * }
     * }</pre>
     *
     * @param <T> the result row type
     * @param objectType the target class &mdash; an OGM-mapped entity class or a basic value type
     * @param cypher the Cypher query string with {@code $name} parameter placeholders
     * @param parameters named parameters bound by the OGM session
     * @return a lazily-evaluated {@link Stream} of rows mapped to {@code objectType}; close the
     *         stream to release the underlying session
     * @throws RuntimeException if the underlying OGM session rejects the query
     * @see #queryForObject(Class, String, Map)
     * @see #query(String, Map)
     */
    public <T> Stream<T> query(final Class<T> objectType, final String cypher, final Map<String, ?> parameters) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing Cypher: {}", cypher);
        }

        final Session session = getSession();
        try {
            return Stream.of(session.query(objectType, cypher, parameters).iterator()).onClose(newCloseHandle(session));
        } catch (RuntimeException | Error e) {
            closeSession(session);
            throw e;
        }
    }

    private Runnable newCloseHandle(final Session session) {
        return () -> closeSession(session);
    }

    /**
     * Counts the number of nodes of the specified type that match the given filter criteria.
     * <p>
     * This method efficiently counts nodes without loading them into memory.
     * Useful for pagination calculations and data analytics.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Count with single filter
     * Filter statusFilter = new Filter("status", ComparisonOperator.EQUALS, "active");
     * long activeCount = executor.count(Person.class, Arrays.asList(statusFilter));
     * System.out.println("Active persons: " + activeCount);
     *
     * // Count with multiple filters
     * Filters filters = new Filters();
     * filters.add(new Filter("status", ComparisonOperator.EQUALS, "active"));
     * filters.add(new Filter("age", ComparisonOperator.GREATER_THAN, 25));
     * long count = executor.count(Person.class, filters);
     *
     * // Count for pagination
     * Filter departmentFilter = new Filter("department", ComparisonOperator.EQUALS, "IT");
     * long totalItems = executor.count(Person.class, Arrays.asList(departmentFilter));
     * int pageSize = 10;
     * int totalPages = (int) Math.ceil((double) totalItems / pageSize);
     * }</pre>
     *
     * @param clazz the OGM-mapped class whose nodes are counted
     * @param filters an {@link Iterable} of OGM {@link Filter}s combined with AND semantics; pass
     *                an empty iterable to count all nodes of {@code clazz}
     * @return the number of nodes of {@code clazz} that satisfy all the supplied filters
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #countEntitiesOfType(Class)
     * @see org.neo4j.ogm.cypher.Filter
     */
    public long count(final Class<?> clazz, final Iterable<Filter> filters) {
        final Session session = getSession();

        try {
            return session.count(clazz, filters);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Counts the total number of nodes of the specified type in the Neo4j database.
     * <p>
     * This method efficiently counts all nodes of the given type without loading
     * them into memory. Useful for statistics, monitoring, and pagination setup.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Count all persons in the database
     * long totalPersons = executor.countEntitiesOfType(Person.class);
     * System.out.println("Total persons: " + totalPersons);
     *
     * // Count all companies
     * long totalCompanies = executor.countEntitiesOfType(Company.class);
     *
     * // Use for database statistics
     * Map<String, Long> stats = new HashMap<>();
     * stats.put("persons", executor.countEntitiesOfType(Person.class));
     * stats.put("companies", executor.countEntitiesOfType(Company.class));
     * stats.put("projects", executor.countEntitiesOfType(Project.class));
     *
     * // Check if database is empty
     * if (executor.countEntitiesOfType(Person.class) == 0) {
     *     System.out.println("No persons found, initializing data...");
     *     initializeData();
     * }
     * }</pre>
     *
     * @param entity the OGM-mapped class whose nodes are counted
     * @return the total number of nodes mapped by {@code entity}
     * @throws RuntimeException if the underlying OGM session rejects the request
     * @see #count(Class, Iterable)
     */
    public long countEntitiesOfType(final Class<?> entity) {
        final Session session = getSession();

        try {
            return session.countEntitiesOfType(entity);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Resolves the native Neo4j node ID for {@code possibleEntity}, if it is an OGM-managed
     * entity.
     * <p>
     * Delegates to {@link Session#resolveGraphIdFor(Object)}. Because the executor borrows a fresh
     * pooled session for the call, this only returns a non-null ID when the entity carries an ID
     * field that OGM has already populated (either because it was loaded from the database or
     * because a previous {@code save} assigned it). Transient objects that have never been saved
     * or that are not mapped at all return {@code null}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Person person = executor.load(Person.class, 123L);
     * Long graphId = executor.resolveGraphIdFor(person); // 123
     *
     * Person newPerson = new Person("John Doe");
     * executor.resolveGraphIdFor(newPerson); // null - not yet saved
     * executor.save(newPerson);
     * Long savedId = executor.resolveGraphIdFor(newPerson); // assigned by Neo4j
     * }</pre>
     *
     * @param possibleEntity an object that may or may not be an OGM-mapped entity
     * @return the native Neo4j graph ID for the entity, or {@code null} if {@code possibleEntity}
     *         is not a mapped/managed entity with an assigned ID
     * @see #load(Class, Long)
     */
    public Long resolveGraphIdFor(final Object possibleEntity) {
        final Session session = getSession();

        try {
            return session.resolveGraphIdFor(possibleEntity);
        } finally {
            closeSession(session);
        }
    }

    private Session getSession() {
        Session session = null;

        try {
            session = sessionPool.poll(100, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); // Preserve interrupt status
            // After interrupt, still need to return a session
            if (logger.isWarnEnabled()) {
                logger.warn("Interrupted while waiting for a session from the pool; opening a new session instead", e);
            }
            session = openSession();
            return session;
        }

        if (session == null) {
            session = openSession();
        }

        return session;
    }

    private void closeSession(final Session session) {
        if (session != null) {
            session.clear();

            try {
                sessionPool.offer(session, 100, TimeUnit.MILLISECONDS); //NOSONAR
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt(); // Preserve interrupt status
                if (logger.isWarnEnabled()) {
                    logger.warn("Interrupted while returning a session to the pool; the session may not be reused", e);
                }
            }
        }
    }

}
