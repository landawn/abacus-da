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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.ogm.cypher.Filter;
import org.neo4j.ogm.cypher.Filters;
import org.neo4j.ogm.cypher.function.FilterFunction;
import org.neo4j.ogm.cypher.query.Pagination;
import org.neo4j.ogm.cypher.query.SortOrder;
import org.neo4j.ogm.session.Neo4jSession;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.transaction.Transaction;
import org.neo4j.ogm.transaction.TransactionManager;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.da.cs;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Optional;

/**
 * A thin executor that wraps the <a href="https://neo4j.com/docs/ogm-manual/current/">Neo4j OGM</a>
 * {@link Session} API, adding lightweight session pooling and {@link Stream}-based result handling
 * on top of it. Each public operation borrows a {@link Session} from an internal pool, delegates to
 * the equivalent method on the OGM session, and returns the cleared session to the pool before the
 * operation returns. The OGM query methods used by the {@code stream} overloads materialize their
 * results eagerly, so the returned {@link Stream} does not retain a session.
 *
 * <h2>Session and Transaction Semantics</h2>
 * <ul>
 *   <li><b>Pool:</b> Sessions are held in a bounded {@link LinkedBlockingQueue} (capacity 8192).
 *       Each operation polls the queue without blocking and opens a fresh OGM session via
 *       {@link SessionFactory#openSession()} (a cheap in-memory operation) when the queue is
 *       empty.</li>
 *   <li><b>Release:</b> Any transaction left open on the session is rolled back, then
 *       {@link Session#clear()} is invoked before the
 *       session is offered back to the queue. If the queue is full the session is silently dropped
 *       (no {@code close()} call is made; the OGM {@link Session} has no {@code close} operation).
 *       A runtime failure while inspecting, rolling back, or clearing a returned session is logged
 *       and the session is discarded. The session return has try-with-resources semantics: once the
 *       database operation itself has thrown, nothing raised while returning its session &mdash; an
 *       {@link Error} included &mdash; can replace that exception; it is attached to the operation's
 *       exception as a suppressed exception instead. An {@code Error} raised by the session return
 *       propagates only when the operation completed normally.
 *       A session discarded because its leftover transaction could not be rolled back is still cleared
 *       (best effort), and for a {@link Neo4jSession} the stale transaction is detached from the
 *       session's transaction manager without committing it, so the discarded session is not kept
 *       reachable by the manager's thread-local state.
 *       Other session-level state mutated by callers of {@link #run(Consumer)}/{@link #call(Function)}
 *       — registered event listeners, bookmarks, load strategy — is <i>not</i> reset and would leak
 *       into subsequent borrowers; do not mutate such configuration on a pooled session.</li>
 *   <li><b>Transactions:</b> Each operation uses Neo4j-OGM's per-call transaction handling unless
 *       callers use {@link #run(Consumer)} or {@link #call(Function)} to open a multi-statement
 *       transaction explicitly via {@link Session#beginTransaction()}. Read/write routing is the OGM/driver's
 *       responsibility; the {@code readOnly} flag accepted by
 *       {@link #stream(String, Map, boolean)} is forwarded to the underlying driver as a routing
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
 * <p>The {@link #stream(String, Map)}, {@link #stream(String, Map, boolean)}, and
 * {@link #stream(Class, String, Map)} overloads execute the Cypher immediately and fully fetch the
 * result rows. The borrowed session is cleared and returned to the pool before the method returns;
 * the returned {@link Stream} lazily iterates only the already-materialized rows. Because the session
 * is already released, these streams hold no resource — closing them is optional and merely
 * conventional.</p>
 *
 * <h2>Load Depth</h2>
 * <p>A {@code depth} of {@code -1} (unlimited) is accepted by the {@code load}/{@code loadAll} overloads for
 * node-entity classes only: for a relationship-entity class Neo4j-OGM rejects any negative {@code depth} with an
 * {@link IllegalArgumentException} before querying the database.</p>
 *
 * <h2>Null Arguments</h2>
 * <p>A {@code null} argument that Neo4j-OGM can never accept is rejected up front with an
 * {@link IllegalArgumentException}, before a session is borrowed: the {@link SessionFactory} handed to
 * the constructor, the {@code action} callback of {@link #run(Consumer)} / {@link #call(Function)},
 * every {@code targetClass} (entity or query target class), the {@code id} of a {@code load} overload,
 * a single {@link Filter}, the saved/deleted {@code object}, the {@code filters} argument of
 * {@link #count(Class, Iterable)}, and the {@code cypher} and {@code parameters} of the query methods.
 * The remaining arguments are forwarded unvalidated to the OGM {@link Session}. With Neo4j-OGM 5.x a
 * {@code null} there is:</p>
 * <ul>
 *   <li>silently accepted, with the meaning documented on that parameter, for a {@code null}
 *       {@link Filters} (no filtering, so <i>every</i> node of the type is loaded),
 *       {@link Pagination} (no paging), {@link SortOrder} of a class-based {@code loadAll} overload (no
 *       ordering), {@code objects} collection (returned as-is) or {@code possibleEntity} in
 *       {@link #getGraphId(Object)} ({@code null} is returned);</li>
 *   <li>not rejected by this executor for the {@code ids} collection or the {@link SortOrder} of an
 *       {@code ids}/{@code objects} overload, because OGM tolerates it on some paths (an unmapped
 *       {@code targetClass} returns an empty collection before either is read, and a {@code null} or empty
 *       {@code objects} collection is returned before {@code sortOrder} is read); otherwise OGM typically
 *       fails with a {@link NullPointerException}.</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize executor (the Bolt driver must also be present at runtime)
 * Configuration configuration = new Configuration.Builder()
 *         .uri("bolt://localhost")
 *         .credentials("neo4j", "password")
 *         .build();
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
 * try (Stream<Person> adults = executor.stream(Person.class,
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
 * <p><b>Naming convention:</b> this executor stays close to the Neo4j OGM vocabulary ({@code load}/{@code loadAll},
 * {@code save}, {@code delete}/{@code deleteAll}, {@code count}) so it reads
 * naturally to Neo4j-OGM users, rather than wholesale adopting the abacus "house" CRUD vocabulary
 * ({@code list}/{@code insert}/{@code update}/{@code delete}) used by the
 * {@code Condition}-based executors such as Cassandra and BigQuery. It does, however, apply a few
 * abacus conventions where they improve the API: the single-row reader is exposed as
 * {@code findOnly(Class, String, Map)}, which returns a {@link com.landawn.abacus.util.u.Optional}
 * instead of OGM's {@code queryForObject} raw {@code null}; the OGM {@code countEntitiesOfType(Class)} is exposed as a {@code count(Class)}
 * overload; and OGM's {@code resolveGraphIdFor(Object)} is surfaced as the more conventional
 * {@code getGraphId(Object)}.</p>
 *
 * @see org.neo4j.ogm.session.SessionFactory
 * @see org.neo4j.ogm.session.Session
 * @see <a href="https://neo4j.com/docs/ogm-manual/current/">Neo4j OGM Documentation</a>
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Configuration configuration = new Configuration.Builder()
     *         .uri("bolt://localhost")
     *         .credentials("neo4j", "password")
     *         .build();
     * SessionFactory factory = new SessionFactory(configuration, "com.example.model");
     * Neo4jExecutor executor = new Neo4jExecutor(factory);   // ready to use; no session opened yet
     * assert executor.sessionFactory() == factory;           // factory is retained
     *
     * // A pre-built factory can be shared by many executors
     * Neo4jExecutor reader = new Neo4jExecutor(factory);
     *
     * // Passing null is rejected immediately
     * Neo4jExecutor bad = new Neo4jExecutor(null);           // throws IllegalArgumentException
     * }</pre>
     *
     * @param sessionFactory the Neo4j {@link SessionFactory} used to open new sessions on demand
     * @throws IllegalArgumentException if {@code sessionFactory} is {@code null}
     */
    public Neo4jExecutor(final SessionFactory sessionFactory) throws IllegalArgumentException {
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Configuration configuration = new Configuration.Builder()
     *         .uri("bolt://localhost")
     *         .credentials("neo4j", "password")
     *         .build();
     * SessionFactory factory = new SessionFactory(configuration, "com.example.model");
     * Neo4jExecutor executor = new Neo4jExecutor(factory);
     *
     * SessionFactory same = executor.sessionFactory();   // returns the exact instance passed in
     * assert same == factory;                            // true
     *
     * // Open an unpooled session straight from the factory for OGM-only features
     * Session adHoc = executor.sessionFactory().openSession();   // never null
     * }</pre>
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
     * Person personA = new Person("Alice", 30);
     * Person personB = new Person("Bob", 32);
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
     * @throws IllegalArgumentException if {@code action} is {@code null}
     * @throws RuntimeException if {@code action} throws while using the borrowed session
     * @see #call(Function)
     */
    @Beta
    public void run(final Consumer<? super Session> action) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(action, cs.action);

        runWithSession(action);
    }

    /**
     * Borrows a pooled {@link Session}, applies {@code action} to it, returns the result, and
     * releases the session back to the pool when {@code action} completes (whether normally or
     * exceptionally).
     * <p>
     * <b>Important:</b> {@code action} must not return or otherwise retain the {@link Session}
     * itself because it is cleared (see {@link Session#clear()}) and returned to the pool when this
     * method completes. Entity objects may be returned, but they are detached from that cleared
     * session and subsequent persistence work must use another executor operation or session.
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
     *         .toList());
     * }</pre>
     *
     * @param <T> the type returned by {@code action}
     * @param action the function invoked with the pooled session; must not be {@code null}
     * @return the value produced by {@code action}
     * @throws IllegalArgumentException if {@code action} is {@code null}
     * @throws RuntimeException if {@code action} throws while using the borrowed session
     * @see #run(Consumer)
     */
    @Beta
    public <T> T call(final Function<? super Session, ? extends T> action) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(action, cs.action);

        return withSession(action);
    }

    /**
     * Loads a single mapped entity by its OGM identifier using the default load depth.
     * <p>
     * Delegates to {@link Session#load(Class, java.io.Serializable)}. The session's default load
     * depth is&nbsp;1, so immediate relationships are typically also loaded; use
     * {@link #load(Class, Serializable, int)} to control the depth explicitly.
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
     * @param id the configured primary-index value, or a native graph ID for an entity class
     *           without a primary index
     * @return the loaded entity, or {@code null} if no node with that ID exists
     * @throws IllegalArgumentException if {@code targetClass} or {@code id} is {@code null}; also thrown by Neo4j-OGM if
     *         {@code targetClass} is not a managed entity class, or if the type of {@code id} does not match the class's
     *         primary index (or is not a {@code Long} native graph ID for a class without a primary index)
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #load(Class, Serializable, int)
     * @see #loadAll(Class, Collection)
     */
    public <T> T load(final Class<T> targetClass, final Serializable id) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(id, cs.id);

        return withSession(session -> session.load(targetClass, id));
    }

    /**
     * Loads a single mapped entity by its OGM identifier, populating related entities to the given
     * depth.
     * <p>
     * Delegates to {@link Session#load(Class, java.io.Serializable, int)}. {@code depth == 0}
     * loads only the node's scalar properties; {@code depth == 1} loads it together with
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
     * @param id the configured primary-index value, or a native graph ID for an entity class
     *           without a primary index
     * @param depth the depth of relationships to traverse: {@code 0} for the node only, a positive
     *              integer for that many hops, or {@code -1} for unlimited (node-entity classes only)
     * @return the loaded entity, or {@code null} if no node with that ID exists
     * @throws IllegalArgumentException if {@code targetClass} or {@code id} is {@code null}; also thrown by Neo4j-OGM if
     *         {@code targetClass} is not a managed entity class, if the type of {@code id} does not match the class's
     *         primary index (or is not a {@code Long} native graph ID for a class without a primary index), or if
     *         {@code depth} is negative for a relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #load(Class, Serializable)
     * @see #loadAll(Class, Collection, int)
     */
    public <T> T load(final Class<T> targetClass, final Serializable id, final int depth) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(id, cs.id);

        return withSession(session -> session.load(targetClass, id, depth));
    }

    /**
     * Loads multiple mapped entities from Neo4j by their identifiers.
     * <p>
     * This method efficiently retrieves multiple nodes in a single database operation.
     * Uses the OGM default load depth of 1, so immediate relationships are loaded along with node
     * properties; use {@link #loadAll(Class, Collection, int)} with depth 0 to load properties only.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> people = executor.loadAll(Person.class, Arrays.asList(1L, 2L, 3L));
     * // returns the matching Person nodes; IDs with no node are simply absent from the result
     *
     * for (Person p : executor.loadAll(Person.class, List.of(42L))) {
     *     System.out.println(p.getName());   // single-ID convenience
     * }
     *
     * // Empty id collection -> empty result
     * Collection<Person> none = executor.loadAll(Person.class, Collections.emptyList()); // returns empty collection
     *
     * // An unregistered/unmapped type yields an (immutable) empty collection rather than throwing
     * Collection<Object> unmapped = executor.loadAll(Object.class, List.of(1L));          // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids primary-index values, or native graph IDs for a class without a primary index
     * @return collection of loaded node entities, or an empty collection if no nodes are found
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Collection, int)
     * @see #load(Class, Serializable)
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<? extends Serializable> ids)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, ids));
    }

    /**
     * Loads multiple mapped entities from Neo4j by their identifiers with a specified relationship
     * depth.
     * <p>
     * This method efficiently retrieves multiple nodes and their relationships in a single
     * database operation. The depth parameter controls how deeply connected nodes are loaded.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Long> ids = Arrays.asList(1L, 2L);
     * Collection<Person> flat = executor.loadAll(Person.class, ids, 0);     // properties only, no relationships
     * Collection<Person> withRels = executor.loadAll(Person.class, ids, 1); // each node plus immediate relationships
     *
     * // depth -1 pulls the entire reachable sub-graph for each node (use with care on large graphs)
     * Collection<Person> deep = executor.loadAll(Person.class, ids, -1);
     *
     * // No matches -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, Collections.emptyList(), 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids primary-index values, or native graph IDs for a class without a primary index
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of loaded node entities with relationships, may be empty if no nodes found
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Collection)
     * @see #load(Class, Serializable, int)
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<? extends Serializable> ids, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, ids, depth));
    }

    /**
     * Loads multiple mapped entities from Neo4j by their identifiers with a specified sort order.
     * <p>
     * This method retrieves multiple nodes and applies the specified sorting to the results.
     * The OGM default load depth of 1 applies: immediate relationships are loaded along with node properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Long> ids = Arrays.asList(1L, 2L, 3L);
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sorted = executor.loadAll(Person.class, ids, byName); // ordered by name ascending
     *
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Collection<Person> oldestFirst = executor.loadAll(Person.class, ids, byAgeDesc);
     *
     * // An empty SortOrder simply returns the matches in id order
     * Collection<Person> unordered = executor.loadAll(Person.class, ids, new SortOrder());
     *
     * // No matching ids -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, Collections.emptyList(), byName); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids primary-index values, or native graph IDs for a class without a primary index
     * @param sortOrder the sort order specification for results; must not be {@code null}
     * @return collection of loaded node entities sorted as specified, may be empty if no nodes found
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Collection, SortOrder, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<? extends Serializable> ids, final SortOrder sortOrder)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, ids, sortOrder));
    }

    /**
     * Loads multiple mapped entities from Neo4j by their identifiers with a specified sort order
     * and relationship depth.
     * <p>
     * This method retrieves multiple nodes and their relationships, applying the specified sorting
     * to the results. The depth parameter controls how deeply connected nodes are loaded.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Long> ids = Arrays.asList(1L, 2L);
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sorted = executor.loadAll(Person.class, ids, byName, 1); // sorted, with immediate relationships
     *
     * // Sorted nodes, properties only
     * Collection<Person> flat = executor.loadAll(Person.class, ids, byName, 0);
     *
     * // No matches -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, Collections.emptyList(), byName, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids primary-index values, or native graph IDs for a class without a primary index
     * @param sortOrder the sort order specification for results; must not be {@code null}
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of loaded node entities with relationships, sorted as specified
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Collection, SortOrder)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<? extends Serializable> ids, final SortOrder sortOrder, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, ids, sortOrder, depth));
    }

    /**
     * Loads multiple mapped entities from Neo4j by their identifiers with pagination.
     * <p>
     * This method retrieves a subset of nodes based on pagination settings, allowing
     * efficient handling of large result sets. The OGM default load depth of 1 applies (immediate relationships are loaded too).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
     * Pagination firstPage = new Pagination(0, 2);                               // page index 0, size 2
     * Collection<Person> page0 = executor.loadAll(Person.class, ids, firstPage); // at most 2 nodes
     *
     * Pagination secondPage = new Pagination(1, 2);    // page index 1, size 2
     * Collection<Person> page1 = executor.loadAll(Person.class, ids, secondPage);
     *
     * // A page beyond the available data -> empty collection
     * Pagination farPage = new Pagination(99, 2);
     * Collection<Person> none = executor.loadAll(Person.class, ids, farPage);     // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids primary-index values, or native graph IDs for a class without a primary index
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return collection of loaded node entities for the specified page, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Collection, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<? extends Serializable> ids, final Pagination pagination)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, ids, pagination));
    }

    /**
     * Loads multiple mapped entities from Neo4j by their identifiers with pagination and
     * relationship depth.
     * <p>
     * This method retrieves a subset of nodes and their relationships based on pagination settings,
     * allowing efficient handling of large result sets with related data.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Long> ids = Arrays.asList(1L, 2L, 3L, 4L);
     * Pagination page = new Pagination(0, 2);
     * Collection<Person> withRels = executor.loadAll(Person.class, ids, page, 1); // <=2 nodes, each with relationships
     *
     * // Same page, properties only
     * Collection<Person> flat = executor.loadAll(Person.class, ids, page, 0);
     *
     * // No matching ids -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, Collections.emptyList(), page, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids primary-index values, or native graph IDs for a class without a primary index
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of loaded node entities with relationships for the specified page
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Collection, Pagination)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<? extends Serializable> ids, final Pagination pagination, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, ids, pagination, depth));
    }

    /**
     * Loads multiple mapped entities from Neo4j by their identifiers with sort order and
     * pagination.
     * <p>
     * This method retrieves a subset of sorted nodes based on pagination settings.
     * The combination of sorting and pagination provides efficient navigation through
     * large, ordered result sets.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
     * SortOrder byName = new SortOrder().add("name");
     * Pagination firstPage = new Pagination(0, 2);
     * Collection<Person> page0 = executor.loadAll(Person.class, ids, byName, firstPage); // first 2 by name
     *
     * Collection<Person> page1 = executor.loadAll(Person.class, ids, byName, new Pagination(1, 2)); // next 2 by name
     *
     * // Page past the end -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, ids, byName, new Pagination(99, 2)); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids primary-index values, or native graph IDs for a class without a primary index
     * @param sortOrder the sort order specification for results; must not be {@code null}
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return collection of loaded, sorted node entities for the specified page
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Collection, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<? extends Serializable> ids, final SortOrder sortOrder,
            final Pagination pagination) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, ids, sortOrder, pagination));
    }

    /**
     * Loads multiple mapped entities from Neo4j by their identifiers with sort order, pagination,
     * and relationship depth.
     * <p>
     * This is the most comprehensive loadAll method, providing full control over sorting,
     * pagination, and relationship loading. Ideal for building complex data views with
     * navigation and detailed relationship information.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Long> ids = Arrays.asList(1L, 2L, 3L, 4L);
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Pagination page = new Pagination(0, 2);
     * Collection<Person> top2 = executor.loadAll(Person.class, ids, byAgeDesc, page, 1); // 2 oldest, with relationships
     *
     * // Properties only, sorted and paged
     * Collection<Person> flat = executor.loadAll(Person.class, ids, byAgeDesc, page, 0);
     *
     * // No matches -> empty collection
     * Collection<Person> none =
     *     executor.loadAll(Person.class, Collections.emptyList(), byAgeDesc, page, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param ids primary-index values, or native graph IDs for a class without a primary index
     * @param sortOrder the sort order specification for results; must not be {@code null}
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of loaded, sorted node entities with relationships for the specified page
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Collection, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Collection<? extends Serializable> ids, final SortOrder sortOrder,
            final Pagination pagination, final int depth) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, ids, sortOrder, pagination, depth));
    }

    /**
     * Reloads a collection of existing node entities from Neo4j.
     * <p>
     * This method extracts the identifiers from the provided entities and returns entities loaded
     * in the borrowed session; it does not mutate the supplied instances or collection. The
     * entities must already have identifiers: an entity whose primary-index value (or native graph ID,
     * for a class without a primary index) is {@code null} is skipped and is absent from the result.
     * The OGM default load depth of 1 applies.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> stale = executor.loadAll(Person.class, Arrays.asList(1L, 2L));
     * Collection<Person> fresh = executor.loadAll(stale);   // newly loaded results; stale is unchanged
     *
     * // A null collection is passed straight through and returned as-is
     * Collection<Person> nil = executor.loadAll((Collection<Person>) null); // returns null
     *
     * // An empty collection is returned unchanged (the same empty instance, no DB round-trip)
     * Collection<Person> empty = executor.loadAll(Collections.<Person> emptyList()); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param objects collection of node entities to reload; may be {@code null} or empty, in which
     *                case it is returned unchanged without a database round-trip
     * @return collection of reloaded node entities with current database state
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Collection, int)
     */
    public <T> Collection<T> loadAll(final Collection<T> objects) throws RuntimeException {
        return withSession(session -> session.loadAll(objects));
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with specified relationship depth.
     * <p>
     * This method reloads the provided entities from the database by identifier and loads their
     * relationships up to the specified depth. As with {@link #loadAll(Collection)}, the result holds
     * newly loaded instances; the supplied instances are not modified.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> people = executor.loadAll(Person.class, Arrays.asList(1L, 2L));
     * Collection<Person> withRels = executor.loadAll(people, 1);  // refreshed, with immediate relationships
     * Collection<Person> deep = executor.loadAll(people, -1);     // refreshed, whole reachable sub-graph
     *
     * // null in, null out
     * Collection<Person> nil = executor.loadAll((Collection<Person>) null, 1); // returns null
     *
     * // Empty in, same empty collection out
     * Collection<Person> empty = executor.loadAll(Collections.<Person> emptyList(), 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param objects collection of node entities to reload; may be {@code null} or empty, in which
     *                case it is returned unchanged without a database round-trip
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of reloaded node entities with relationships and current database state
     * @throws IllegalArgumentException thrown by Neo4j-OGM if {@code depth} is negative and {@code objects} is a non-empty collection of
     *         relationship entities
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Collection)
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final int depth) throws RuntimeException {
        return withSession(session -> session.loadAll(objects, depth));
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with specified sort order.
     * <p>
     * This method reloads the provided entities from the database by identifier and returns the
     * newly loaded instances sorted according to the specified order; the supplied instances are
     * not modified.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> people = executor.loadAll(Person.class, Arrays.asList(1L, 2L, 3L));
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sorted = executor.loadAll(people, byName);   // refreshed and ordered by name
     *
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Collection<Person> oldestFirst = executor.loadAll(people, byAgeDesc);
     *
     * // null in, null out
     * Collection<Person> nil = executor.loadAll((Collection<Person>) null, byName); // returns null
     *
     * // Empty in, same empty collection out
     * Collection<Person> empty = executor.loadAll(Collections.<Person> emptyList(), byName); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param objects collection of node entities to reload; may be {@code null} or empty, in which
     *                case it is returned unchanged without a database round-trip
     * @param sortOrder the sort order specification for results; must not be {@code null}
     * @return collection of reloaded, sorted node entities with current database state
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Collection, SortOrder, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final SortOrder sortOrder) throws RuntimeException {
        return withSession(session -> session.loadAll(objects, sortOrder));
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with sort order and relationship depth.
     * <p>
     * This method reloads the provided entities by identifier, loads their relationships to the
     * specified depth, and returns the newly loaded instances sorted according to the specified
     * order; the supplied instances are not modified.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> people = executor.loadAll(Person.class, Arrays.asList(1L, 2L));
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sortedWithRels = executor.loadAll(people, byName, 1); // ordered, with relationships
     * Collection<Person> sortedFlat = executor.loadAll(people, byName, 0);     // ordered, properties only
     *
     * // null in, null out
     * Collection<Person> nil = executor.loadAll((Collection<Person>) null, byName, 1); // returns null
     *
     * // Empty in, same empty collection out
     * Collection<Person> empty = executor.loadAll(Collections.<Person> emptyList(), byName, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param objects collection of node entities to reload; may be {@code null} or empty, in which
     *                case it is returned unchanged without a database round-trip
     * @param sortOrder the sort order specification for results; must not be {@code null}
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of reloaded, sorted node entities with relationships
     * @throws IllegalArgumentException thrown by Neo4j-OGM if {@code depth} is negative and {@code objects} is a non-empty collection of
     *         relationship entities
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Collection, SortOrder)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final SortOrder sortOrder, final int depth) throws RuntimeException {
        return withSession(session -> session.loadAll(objects, sortOrder, depth));
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with pagination.
     * <p>
     * This method reloads the provided entities by identifier and returns a paginated subset of
     * the newly loaded instances; the supplied instances are not modified.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> people = executor.loadAll(Person.class, Arrays.asList(1L, 2L, 3L, 4L));
     * Pagination page = new Pagination(0, 2);
     * Collection<Person> firstTwo = executor.loadAll(people, page);   // at most 2 refreshed entities
     *
     * Collection<Person> nextTwo = executor.loadAll(people, new Pagination(1, 2));
     *
     * // null in, null out
     * Collection<Person> nil = executor.loadAll((Collection<Person>) null, page); // returns null
     *
     * // Empty in, same empty collection out
     * Collection<Person> empty = executor.loadAll(Collections.<Person> emptyList(), page); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param objects collection of node entities to reload; may be {@code null} or empty, in which
     *                case it is returned unchanged without a database round-trip
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return paginated collection of reloaded node entities with current database state
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Collection, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final Pagination pagination) throws RuntimeException {
        return withSession(session -> session.loadAll(objects, pagination));
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with pagination and relationship depth.
     * <p>
     * This method reloads the provided entities by identifier together with their relationships
     * and returns a paginated subset of the newly loaded instances; the supplied instances are not
     * modified.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> people = executor.loadAll(Person.class, Arrays.asList(1L, 2L, 3L, 4L));
     * Pagination page = new Pagination(0, 2);
     * Collection<Person> withRels = executor.loadAll(people, page, 1); // <=2 refreshed entities, with relationships
     * Collection<Person> flat = executor.loadAll(people, page, 0);     // <=2 refreshed entities, properties only
     *
     * // null in, null out
     * Collection<Person> nil = executor.loadAll((Collection<Person>) null, page, 1); // returns null
     *
     * // Empty in, same empty collection out
     * Collection<Person> empty = executor.loadAll(Collections.<Person> emptyList(), page, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param objects collection of node entities to reload; may be {@code null} or empty, in which
     *                case it is returned unchanged without a database round-trip
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return paginated collection of reloaded node entities with relationships
     * @throws IllegalArgumentException thrown by Neo4j-OGM if {@code depth} is negative and {@code objects} is a non-empty collection of
     *         relationship entities
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Collection, Pagination)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final Pagination pagination, final int depth) throws RuntimeException {
        return withSession(session -> session.loadAll(objects, pagination, depth));
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with sort order and pagination.
     * <p>
     * This method reloads the provided entities by identifier, sorts the newly loaded instances
     * according to the specified order, and returns a paginated subset; the supplied instances are
     * not modified.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> people = executor.loadAll(Person.class, Arrays.asList(1L, 2L, 3L, 4L));
     * SortOrder byName = new SortOrder().add("name");
     * Pagination page = new Pagination(0, 2);
     * Collection<Person> firstTwoByName = executor.loadAll(people, byName, page); // first 2 refreshed, by name
     *
     * Collection<Person> nextTwoByName = executor.loadAll(people, byName, new Pagination(1, 2));
     *
     * // null in, null out
     * Collection<Person> nil = executor.loadAll((Collection<Person>) null, byName, page); // returns null
     *
     * // Empty in, same empty collection out
     * Collection<Person> empty = executor.loadAll(Collections.<Person> emptyList(), byName, page); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param objects collection of node entities to reload; may be {@code null} or empty, in which
     *                case it is returned unchanged without a database round-trip
     * @param sortOrder the sort order specification for results; must not be {@code null}
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return paginated collection of reloaded, sorted node entities
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Collection, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final SortOrder sortOrder, final Pagination pagination) throws RuntimeException {
        return withSession(session -> session.loadAll(objects, sortOrder, pagination));
    }

    /**
     * Reloads a collection of existing node entities from Neo4j with sort order, pagination, and relationship depth.
     * <p>
     * This is the most comprehensive reload method for existing entities, providing full control
     * over sorting, pagination, and relationship loading. Perfect for building complex data views
     * that require refreshed entity data with complete relationship information.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> people = executor.loadAll(Person.class, Arrays.asList(1L, 2L, 3L, 4L));
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Pagination page = new Pagination(0, 2);
     * Collection<Person> top2WithRels = executor.loadAll(people, byAgeDesc, page, 1); // 2 oldest, with relationships
     * Collection<Person> top2Flat = executor.loadAll(people, byAgeDesc, page, 0);     // 2 oldest, properties only
     *
     * // null in, null out
     * Collection<Person> nil = executor.loadAll((Collection<Person>) null, byAgeDesc, page, 1); // returns null
     *
     * // Empty in, same empty collection out
     * Collection<Person> empty =
     *     executor.loadAll(Collections.<Person> emptyList(), byAgeDesc, page, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param objects collection of node entities to reload; may be {@code null} or empty, in which
     *                case it is returned unchanged without a database round-trip
     * @param sortOrder the sort order specification for results; must not be {@code null}
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return paginated collection of reloaded, sorted node entities with relationships
     * @throws IllegalArgumentException thrown by Neo4j-OGM if {@code depth} is negative and {@code objects} is a non-empty collection of
     *         relationship entities
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Collection, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Collection<T> objects, final SortOrder sortOrder, final Pagination pagination, final int depth)
            throws RuntimeException {
        return withSession(session -> session.loadAll(objects, sortOrder, pagination, depth));
    }

    /**
     * Loads all nodes of the specified type from Neo4j.
     * <p>
     * This method retrieves all nodes of the given type from the database.
     * The OGM default load depth of 1 applies: immediate relationships are loaded along with node properties.
     * Use with caution on large datasets as it loads all matching nodes.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> everyone = executor.loadAll(Person.class);
     * System.out.println("Total persons: " + everyone.size());
     *
     * for (Person p : executor.loadAll(Person.class)) {
     *     System.out.println(p.getName());
     * }
     *
     * // A type with no stored nodes -> empty collection
     * Collection<Company> noneYet = executor.loadAll(Company.class); // returns empty collection when none exist
     *
     * // An unregistered/unmapped type returns an (immutable) empty collection rather than throwing
     * Collection<Object> unmapped = executor.loadAll(Object.class);  // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @return collection of all nodes of the specified type, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, int)
     * @see #loadAll(Class, Pagination)
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass));
    }

    /**
     * Loads all nodes of the specified type from Neo4j with relationship depth.
     * <p>
     * This method retrieves all nodes of the given type and their relationships
     * up to the specified depth. Use with caution on large datasets as it loads
     * all matching nodes and their relationships.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<Person> flat = executor.loadAll(Person.class, 0);     // every Person, properties only
     * Collection<Person> withRels = executor.loadAll(Person.class, 1); // every Person plus immediate relationships
     * Collection<Person> deep = executor.loadAll(Person.class, -1);    // entire reachable graph (use with care)
     *
     * // Unmapped type -> empty collection (no exception)
     * Collection<Object> unmapped = executor.loadAll(Object.class, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of all nodes of the specified type with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class)
     * @see #loadAll(Class, Pagination, int)
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final int depth) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, depth));
    }

    /**
     * Loads all nodes of the specified type from Neo4j with sort order.
     * <p>
     * This method retrieves all nodes of the given type and sorts them according
     * to the specified order. The OGM default load depth of 1 applies (immediate relationships are loaded too). Use with caution
     * on large datasets as it loads and sorts all matching nodes.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> byNameAsc = executor.loadAll(Person.class, byName); // all Persons, name ascending
     *
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Collection<Person> oldestFirst = executor.loadAll(Person.class, byAgeDesc);
     *
     * // Unmapped type -> empty collection (no exception)
     * Collection<Object> unmapped = executor.loadAll(Object.class, byName); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @return collection of all sorted nodes of the specified type, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, SortOrder, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final SortOrder sortOrder) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, sortOrder));
    }

    /**
     * Loads all nodes of the specified type from Neo4j with sort order and relationship depth.
     * <p>
     * This method retrieves all nodes of the given type, loads their relationships to
     * the specified depth, and sorts the results. Use with caution on large datasets
     * as it loads all matching nodes with their relationships.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sortedWithRels = executor.loadAll(Person.class, byName, 1); // all Persons, sorted, with relationships
     * Collection<Person> sortedFlat = executor.loadAll(Person.class, byName, 0);     // all Persons, sorted, properties only
     *
     * // Unmapped type -> empty collection (no exception)
     * Collection<Object> unmapped = executor.loadAll(Object.class, byName, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of all sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, SortOrder)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final SortOrder sortOrder, final int depth) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, sortOrder, depth));
    }

    /**
     * Loads a paginated subset of nodes of the specified type from Neo4j.
     * <p>
     * This method retrieves a specific page of nodes of the given type, allowing
     * efficient navigation through large datasets. The OGM default load depth of 1 applies (immediate relationships are loaded too).
     * Recommended approach for handling large node collections.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Pagination firstPage = new Pagination(0, 20);                         // page index 0, size 20
     * Collection<Person> page0 = executor.loadAll(Person.class, firstPage); // up to 20 Persons
     *
     * Pagination secondPage = new Pagination(1, 20);
     * Collection<Person> page1 = executor.loadAll(Person.class, secondPage);
     *
     * // A page beyond the stored data -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, new Pagination(9999, 20)); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return paginated collection of nodes of the specified type, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Pagination pagination) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, pagination));
    }

    /**
     * Loads a paginated subset of nodes of the specified type from Neo4j with relationship depth.
     * <p>
     * This method retrieves a specific page of nodes and their relationships, providing
     * efficient navigation through large datasets with related data. Combines pagination
     * with relationship loading for optimal memory usage.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Pagination page = new Pagination(0, 20);
     * Collection<Person> withRels = executor.loadAll(Person.class, page, 1); // up to 20 Persons, with relationships
     * Collection<Person> flat = executor.loadAll(Person.class, page, 0);     // up to 20 Persons, properties only
     *
     * // A page beyond the stored data -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, new Pagination(9999, 20), 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return paginated collection of nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Pagination)
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Pagination pagination, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, pagination, depth));
    }

    /**
     * Loads a paginated, sorted subset of nodes of the specified type from Neo4j.
     * <p>
     * This method retrieves a specific page of nodes, sorted according to the specified
     * order. Ideal for building paginated, sorted views of large node collections.
     * The OGM default load depth of 1 applies (immediate relationships are loaded too).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SortOrder byName = new SortOrder().add("name");
     * Pagination firstPage = new Pagination(0, 20);
     * Collection<Person> page0 = executor.loadAll(Person.class, byName, firstPage); // first 20 by name
     *
     * Collection<Person> page1 = executor.loadAll(Person.class, byName, new Pagination(1, 20)); // next 20 by name
     *
     * // A page beyond the stored data -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, byName, new Pagination(9999, 20)); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return paginated collection of sorted nodes of the specified type, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final SortOrder sortOrder, final Pagination pagination)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, sortOrder, pagination));
    }

    /**
     * Loads a paginated, sorted subset of nodes of the specified type from Neo4j with relationship depth.
     * <p>
     * This is the most comprehensive loadAll method for class-based loading, providing full control
     * over sorting, pagination, and relationship loading. Perfect for building complex, navigable
     * data views with complete relationship information.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Pagination page = new Pagination(0, 20);
     * Collection<Person> oldest20WithRels = executor.loadAll(Person.class, byAgeDesc, page, 1); // 20 oldest, with relationships
     * Collection<Person> oldest20Flat = executor.loadAll(Person.class, byAgeDesc, page, 0);     // 20 oldest, properties only
     *
     * // A page beyond the stored data -> empty collection
     * Collection<Person> none =
     *     executor.loadAll(Person.class, byAgeDesc, new Pagination(9999, 20), 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return paginated collection of sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final SortOrder sortOrder, final Pagination pagination, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, sortOrder, pagination, depth));
    }

    /**
     * Loads nodes of the specified type from Neo4j that match the given filter criteria.
     * <p>
     * This method retrieves nodes that satisfy the specified filter conditions.
     * The OGM default load depth of 1 applies: immediate relationships are loaded along with node properties.
     * This is the primary method for filtered node retrieval.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter nameIsJohn = new Filter("name", ComparisonOperator.EQUALS, "John");
     * Collection<Person> johns = executor.loadAll(Person.class, nameIsJohn); // every Person named "John"
     *
     * Filter adult = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * Collection<Person> adults = executor.loadAll(Person.class, adult);
     *
     * // A filter that matches nothing -> empty collection
     * Filter impossible = new Filter("name", ComparisonOperator.EQUALS, " no-such-name");
     * Collection<Person> none = executor.loadAll(Person.class, impossible); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes; must not be {@code null}
     * @return collection of nodes matching the filter criteria, may be empty
     * @throws IllegalArgumentException if {@code targetClass} or {@code filter} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filter, int)
     * @see org.neo4j.ogm.cypher.Filter
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filter, cs.filter);

        return withSession(session -> session.loadAll(targetClass, filter));
    }

    /**
     * Loads nodes of the specified type from Neo4j that match the filter criteria with relationship depth.
     * <p>
     * This method retrieves nodes that satisfy the filter conditions and loads their
     * relationships up to the specified depth. Combines filtering with relationship
     * loading for comprehensive data retrieval.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter adult = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * Collection<Person> withRels = executor.loadAll(Person.class, adult, 1); // matching Persons, with relationships
     * Collection<Person> flat = executor.loadAll(Person.class, adult, 0);     // matching Persons, properties only
     *
     * // A filter that matches nothing -> empty collection
     * Filter impossible = new Filter("name", ComparisonOperator.EQUALS, " no-such-name");
     * Collection<Person> none = executor.loadAll(Person.class, impossible, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes; must not be {@code null}
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of filtered nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} or {@code filter} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is
     *         negative for a relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filter)
     * @see org.neo4j.ogm.cypher.Filter
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final int depth) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filter, cs.filter);

        return withSession(session -> session.loadAll(targetClass, filter, depth));
    }

    /**
     * Loads nodes of the specified type that match the filter criteria with sort order.
     * <p>
     * This method retrieves nodes that satisfy the filter conditions and sorts them
     * according to the specified order. Combines filtering with sorting for organized
     * data retrieval. The OGM default load depth of 1 applies (immediate relationships are loaded too).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter adult = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sorted = executor.loadAll(Person.class, adult, byName); // matching Persons, ordered by name
     *
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Collection<Person> oldestFirst = executor.loadAll(Person.class, adult, byAgeDesc);
     *
     * // A filter that matches nothing -> empty collection
     * Filter impossible = new Filter("name", ComparisonOperator.EQUALS, " no-such-name");
     * Collection<Person> none = executor.loadAll(Person.class, impossible, byName); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes; must not be {@code null}
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @return collection of filtered, sorted nodes, may be empty
     * @throws IllegalArgumentException if {@code targetClass} or {@code filter} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filter, SortOrder, int)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final SortOrder sortOrder)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filter, cs.filter);

        return withSession(session -> session.loadAll(targetClass, filter, sortOrder));
    }

    /**
     * Loads nodes of the specified type that match the filter criteria with sort order and relationship depth.
     * <p>
     * This method retrieves nodes that satisfy the filter conditions, loads their relationships
     * to the specified depth, and sorts the results. Provides comprehensive filtered data
     * retrieval with complete relationship information.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter adult = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sortedWithRels = executor.loadAll(Person.class, adult, byName, 1); // filtered, sorted, with relationships
     * Collection<Person> sortedFlat = executor.loadAll(Person.class, adult, byName, 0);     // filtered, sorted, properties only
     *
     * // A filter that matches nothing -> empty collection
     * Filter impossible = new Filter("name", ComparisonOperator.EQUALS, " no-such-name");
     * Collection<Person> none = executor.loadAll(Person.class, impossible, byName, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes; must not be {@code null}
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of filtered, sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} or {@code filter} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is
     *         negative for a relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filter, SortOrder)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final SortOrder sortOrder, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filter, cs.filter);

        return withSession(session -> session.loadAll(targetClass, filter, sortOrder, depth));
    }

    /**
     * Loads a paginated subset of nodes that match the filter criteria.
     * <p>
     * This method retrieves a specific page of nodes that satisfy the filter conditions.
     * Combines filtering with pagination for efficient handling of large filtered datasets.
     * The OGM default load depth of 1 applies (immediate relationships are loaded too).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter adult = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * Pagination firstPage = new Pagination(0, 20);
     * Collection<Person> page0 = executor.loadAll(Person.class, adult, firstPage); // up to 20 matching Persons
     *
     * Collection<Person> page1 = executor.loadAll(Person.class, adult, new Pagination(1, 20)); // next page
     *
     * // A page beyond the matches -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, adult, new Pagination(9999, 20)); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes; must not be {@code null}
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return paginated collection of filtered nodes, may be empty
     * @throws IllegalArgumentException if {@code targetClass} or {@code filter} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filter, Pagination, int)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final Pagination pagination)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filter, cs.filter);

        return withSession(session -> session.loadAll(targetClass, filter, pagination));
    }

    /**
     * Loads a paginated subset of nodes that match the filter criteria with relationship depth.
     * <p>
     * This method retrieves a specific page of nodes that satisfy the filter conditions
     * and loads their relationships to the specified depth. Combines filtering, pagination,
     * and relationship loading for comprehensive data management.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter adult = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * Pagination page = new Pagination(0, 20);
     * Collection<Person> withRels = executor.loadAll(Person.class, adult, page, 1); // up to 20 matches, with relationships
     * Collection<Person> flat = executor.loadAll(Person.class, adult, page, 0);     // up to 20 matches, properties only
     *
     * // A page beyond the matches -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, adult, new Pagination(9999, 20), 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes; must not be {@code null}
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return paginated collection of filtered nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} or {@code filter} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is
     *         negative for a relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filter, Pagination)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final Pagination pagination, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filter, cs.filter);

        return withSession(session -> session.loadAll(targetClass, filter, pagination, depth));
    }

    /**
     * Loads a paginated, sorted subset of nodes that match the filter criteria.
     * <p>
     * This method retrieves a specific page of nodes that satisfy the filter conditions
     * and sorts them according to the specified order. Combines filtering, sorting,
     * and pagination for building organized, navigable data views.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter adult = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * SortOrder byName = new SortOrder().add("name");
     * Pagination firstPage = new Pagination(0, 20);
     * Collection<Person> page0 = executor.loadAll(Person.class, adult, byName, firstPage); // first 20 matches, by name
     *
     * Collection<Person> page1 = executor.loadAll(Person.class, adult, byName, new Pagination(1, 20));
     *
     * // A page beyond the matches -> empty collection
     * Collection<Person> none =
     *     executor.loadAll(Person.class, adult, byName, new Pagination(9999, 20)); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes; must not be {@code null}
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return paginated collection of filtered, sorted nodes, may be empty
     * @throws IllegalArgumentException if {@code targetClass} or {@code filter} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filter, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final SortOrder sortOrder, final Pagination pagination)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filter, cs.filter);

        return withSession(session -> session.loadAll(targetClass, filter, sortOrder, pagination));
    }

    /**
     * Loads a paginated, sorted subset of nodes that match the filter criteria with relationship depth.
     * <p>
     * This is the most comprehensive filtered loadAll method, providing full control over
     * filtering, sorting, pagination, and relationship loading. Perfect for building
     * complex, navigable, filtered data views with complete relationship information.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter adult = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Pagination page = new Pagination(0, 20);
     * Collection<Person> withRels = executor.loadAll(Person.class, adult, byAgeDesc, page, 1); // filtered/sorted/paged, with relationships
     * Collection<Person> flat = executor.loadAll(Person.class, adult, byAgeDesc, page, 0);     // filtered/sorted/paged, properties only
     *
     * // A page beyond the matches -> empty collection
     * Collection<Person> none =
     *     executor.loadAll(Person.class, adult, byAgeDesc, new Pagination(9999, 20), 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filter the filter criteria to apply when loading nodes; must not be {@code null}
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return paginated collection of filtered, sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} or {@code filter} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is
     *         negative for a relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filter, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.Filter
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filter filter, final SortOrder sortOrder, final Pagination pagination, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filter, cs.filter);

        return withSession(session -> session.loadAll(targetClass, filter, sortOrder, pagination, depth));
    }

    /**
     * Loads nodes of the specified type from Neo4j that match the given multiple filter criteria.
     * <p>
     * This method retrieves entities that satisfy the logical expression encoded by the
     * {@link Filters} object. Use {@link Filters#and(Filter)} and {@link Filters#or(Filter)} to
     * specify how each filter after the first is joined to the preceding expression.
     * The OGM default load depth of 1 applies: immediate relationships are loaded along with node properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filters filters = new Filters()
     *         .add(new Filter("status", ComparisonOperator.EQUALS, "active"))
     *         .and(new Filter("age", ComparisonOperator.GREATER_THAN, 18));
     * Collection<Person> activeAdults = executor.loadAll(Person.class, filters); // matches every supplied condition
     *
     * // An empty Filters object matches everything (equivalent to loadAll(Person.class))
     * Collection<Person> everyone = executor.loadAll(Person.class, new Filters());
     *
     * // Filters that match nothing -> empty collection
     * Filters impossible = new Filters().add(new Filter("name", ComparisonOperator.EQUALS, " none"));
     * Collection<Person> none = executor.loadAll(Person.class, impossible); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes; may be {@code null}
     *                or empty, in which case every node of {@code targetClass} is loaded
     * @return collection of entities matching the combined filter expression, possibly empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filters, int)
     * @see #loadAll(Class, Filter)
     * @see org.neo4j.ogm.cypher.Filters
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, filters));
    }

    /**
     * Loads nodes of the specified type that match the multiple filter criteria with relationship depth.
     * <p>
     * This method retrieves entities that satisfy the combined filter expression and loads their
     * relationships up to the specified depth. Combines complex filtering with relationship
     * loading for comprehensive data retrieval with multiple criteria.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filters filters = new Filters()
     *         .add(new Filter("status", ComparisonOperator.EQUALS, "active"))
     *         .and(new Filter("age", ComparisonOperator.GREATER_THAN, 18));
     * Collection<Person> withRels = executor.loadAll(Person.class, filters, 1); // matches, with immediate relationships
     * Collection<Person> flat = executor.loadAll(Person.class, filters, 0);     // matches, properties only
     *
     * // Filters that match nothing -> empty collection
     * Filters impossible = new Filters().add(new Filter("name", ComparisonOperator.EQUALS, " none"));
     * Collection<Person> none = executor.loadAll(Person.class, impossible, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes; may be {@code null}
     *                or empty, in which case every node of {@code targetClass} is loaded
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of filtered nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filters)
     * @see org.neo4j.ogm.cypher.Filters
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final int depth) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, filters, depth));
    }

    /**
     * Loads nodes of the specified type that match multiple filter criteria with sort order.
     * <p>
     * This method retrieves entities that satisfy the combined filter expression and sorts them
     * according to the specified order. Combines complex filtering with sorting for
     * organized data retrieval with multiple criteria. The OGM default load depth of 1 applies (immediate relationships are loaded too).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filters filters = new Filters()
     *         .add(new Filter("status", ComparisonOperator.EQUALS, "active"))
     *         .and(new Filter("age", ComparisonOperator.GREATER_THAN, 18));
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sorted = executor.loadAll(Person.class, filters, byName); // matches, ordered by name
     *
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Collection<Person> oldestFirst = executor.loadAll(Person.class, filters, byAgeDesc);
     *
     * // Filters that match nothing -> empty collection
     * Filters impossible = new Filters().add(new Filter("name", ComparisonOperator.EQUALS, " none"));
     * Collection<Person> none = executor.loadAll(Person.class, impossible, byName); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes; may be {@code null}
     *                or empty, in which case every node of {@code targetClass} is loaded
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @return collection of filtered, sorted nodes, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filters, SortOrder, int)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final SortOrder sortOrder)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, filters, sortOrder));
    }

    /**
     * Loads nodes of the specified type that match multiple filter criteria with sort order and relationship depth.
     * <p>
     * This method retrieves entities that satisfy the combined filter expression, loads their
     * relationships to the specified depth, and sorts the results. Provides comprehensive
     * filtered data retrieval with complete relationship information and multiple criteria.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filters filters = new Filters()
     *         .add(new Filter("status", ComparisonOperator.EQUALS, "active"))
     *         .and(new Filter("age", ComparisonOperator.GREATER_THAN, 18));
     * SortOrder byName = new SortOrder().add("name");
     * Collection<Person> sortedWithRels = executor.loadAll(Person.class, filters, byName, 1); // filtered, sorted, with relationships
     * Collection<Person> sortedFlat = executor.loadAll(Person.class, filters, byName, 0);     // filtered, sorted, properties only
     *
     * // Filters that match nothing -> empty collection
     * Filters impossible = new Filters().add(new Filter("name", ComparisonOperator.EQUALS, " none"));
     * Collection<Person> none = executor.loadAll(Person.class, impossible, byName, 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes; may be {@code null}
     *                or empty, in which case every node of {@code targetClass} is loaded
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return collection of filtered, sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filters, SortOrder)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.SortOrder
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final SortOrder sortOrder, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, filters, sortOrder, depth));
    }

    /**
     * Loads a paginated subset of nodes that match multiple filter criteria.
     * <p>
     * This method retrieves a specific page of entities that satisfy the combined filter expression.
     * Combines complex filtering with pagination for efficient handling of large filtered
     * datasets with multiple criteria. The OGM default load depth of 1 applies (immediate relationships are loaded too).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filters filters = new Filters()
     *         .add(new Filter("status", ComparisonOperator.EQUALS, "active"));
     * Pagination firstPage = new Pagination(0, 20);
     * Collection<Person> page0 = executor.loadAll(Person.class, filters, firstPage); // up to 20 matches
     *
     * Collection<Person> page1 = executor.loadAll(Person.class, filters, new Pagination(1, 20));
     *
     * // A page beyond the matches -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, filters, new Pagination(9999, 20)); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes; may be {@code null}
     *                or empty, in which case every node of {@code targetClass} is loaded
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return paginated collection of filtered nodes, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filters, Pagination, int)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final Pagination pagination)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, filters, pagination));
    }

    /**
     * Loads a paginated subset of nodes that match multiple filter criteria with relationship depth.
     * <p>
     * This method retrieves a specific page of entities that satisfy the combined filter expression
     * and loads their relationships to the specified depth. Combines complex filtering,
     * pagination, and relationship loading for comprehensive data management.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filters filters = new Filters()
     *         .add(new Filter("status", ComparisonOperator.EQUALS, "active"));
     * Pagination page = new Pagination(0, 20);
     * Collection<Person> withRels = executor.loadAll(Person.class, filters, page, 1); // up to 20 matches, with relationships
     * Collection<Person> flat = executor.loadAll(Person.class, filters, page, 0);     // up to 20 matches, properties only
     *
     * // A page beyond the matches -> empty collection
     * Collection<Person> none = executor.loadAll(Person.class, filters, new Pagination(9999, 20), 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes; may be {@code null}
     *                or empty, in which case every node of {@code targetClass} is loaded
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return paginated collection of filtered nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filters, Pagination)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final Pagination pagination, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, filters, pagination, depth));
    }

    /**
     * Loads a paginated, sorted subset of nodes that match multiple filter criteria.
     * <p>
     * This method retrieves a specific page of entities that satisfy the combined filter expression
     * and sorts them according to the specified order. Combines complex filtering, sorting,
     * and pagination for building sophisticated, navigable data views with multiple criteria.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filters filters = new Filters()
     *         .add(new Filter("status", ComparisonOperator.EQUALS, "active"));
     * SortOrder byName = new SortOrder().add("name");
     * Pagination firstPage = new Pagination(0, 20);
     * Collection<Person> page0 = executor.loadAll(Person.class, filters, byName, firstPage); // first 20 matches, by name
     *
     * Collection<Person> page1 = executor.loadAll(Person.class, filters, byName, new Pagination(1, 20));
     *
     * // A page beyond the matches -> empty collection
     * Collection<Person> none =
     *     executor.loadAll(Person.class, filters, byName, new Pagination(9999, 20)); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes; may be {@code null}
     *                or empty, in which case every node of {@code targetClass} is loaded
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @return paginated collection of filtered, sorted nodes, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filters, SortOrder, Pagination, int)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final SortOrder sortOrder, final Pagination pagination)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, filters, sortOrder, pagination));
    }

    /**
     * Loads a paginated, sorted subset of nodes that match multiple filter criteria with relationship depth.
     * <p>
     * This is the most comprehensive filtered loadAll method using Filters, providing complete control
     * over complex filtering, sorting, pagination, and relationship loading. Perfect for building
     * sophisticated, navigable data views with multiple filter criteria and full relationship information.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filters filters = new Filters()
     *         .add(new Filter("status", ComparisonOperator.EQUALS, "active"))
     *         .and(new Filter("age", ComparisonOperator.GREATER_THAN, 18));
     * SortOrder byAgeDesc = new SortOrder(SortOrder.Direction.DESC, "age");
     * Pagination page = new Pagination(0, 20);
     * Collection<Person> withRels = executor.loadAll(Person.class, filters, byAgeDesc, page, 1); // filtered/sorted/paged, with relationships
     * Collection<Person> flat = executor.loadAll(Person.class, filters, byAgeDesc, page, 0);     // filtered/sorted/paged, properties only
     *
     * // A page beyond the matches -> empty collection
     * Collection<Person> none =
     *     executor.loadAll(Person.class, filters, byAgeDesc, new Pagination(9999, 20), 1); // returns empty collection
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type to load
     * @param filters the multiple filter criteria to apply when loading nodes; may be {@code null}
     *                or empty, in which case every node of {@code targetClass} is loaded
     * @param sortOrder the sort order specification for results; may be {@code null} for no ordering
     * @param pagination pagination settings (page number and size, or an explicit offset); may be
     *                   {@code null} for no pagination
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite for node-entity classes only)
     * @return paginated collection of filtered, sorted nodes with relationships, may be empty
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; also thrown by Neo4j-OGM if {@code depth} is negative for a
     *         relationship-entity class
     * @throws RuntimeException if OGM cannot map the requested entities or identifiers, the database cannot be reached, or Neo4j rejects the
     *         load query
     * @see #loadAll(Class, Filters, SortOrder, Pagination)
     * @see org.neo4j.ogm.cypher.Filters
     * @see org.neo4j.ogm.cypher.query.SortOrder
     * @see org.neo4j.ogm.cypher.query.Pagination
     */
    public <T> Collection<T> loadAll(final Class<T> targetClass, final Filters filters, final SortOrder sortOrder, final Pagination pagination, final int depth)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.loadAll(targetClass, filters, sortOrder, pagination, depth));
    }

    /**
     * Persists an OGM-mapped entity, entity array, or entity {@link Iterable} to Neo4j using the
     * session's default save depth.
     * <p>
     * Delegates to {@link Session#save(Object)}, which by default traverses the object graph at
     * unlimited depth ({@code -1}), saving the supplied entity together with every reachable
     * related entity. New nodes are created and assigned a generated ID (written back onto the
     * entity); existing nodes (those with an ID) are updated in place. Use {@link #save(Object, int)}
     * to bound the traversal explicitly.
     * <p>
     * Each call runs on a pooled session whose mapping context has been cleared, so OGM does not know the
     * relationships an entity had when it was loaded by an earlier call: a relationship removed from such an
     * entity (e.g. {@code person.setCompany(null)}) is <i>not</i> deleted by this method. Load, modify and
     * save within one {@link #run(Consumer)}/{@link #call(Function)} session, or delete the relationship with Cypher.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create and save a new node
     * Person person = new Person("John Doe", 30);
     * executor.save(person);   // post-state: node persisted; person.getId() is now populated
     * System.out.println("Saved with ID: " + person.getId());
     *
     * // Update an existing node together with its (potentially deep) relationships
     * Person existing = executor.load(Person.class, 123L, 1);
     * existing.setAge(31);
     * executor.save(existing); // post-state: the matching node is updated in place
     * }</pre>
     *
     * @param object a mapped entity, an array of mapped entities, or an {@link Iterable} of mapped
     *               entities; must not be {@code null}. An element of an array or {@code Iterable} whose class is not
     *               mapped is not saved: it is skipped silently, or rejected with an {@link IllegalArgumentException}
     *               when event listeners are registered with the {@link SessionFactory}. A {@code null} element fails
     *               with a {@link NullPointerException}
     * @throws IllegalArgumentException if {@code object} is {@code null}; also thrown by Neo4j-OGM if {@code object} is
     *         neither an array nor an {@code Iterable} and is not an instance of a mapped entity class
     * @throws RuntimeException if OGM cannot map the supplied object, the database cannot be reached, or Neo4j rejects a write or constraint
     * @see #save(Object, int)
     */
    public void save(final Object object) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(object, cs.object);

        runWithSession(session -> session.save(object));
    }

    /**
     * Persists an OGM-mapped entity, entity array, or entity {@link Iterable} to Neo4j, traversing
     * related entities to the given depth.
     * <p>
     * Delegates to {@link Session#save(Object, int)}. {@code depth == 0} saves only the
     * node's scalar properties (no relationships are written); a positive integer recursively
     * saves related entities to that many hops; {@code -1} saves the entire reachable sub-graph.
     * As with {@link #save(Object)}, a relationship removed from an entity loaded by an earlier call is not
     * deleted, because the session's mapping context does not carry over between calls.
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
     * executor.save(alice, -1);    // entire reachable sub-graph (use with care)
     * }</pre>
     *
     * @param object a mapped entity, an array of mapped entities, or an {@link Iterable} of mapped
     *               entities; must not be {@code null}. An element of an array or {@code Iterable} whose class is not
     *               mapped is not saved: it is skipped silently, or rejected with an {@link IllegalArgumentException}
     *               when event listeners are registered with the {@link SessionFactory}. A {@code null} element fails
     *               with a {@link NullPointerException}
     * @param depth the depth of related entities to traverse and persist: {@code 0} for the node
     *              only, a positive integer for that many hops, or {@code -1} for unlimited
     * @throws IllegalArgumentException if {@code object} is {@code null}; also thrown by Neo4j-OGM if {@code object} is
     *         neither an array nor an {@code Iterable} and is not an instance of a mapped entity class
     * @throws RuntimeException if OGM cannot map the supplied object, the database cannot be reached, or Neo4j rejects a write or constraint
     * @see #save(Object)
     */
    public void save(final Object object, final int depth) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(object, cs.object);

        runWithSession(session -> session.save(object, depth));
    }

    /**
     * Deletes one or more OGM-managed entities from Neo4j. The argument may be a single entity, an
     * entity array, or an {@link Iterable}; deleting a node entity also deletes its incident
     * relationships.
     * <p>
     * Delegates to {@link Session#delete(Object)}. The entity must carry a populated graph ID
     * (i.e. it has previously been loaded or saved); otherwise OGM has nothing to delete. For an
     * entity class without a native graph-ID field (primary index only), the cleared pooled session
     * cannot resolve the graph ID, so OGM first reloads each entity by its primary-index value (and logs
     * a warning) before deleting it. The delete is irreversible.
     * <p>
     * The deletes of an array or {@code Iterable} are not atomic: Neo4j-OGM deletes each entity in a transaction of
     * its own, so when one of them fails, the entities before it stay deleted and those after it are not attempted.
     * To delete several entities all-or-nothing, call {@link Session#delete(Object)} inside an explicit transaction
     * through {@link #run(Consumer)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Person person = executor.load(Person.class, 123L);
     * if (person != null) {
     *     executor.delete(person);   // post-state: the node and its relationships are removed
     * }
     *
     * Collection<Person> inactive = executor.loadAll(Person.class,
     *     new Filter("status", ComparisonOperator.EQUALS, "inactive"));
     * executor.delete(inactive);   // every inactive Person node is removed (OGM sends one DELETE statement per entity)
     * }</pre>
     *
     * @param object a mapped entity, an array of mapped entities, or an {@link Iterable} of mapped
     *               entities to delete; must not be {@code null}
     * @throws IllegalArgumentException if {@code object} is {@code null}; also thrown by Neo4j-OGM if {@code object} (or one of its
     *         elements) is not an instance of a mapped entity class
     * @throws RuntimeException if OGM cannot resolve the objects to delete, the database cannot be reached, or Neo4j rejects the delete query
     * @see #deleteAll(Class)
     */
    public void delete(final Object object) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(object, cs.object);

        runWithSession(session -> session.delete(object));
    }

    /**
     * Deletes every entity of the supplied OGM-mapped class. For a node-entity class, incident
     * relationships are deleted as well; for a relationship-entity class, the mapped relationships
     * are deleted.
     * <p>
     * Delegates to {@link Session#deleteAll(Class)}. <strong>WARNING:</strong> this is a destructive
     * bulk operation that cannot be undone; verify the target class before invoking it.
     * If {@code targetClass} is not a mapped entity class (or no label can be determined for it),
     * Neo4j-OGM logs a warning and deletes nothing; no exception is thrown.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Remove every Person node (and its relationships) from the graph
     * executor.deleteAll(Person.class);
     * assert executor.count(Person.class) == 0; // post-state: no Person nodes remain
     *
     * // Typical teardown after a test run
     * executor.deleteAll(Company.class);
     * executor.deleteAll(Person.class);
     *
     * // Re-running on an already-empty type is harmless (still no-op deletes)
     * executor.deleteAll(Person.class); // nothing left to delete
     * }</pre>
     *
     * @param targetClass the OGM-mapped entity class to delete
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot resolve the objects to delete, the database cannot be reached, or Neo4j rejects the delete query
     * @see #delete(Object)
     */
    public void deleteAll(final Class<?> targetClass) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        runWithSession(session -> session.deleteAll(targetClass));

    }

    /**
     * Executes a Cypher query and returns the single result row mapped to {@code targetClass}, wrapped
     * in an {@link Optional}.
     * <p>
     * Delegates to {@link Session#queryForObject(Class, String, Map)}. The query must return
     * exactly zero or one row; if multiple rows are returned the underlying OGM session raises a
     * {@link RuntimeException}. The method name reflects this cardinality requirement: it does not
     * silently select the first row. Add a {@code LIMIT 1} clause or a uniqueness constraint to the
     * Cypher when the query could otherwise return multiple rows.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query for a single person by name
     * Map<String, Object> params = Map.of("name", "John Doe");
     * Optional<Person> person = executor.findOnly(Person.class,
     *     "MATCH (p:Person {name: $name}) RETURN p", params);
     *
     * // Query for single aggregation result
     * Optional<Long> count = executor.findOnly(Long.class,
     *     "MATCH (p:Person) WHERE p.age > $minAge RETURN count(p) as count",
     *     Map.of("minAge", 25));
     *
     * // Query with LIMIT 1 to guarantee a single row
     * Optional<Person> oldest = executor.findOnly(Person.class,
     *     "MATCH (p:Person) RETURN p ORDER BY p.age DESC LIMIT 1",
     *     Collections.emptyMap());
     * }</pre>
     *
     * @param <T> the result type
     * @param targetClass the target class &mdash; either a mapped OGM entity class or a basic value
     *                   type (e.g. {@code Long.class} for an aggregate result)
     * @param cypher the Cypher query string with {@code $name} parameter placeholders; must not be
     *               {@code null} or empty
     * @param parameters named parameters bound by the OGM session; may be empty but must not be
     *                   {@code null}
     * @return an {@link Optional} describing the single mapped result, or an empty {@code Optional} if
     *         the query returns no rows or its single row's value is {@code null} (for example {@code RETURN null})
     * @throws IllegalArgumentException if {@code targetClass}, {@code cypher}, or {@code parameters} is {@code null}
     * @throws RuntimeException if {@code cypher} is empty or {@code targetClass} is a void type (Neo4j-OGM rejects these
     *         with a plain {@code RuntimeException}); if the database cannot be reached or Neo4j rejects the query; if the
     *         query returns more than one row; or if the result cannot be mapped to {@code targetClass}
     * @see #stream(Class, String, Map)
     * @see #stream(String, Map)
     */
    public <T> Optional<T> findOnly(final Class<T> targetClass, final String cypher, final Map<String, ?> parameters)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(cypher, cs.cypher);
        N.checkArgNotNull(parameters, cs.parameters);

        if (logger.isDebugEnabled()) {
            logger.debug("Executing Cypher: {}", cypher);
        }

        return withSession(session -> Optional.ofNullable(session.queryForObject(targetClass, cypher, parameters)));
    }

    /**
     * Executes a Cypher query and returns each result row as a {@code Map<String, Object>} keyed
     * by the column alias declared in the {@code RETURN} clause.
     * <p>
     * The Cypher executes immediately and the result rows are fully fetched. The borrowed session
     * is cleared and returned to the pool before this method returns, and the returned stream lazily
     * iterates only those materialized rows.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String cypher = "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) " +
     *                 "WHERE c.name = $companyName " +
     *                 "RETURN p.name as personName, c.name as companyName";
     * Map<String, Object> params = Map.of("companyName", "Tech Corp");
     *
     * try (Stream<Map<String, Object>> rows = executor.stream(cypher, params)) {
     *     rows.forEach(row -> System.out.println(
     *             row.get("personName") + " works at " + row.get("companyName")));
     * }
     * }</pre>
     *
     * @param cypher the Cypher query string with {@code $name} parameter placeholders; must not be
     *               {@code null} or empty
     * @param parameters named parameters bound by the OGM session; may be empty but must not be
     *                   {@code null}
     * @return a {@link Stream} over the already-fetched result rows, each row a {@code Map} keyed by
     *         the {@code RETURN}-clause aliases; it does not retain the borrowed session
     * @throws IllegalArgumentException if {@code cypher} or {@code parameters} is {@code null}
     * @throws RuntimeException if {@code cypher} is empty (Neo4j-OGM rejects it with a plain {@code RuntimeException});
     *         if the database cannot be reached or Neo4j rejects the query; or if the result rows cannot be mapped
     * @see #stream(Class, String, Map)
     * @see #stream(String, Map, boolean)
     * @see #findOnly(Class, String, Map)
     */
    public Stream<Map<String, Object>> stream(final String cypher, final Map<String, ?> parameters) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(cypher, cs.cypher);
        N.checkArgNotNull(parameters, cs.parameters);

        if (logger.isDebugEnabled()) {
            logger.debug("Executing Cypher: {}", cypher);
        }

        return withSession(session -> Stream.of(session.query(cypher, parameters).iterator()));
    }

    /**
     * Executes a Cypher query with an explicit read-only hint and returns each row as a
     * {@code Map<String, Object>}.
     * <p>
     * The {@code readOnly} flag is forwarded to {@link Session#query(String, Map, boolean)} so the
     * Neo4j driver can route the request to a read replica in a clustered deployment or otherwise
     * optimise execution. Pass {@code false} for any query that mutates the graph; passing
     * {@code true} for a write query may fail at the driver/server depending on the deployment
     * (OGM logs a debug-level diagnostic and still submits the query in a read-only transaction).
     * <p>
     * The Cypher executes immediately and the result rows are fully fetched. The borrowed session
     * is cleared and returned to the pool before this method returns; the returned stream iterates
     * only the materialized rows.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Read-only query: eligible for read-replica routing in a cluster
     * Map<String, Object> params = Map.of("minAge", 25);
     * try (Stream<Map<String, Object>> rows =
     *         executor.stream("MATCH (p:Person) WHERE p.age > $minAge RETURN p.name AS name", params, true)) {
     *     rows.forEach(row -> System.out.println(row.get("name")));
     * }
     *
     * // Mutating query MUST pass readOnly = false
     * try (Stream<Map<String, Object>> created =
     *         executor.stream("CREATE (p:Person {name: $name}) RETURN id(p) AS id",
     *                 Map.of("name", "John"), false)) {
     *     created.forEach(row -> System.out.println("new id: " + row.get("id")));
     * }
     *
     * // The session has already been released; try-with-resources is still safe and conventional.
     * }</pre>
     *
     * @param cypher the Cypher query string with {@code $name} parameter placeholders; must not be
     *               {@code null} or empty
     * @param parameters named parameters bound by the OGM session; may be empty but must not be
     *                   {@code null}
     * @param readOnly {@code true} to mark the query as read-only (eligible for read-replica
     *                 routing); must be {@code false} for queries that write to the graph
     * @return a {@link Stream} over the already-fetched result rows; it does not retain the borrowed
     *         session
     * @throws IllegalArgumentException if {@code cypher} or {@code parameters} is {@code null}
     * @throws RuntimeException if {@code cypher} is empty (Neo4j-OGM rejects it with a plain {@code RuntimeException});
     *         if the database cannot be reached or Neo4j rejects the query (depending on the deployment, this includes a
     *         write query submitted with {@code readOnly == true}); or if the result rows cannot be mapped
     * @see #stream(String, Map)
     * @see #stream(Class, String, Map)
     */
    public Stream<Map<String, Object>> stream(final String cypher, final Map<String, ?> parameters, final boolean readOnly)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(cypher, cs.cypher);
        N.checkArgNotNull(parameters, cs.parameters);

        if (logger.isDebugEnabled()) {
            logger.debug("Executing Cypher: {}", cypher);
        }

        return withSession(session -> Stream.of(session.query(cypher, parameters, readOnly).iterator()));
    }

    /**
     * Executes a Cypher query and maps each row to an instance of {@code targetClass}.
     * <p>
     * Delegates to {@link Session#query(Class, String, Map)}. {@code targetClass} is typically an
     * OGM-mapped entity class for queries that return whole nodes, but may also be a basic value
     * type when the {@code RETURN} clause produces a single scalar per row.
     * <p>
     * The Cypher executes immediately and the result rows are fully fetched. The borrowed session
     * is cleared and returned to the pool before this method returns; the returned stream lazily
     * iterates only the materialized rows.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String cypher = "MATCH (p:Person) WHERE p.age > $minAge RETURN p";
     * Map<String, Object> params = Map.of("minAge", 25);
     *
     * try (Stream<Person> adults = executor.stream(Person.class, cypher, params)) {
     *     adults.filter(p -> p.getName().startsWith("A"))
     *           .forEach(p -> System.out.println(p.getName()));
     * }
     * }</pre>
     *
     * @param <T> the result row type
     * @param targetClass the target class &mdash; an OGM-mapped entity class or a basic value type
     * @param cypher the Cypher query string with {@code $name} parameter placeholders; must not be
     *               {@code null} or empty
     * @param parameters named parameters bound by the OGM session; may be empty but must not be
     *                   {@code null}
     * @return a {@link Stream} over the already-fetched rows mapped to {@code targetClass}; it does
     *         not retain the borrowed session
     * @throws IllegalArgumentException if {@code targetClass}, {@code cypher}, or {@code parameters} is {@code null}
     * @throws RuntimeException if {@code cypher} is empty or {@code targetClass} is a void type (Neo4j-OGM rejects these
     *         with a plain {@code RuntimeException}); if the database cannot be reached or Neo4j rejects the query; or if
     *         a result row cannot be mapped to {@code targetClass} (for example, a scalar query returns more than one column)
     * @see #findOnly(Class, String, Map)
     * @see #stream(String, Map)
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final String cypher, final Map<String, ?> parameters)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(cypher, cs.cypher);
        N.checkArgNotNull(parameters, cs.parameters);

        if (logger.isDebugEnabled()) {
            logger.debug("Executing Cypher: {}", cypher);
        }

        return withSession(session -> Stream.of(session.query(targetClass, cypher, parameters).iterator()));
    }

    /**
     * Counts the number of mapped entities of the specified type that match the given filter
     * expression.
     * <p>
     * This method counts node or relationship entities without loading them into memory.
     * Useful for pagination calculations and data analytics.
     * <p>
     * Neo4j-OGM names each filter's Cypher parameter after the filter's property and its index
     * ({@code <property>_<index>}), and only {@link Filters#add(Filter)} assigns that index: a filter never added to a
     * {@code Filters} object has the index {@code 0}. A {@code Filters} argument (as in the second example below) is
     * passed to Neo4j-OGM as it is, and Neo4j-OGM resolves its filters in place, as it does every filter it is given
     * (owner type, property name, converter and relationship details; for a relationship-entity count it also shortens
     * a filter's nested path). The filters of any other non-empty iterable, such as a plain {@code List}, are sent as
     * copies in a new {@code Filters} object, each copy indexed by its position: two filters on the same property get
     * distinct parameters ({@code age > $age_0 AND age < $age_1}), and the caller's {@code Filter} objects are not
     * modified (neither re-indexed nor resolved). Such an iterable is passed on as it is &mdash; so two of its filters on
     * the same property share one parameter, the last value wins and the count is silently wrong &mdash; if it contains a
     * {@code null} element (Neo4j-OGM then throws a {@code NullPointerException}), an instance of a {@code Filter}
     * subclass, or a filter whose nested path holds an instance of a {@code Filter.NestedPathSegment} subclass. Every
     * iterable is passed on as it is (with a warning logged once) if the Neo4j-OGM {@code Filter} and
     * {@code Filter.NestedPathSegment} classes on the classpath do not declare exactly the fields of Neo4j-OGM 5.0.10, or
     * if the filter's private comparison function, which the copy reads reflectively, cannot be accessed.
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
     * filters.and(new Filter("age", ComparisonOperator.GREATER_THAN, 25));
     * long count = executor.count(Person.class, filters);
     *
     * // Filters on the same property in a plain list: each copy is sent with a parameter of its own
     * Filter minAge = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
     * Filter maxAge = new Filter("age", ComparisonOperator.LESS_THAN, 65);
     * maxAge.setBooleanOperator(BooleanOperator.AND);
     * long workingAge = executor.count(Person.class, Arrays.asList(minAge, maxAge));   // age_0 = 18, age_1 = 65
     *
     * // Count for pagination
     * Filter departmentFilter = new Filter("department", ComparisonOperator.EQUALS, "IT");
     * long totalItems = executor.count(Person.class, Arrays.asList(departmentFilter));
     * int pageSize = 10;
     * int totalPages = (int) Math.ceil((double) totalItems / pageSize);
     * }</pre>
     *
     * @param targetClass the OGM-mapped class whose entities are counted
     * @param filters OGM {@link Filter}s whose {@link org.neo4j.ogm.cypher.BooleanOperator}s define
     *                how each filter is joined to the preceding one; pass an empty iterable to
     *                count all entities of {@code targetClass}. A {@link Filters} object is used as it is,
     *                the filters of any other iterable are copied as described above. Unlike the
     *                {@link Filters} overloads of {@code loadAll}, a {@code null} iterable is <i>not</i>
     *                accepted here
     * @return the number of entities of {@code targetClass} that satisfy the supplied filter
     *         expression
     * @throws IllegalArgumentException if {@code targetClass} or {@code filters} is {@code null}
     * @throws RuntimeException if {@code targetClass} is not a mapped entity class (Neo4j-OGM rejects it with a plain
     *         {@code RuntimeException}, whereas {@link #count(Class)} returns {@code 0} for it), if a filter after the first
     *         has no {@link org.neo4j.ogm.cypher.BooleanOperator}, if OGM cannot map the filters, the database cannot be
     *         reached, or Neo4j rejects the count query
     * @see #count(Class)
     * @see org.neo4j.ogm.cypher.Filter
     */
    public long count(final Class<?> targetClass, final Iterable<Filter> filters) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgNotNull(filters, cs.filters);

        final Iterable<Filter> indexedFilters = FilterCopier.indexedCopiesOf(filters);

        return withSession(session -> session.count(targetClass, indexedFilters));
    }

    /**
     * Counts all node or relationship entities mapped by the specified type.
     * <p>
     * This method counts the mapped entities without loading them into memory. For an unmapped
     * class, Neo4j-OGM returns {@code 0}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Count all persons in the database
     * long totalPersons = executor.count(Person.class);
     * System.out.println("Total persons: " + totalPersons);
     *
     * // Count all companies
     * long totalCompanies = executor.count(Company.class);
     *
     * // Use for database statistics
     * Map<String, Long> stats = new HashMap<>();
     * stats.put("persons", executor.count(Person.class));
     * stats.put("companies", executor.count(Company.class));
     * stats.put("projects", executor.count(Project.class));
     *
     * // Initialize a type when it has no data
     * if (executor.count(Person.class) == 0) {
     *     executor.save(new Person("First user", 30));
     * }
     * }</pre>
     *
     * @param targetClass the OGM-mapped class whose entities are counted
     * @return the total number of entities mapped by {@code targetClass}, or {@code 0} if the class
     *         is not mapped
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}
     * @throws RuntimeException if OGM cannot map the target class, the database cannot be reached, or Neo4j rejects the count query
     * @see #count(Class, Iterable)
     */
    public long count(final Class<?> targetClass) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return withSession(session -> session.countEntitiesOfType(targetClass));
    }

    /**
     * Returns the native Neo4j graph ID for {@code possibleEntity}, if it is an OGM-managed
     * entity.
     * <p>
     * Delegates to {@link Session#resolveGraphIdFor(Object)}. Because the executor uses a cleared
     * pooled session for the call, this only returns a non-null ID when the entity carries an ID
     * field that OGM has already populated (either because it was loaded from the database or
     * because a previous {@code save} assigned it). Transient objects that have never been saved
     * or that are not mapped at all return {@code null}. So does every instance of an entity class without a
     * native graph-ID field (one identified only by a primary index, such as an {@code @Id String} property),
     * even after it has been saved or loaded, because Neo4j-OGM resolves the ID only from that field.
     * <p>
     * Side effect: for a mapped entity whose native-ID field is still {@code null}, Neo4j-OGM writes a
     * temporary negative placeholder ID into that field (as {@code save} does for new entities) and this
     * method returns {@code null}; a later {@code save} still creates a new node and replaces the placeholder.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Person person = executor.load(Person.class, 123L);
     * Long graphId = executor.getGraphId(person); // native graph ID; may differ from a primary-index lookup value
     *
     * Person newPerson = new Person("John Doe");
     * executor.getGraphId(newPerson);                // null - not yet saved (newPerson.getId() is now a negative placeholder)
     * executor.save(newPerson);                      // post-state: newPerson now has a generated id
     * Long savedId = executor.getGraphId(newPerson); // assigned by Neo4j
     * }</pre>
     *
     * @param possibleEntity an object that may or may not be an OGM-mapped entity; may be
     *                       {@code null}, in which case {@code null} is returned
     * @return the native Neo4j graph ID for the entity, or {@code null} if {@code possibleEntity}
     *         is not a mapped/managed entity with an assigned ID, or its class has no native graph-ID field
     * @throws RuntimeException if OGM cannot read or convert the mapped graph identifier of {@code possibleEntity}
     * @see #load(Class, Serializable)
     */
    public Long getGraphId(final Object possibleEntity) throws RuntimeException {
        return withSession(session -> session.resolveGraphIdFor(possibleEntity));
    }

    private Session getSession() {
        // Non-blocking: opening an OGM session is a cheap in-memory operation, so there is no point
        // waiting on an empty pool (nothing fills it except returns).
        Session session = sessionPool.poll();

        if (session == null) {
            session = openSession();
        }

        return session;
    }

    /**
     * Borrows a pooled session, applies {@code body} to it and returns the session to the pool with
     * try-with-resources semantics: if {@code body} throws, its exception always wins and whatever the
     * session return throws (an {@link Error} included) is attached to it as a suppressed exception; if
     * {@code body} completes normally, a failure of the session return propagates as it is.
     *
     * @param <T> the type returned by {@code body}
     * @param body the work to do with the borrowed session
     * @return the value produced by {@code body}
     */
    private <T> T withSession(final Function<? super Session, ? extends T> body) {
        final Session session = getSession();
        Throwable primary = null;

        try {
            return body.apply(session);
        } catch (final Throwable e) {
            primary = e;

            throw e;
        } finally {
            closeSession(session, primary);
        }
    }

    /**
     * {@link #withSession(Function)} for a {@code body} that produces no value.
     */
    private void runWithSession(final Consumer<? super Session> body) {
        withSession(session -> {
            body.accept(session);

            return null;
        });
    }

    /**
     * Returns a borrowed session to the pool (see {@link #closeSession(Session)}) on behalf of an operation
     * that threw {@code primary}, or that completed normally when {@code primary} is {@code null}.
     */
    private void closeSession(final Session session, final Throwable primary) {
        if (primary == null) {
            closeSession(session);

            return;
        }

        try {
            closeSession(session);
        } catch (final Throwable cleanupFailure) { // NOSONAR
            // WHY: this runs in the finally block of every borrowing operation, where Java's finally semantics would let
            // an Error escaping the session return (releaseDiscardedSession lets Errors through on purpose) replace the
            // operation's own exception; discardSession can only attach the rollback failure to that Error, never the
            // exception the operation threw. Mirror try-with-resources instead: the operation's exception always wins and
            // the clean-up failure survives as its suppressed exception.
            addSuppressedIfDistinct(primary, cleanupFailure);
        }
    }

    @SuppressWarnings("resource")
    private void closeSession(final Session session) {
        if (session != null) {
            // A run/call action may have left a transaction open (e.g. it threw between
            // beginTransaction() and tx.close()). Never pool a session with a live transaction:
            // roll it back, and discard the session entirely if that fails. On a nested
            // ("extended") OGM transaction - beginTransaction() called again on the same session -
            // each rollback()/close() pair only marks ROLLBACK_PENDING and pops one nesting level;
            // only the root-level pair actually rolls back, hence the loop.
            while (true) {
                final Transaction tx;

                try {
                    tx = session.getTransaction();
                } catch (final RuntimeException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to inspect a returned session's transaction; discarding the session", e);
                    }

                    return;
                }

                if (tx == null) {
                    break;
                }

                try {
                    tx.rollback();
                } catch (final RuntimeException e) {
                    try {
                        // OGM close() commits COMMIT_PENDING; cleanup must never turn a failed rollback into a commit.
                        if (tx.status() != Transaction.Status.COMMIT_PENDING) {
                            tx.close();
                        }
                    } catch (final RuntimeException closeFailure) {
                        if (closeFailure != e) {
                            e.addSuppressed(closeFailure);
                        }
                    }

                    discardSession(session, tx, e);

                    return;
                }

                try {
                    tx.close();
                } catch (final RuntimeException e) {
                    discardSession(session, tx, e);

                    return;
                }
            }

            try {
                session.clear();
            } catch (final RuntimeException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to clear a returned session; discarding the session", e);
                }

                return;
            }

            sessionPool.offer(session); //NOSONAR
        }
    }

    /**
     * Discards a returned session whose leftover transaction could not be rolled back (or closed):
     * releases it through {@link #releaseDiscardedSession} and logs {@code failure} with every secondary
     * failure attached, whatever the release did. An {@code Error} thrown by the release leaves this method
     * with {@code failure} attached as a suppressed exception; {@link #closeSession(Session, Throwable)} then
     * lets it propagate only when the operation that borrowed the session completed normally, otherwise it
     * becomes a suppressed exception of the operation's own exception.
     */
    private static void discardSession(final Session session, final Transaction tx, final RuntimeException failure) {
        try {
            releaseDiscardedSession(session, tx, failure);
        } catch (final Error releaseError) {
            // WHY: releaseDiscardedSession lets an Error propagate on purpose, but the Error must not erase the
            // rollback failure it interrupted: attach it as suppressed (it is the reason the release ran at all).
            addSuppressedIfDistinct(releaseError, failure);

            throw releaseError;
        } finally {
            // WHY: in finally, not after the release, so the rollback failure is still logged when the release
            // throws an Error; otherwise it would only survive as a suppressed exception of that Error.
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to roll back a transaction left open on a returned session; discarding the session", failure);
            }
        }
    }

    /**
     * Best-effort release of a session that is being discarded because its leftover transaction
     * could not be rolled back. Never throws a {@code RuntimeException}: secondary runtime failures are
     * attached to {@code failure} (which the caller logs) so they cannot replace the operation's own
     * exception. An {@code Error} is deliberately not caught: it propagates out of the clean-up, past the
     * rest of this release (a failed detach is then not followed by {@code clear()}), and the caller
     * ({@link #discardSession}) attaches {@code failure} to it as a suppressed exception and still logs
     * {@code failure}. Such an {@code Error} reaches the executor's caller only when the operation that
     * borrowed the session completed normally; if the operation itself threw, the {@code Error} is attached
     * to that exception as a suppressed exception instead (see {@link #closeSession(Session, Throwable)}).
     * Nothing in here commits: the detach runs with a no-op callback.
     */
    private static void releaseDiscardedSession(final Session session, final Transaction tx, final RuntimeException failure) {
        // WHY: OGM's DefaultTransactionManager removes a transaction from its per-manager ThreadLocal
        // only after the native rollback/commit succeeded, so after a failed rollback the stale
        // transaction stays registered. Its value chain (tx -> manager -> ThreadLocal key) keeps the
        // weakly-referenced key alive, so the entry is never expunged and would pin this discarded
        // session - mapping context and native transaction included - for the thread's lifetime.
        // 1) Detach it through the public TransactionManager.close(tx, callback) contract: with a
        //    no-op callback nothing is rolled back or committed (no COMMIT_PENDING commit), the
        //    transaction is just unregistered from the current thread. getTransactionManager() is
        //    documented by OGM as safe to use after down-casting to Neo4jSession.
        if (session instanceof final Neo4jSession neo4jSession) {
            try {
                final TransactionManager txManager = neo4jSession.getTransactionManager();

                // Skip if a close() retry already deregistered it (close() would throw "not current").
                if (txManager != null && txManager.getCurrentTransaction() == tx) {
                    txManager.close(tx, listener -> {
                        // intentionally empty: detach only, do not touch the native transaction
                    });
                }
            } catch (final RuntimeException detachFailure) {
                addSuppressedIfDistinct(failure, detachFailure);
            }
        }

        // 2) Release the mapping context even when the session itself stays reachable (e.g. a
        //    non-Neo4jSession implementation, or a failed detach).
        try {
            session.clear();
        } catch (final RuntimeException clearFailure) {
            addSuppressedIfDistinct(failure, clearFailure);
        }
    }

    private static void addSuppressedIfDistinct(final Throwable failure, final Throwable other) {
        if (other != failure) {
            failure.addSuppressed(other);
        }
    }

    /**
     * Copies the filters of a plain iterable into a new {@link Filters} object for {@link #count(Class, Iterable)}.
     */
    private static final class FilterCopier {
        // WHY: Neo4j-OGM names a filter's Cypher parameter <property>_<index> and only Filters.add assigns the index, so all
        // filters of a plain list keep index 0 and two of them on the same property share one parameter (the last value
        // wins and the count is silently wrong). new Filters(filters) would fix the names but re-index - and so mutate -
        // the caller's own Filter objects, which can make another Filters object they belong to collide; faithful copies are
        // indexed instead. Filter (5.0.10) offers no copy API and no getter for its comparison function, so the function is
        // read reflectively, and only when Filter and NestedPathSegment declare exactly the fields of 5.0.10: a copy made
        // under another layout could silently drop state that a different OGM version renders.
        private static final Set<String> FILTER_FIELDS = Set.of("index", "propertyName", "booleanOperator", "negated", "ownerEntityType",
                "nestedEntityTypeLabel", "nestedPropertyName", "nestedPropertyType", "relationshipType", "relationshipDirection", "propertyConverter",
                "nestedRelationshipEntity", "function", "nestedPath");

        private static final Set<String> SEGMENT_FIELDS = Set.of("propertyName", "propertyType", "relationshipType", "relationshipDirection",
                "nestedEntityTypeLabel", "nestedRelationshipEntity");

        /** {@code Filter.function}, or {@code null} when filters cannot be copied faithfully (filters are then passed on as they are). */
        private static final Field FUNCTION_FIELD = functionField();

        private FilterCopier() {
        }

        /**
         * Returns {@code filters} itself when it is a {@link Filters} object, empty, or holds a filter that cannot be
         * copied faithfully; otherwise a new {@code Filters} object of copies of its filters, indexed by position.
         */
        static Iterable<Filter> indexedCopiesOf(final Iterable<Filter> filters) {
            if (filters instanceof Filters || FUNCTION_FIELD == null) {
                return filters;
            }

            final Filters copies = new Filters();

            for (final Filter filter : filters) {
                final Filter copy = copyOf(filter);

                if (copy == null) {
                    return filters;
                }

                copies.add(copy);
            }

            return copies.isEmpty() ? filters : copies;
        }

        /**
         * Copies every field of {@code filter} except its index, or returns {@code null} for a {@code null} filter (Neo4j-OGM's
         * {@code NullPointerException} is kept) and for a {@code Filter} or {@code NestedPathSegment} subclass, whose
         * additional state or overridden rendering a plain copy would lose.
         */
        private static Filter copyOf(final Filter filter) {
            if (filter == null || filter.getClass() != Filter.class) {
                return null;
            }

            final List<Filter.NestedPathSegment> nestedPath = filter.getNestedPath();
            Filter.NestedPathSegment[] nestedPathCopy = null;

            if (nestedPath != null) {
                nestedPathCopy = new Filter.NestedPathSegment[nestedPath.size()];

                for (int i = 0; i < nestedPathCopy.length; i++) {
                    final Filter.NestedPathSegment segment = nestedPath.get(i);

                    if (segment == null || segment.getClass() != Filter.NestedPathSegment.class) {
                        return null;
                    }

                    // WHY: OGM resolves the segments in place (relationship type/direction, entity label), so they are copied too
                    final Filter.NestedPathSegment segmentCopy = new Filter.NestedPathSegment(segment.getPropertyName(), segment.getPropertyType());
                    segmentCopy.setRelationshipType(segment.getRelationshipType());
                    segmentCopy.setRelationshipDirection(segment.getRelationshipDirection());
                    segmentCopy.setNestedEntityTypeLabel(segment.getNestedEntityTypeLabel());
                    segmentCopy.setNestedRelationshipEntity(segment.isNestedRelationshipEntity());
                    nestedPathCopy[i] = segmentCopy;
                }
            }

            final FilterFunction<?> function;

            try {
                function = (FilterFunction<?>) FUNCTION_FIELD.get(filter);
            } catch (final IllegalAccessException e) {
                return null;
            }

            // WHY: the function is shared, not copied: OGM's functions are immutable (ignoreCase() replaces the filter's
            // function) and are handed the rendering filter's own parameter namer, so the copy renders with its own index
            final Filter copy = new Filter(filter.getPropertyName(), function);
            copy.setBooleanOperator(filter.getBooleanOperator());
            copy.setNegated(filter.isNegated());
            copy.setOwnerEntityType(filter.getOwnerEntityType());
            copy.setNestedEntityTypeLabel(filter.getNestedEntityTypeLabel());
            copy.setNestedPropertyName(filter.getNestedPropertyName());
            copy.setNestedPropertyType(filter.getNestedPropertyType());
            copy.setRelationshipType(filter.getRelationshipType());
            copy.setRelationshipDirection(filter.getRelationshipDirection());
            copy.setPropertyConverter(filter.getPropertyConverter());
            copy.setNestedRelationshipEntity(filter.isNestedRelationshipEntity());

            if (nestedPathCopy != null) {
                copy.setNestedPath(nestedPathCopy);
            }

            return copy;
        }

        private static Field functionField() {
            try {
                if (!FILTER_FIELDS.equals(instanceFieldNames(Filter.class)) || !SEGMENT_FIELDS.equals(instanceFieldNames(Filter.NestedPathSegment.class))) {
                    logger.warn("Neo4j-OGM's Filter or Filter.NestedPathSegment class does not have the fields of Neo4j-OGM 5.0.10;"
                            + " count(Class, Iterable<Filter>) passes the filters of a plain iterable to Neo4j-OGM as they are, so filters on the"
                            + " same property in it share one Cypher parameter");

                    return null;
                }

                final Field field = Filter.class.getDeclaredField("function");
                field.setAccessible(true);

                return field;
            } catch (final NoSuchFieldException | RuntimeException e) {
                logger.warn("Cannot read the comparison function of a Neo4j-OGM Filter; count(Class, Iterable<Filter>) passes the filters of a plain"
                        + " iterable to Neo4j-OGM as they are, so filters on the same property in it share one Cypher parameter", e);

                return null;
            }
        }

        private static Set<String> instanceFieldNames(final Class<?> cls) {
            final Set<String> names = new HashSet<>();

            for (final Field field : cls.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic()) {
                    names.add(field.getName());
                }
            }

            return names;
        }
    }

}
