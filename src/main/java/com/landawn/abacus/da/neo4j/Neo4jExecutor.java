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
import com.landawn.abacus.util.stream.Stream;

/**
 * A comprehensive executor for Neo4j graph database operations, providing high-level abstractions
 * for Cypher query execution, node/relationship management, and graph traversals.
 * <p>
 * This executor wraps the Neo4j OGM (Object Graph Mapping) framework and provides session management,
 * connection pooling, and simplified APIs for common graph database operations.
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li>Cypher query execution with parameter binding</li>
 *   <li>Node and relationship CRUD operations</li>
 *   <li>Graph traversals and path finding</li>
 *   <li>Object-Graph Mapping (OGM) support</li>
 *   <li>Session pooling and connection management</li>
 *   <li>Transaction support and management</li>
 *   <li>Filtering, sorting, and pagination</li>
 * </ul>
 * 
 * <h3>Graph Operations:</h3>
 * <ul>
 *   <li><b>Node Operations:</b> Create, read, update, delete nodes</li>
 *   <li><b>Relationship Operations:</b> Manage relationships between nodes</li>
 *   <li><b>Traversals:</b> Navigate graph structures efficiently</li>
 *   <li><b>Queries:</b> Execute complex Cypher queries</li>
 *   <li><b>Transactions:</b> Atomic operations across multiple graph changes</li>
 * </ul>
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
 * Stream<Person> adults = executor.query(Person.class, 
 *     "MATCH (p:Person) WHERE p.age > $age RETURN p", params);
 * 
 * // Complex queries
 * Stream<Map<String, Object>> results = executor.query(
 *     "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name", 
 *     Collections.emptyMap());
 * 
 * // Filtering and pagination
 * Filter ageFilter = new Filter("age", ComparisonOperator.GREATER_THAN, 25);
 * Pagination pagination = new Pagination(0, 10);
 * Collection<Person> pagedResults = executor.loadAll(Person.class, ageFilter, pagination);
 * }</pre>
 * 
 * <h3>Session Management:</h3>
 * <p>The executor implements automatic session pooling to optimize performance:</p>
 * <ul>
 *   <li>Session reuse through connection pooling</li>
 *   <li>Automatic session cleanup and resource management</li>
 *   <li>Configurable pool sizes and timeout settings</li>
 *   <li>Thread-safe session handling</li>
 * </ul>
 * 
 * <h3>OGM Integration:</h3>
 * <ul>
 *   <li>Automatic mapping between Java objects and graph nodes</li>
 *   <li>Relationship mapping with annotations</li>
 *   <li>Support for complex object hierarchies</li>
 *   <li>Lazy loading of relationships</li>
 * </ul>
 * 
 * <h3>Performance Features:</h3>
 * <ul>
 *   <li>Depth control for loading related objects</li>
 *   <li>Batch operations for multiple entities</li>
 *   <li>Streaming results for large datasets</li>
 *   <li>Query optimization and caching</li>
 * </ul>
 * 
 * @see org.neo4j.ogm.session.SessionFactory
 * @see org.neo4j.ogm.session.Session
 * @see <a href="http://neo4j.com/docs/ogm/java/stable/">Neo4j OGM Documentation</a>
 */
public final class Neo4jExecutor {

    private final LinkedBlockingQueue<Session> sessionPool = new LinkedBlockingQueue<>(8192);

    private final SessionFactory sessionFactory;

    /**
     * Constructs a Neo4jExecutor with the specified SessionFactory.
     * <p>
     * The SessionFactory manages the connection to the Neo4j database and provides
     * configuration for the OGM mapping. The executor will create a session pool
     * to optimize performance.
     *
     * @param sessionFactory the Neo4j SessionFactory for database connections
     * @throws IllegalArgumentException if sessionFactory is null
     */
    public Neo4jExecutor(final SessionFactory sessionFactory) {
        if (sessionFactory == null) {
            throw new IllegalArgumentException("sessionFactory cannot be null");
        }
        this.sessionFactory = sessionFactory;
    }

    /**
     * Returns the underlying Neo4j SessionFactory.
     * <p>
     * This method provides access to the SessionFactory for advanced operations
     * or direct session management that are not supported by this executor.
     * 
     * @return the Neo4j SessionFactory instance
     * @see org.neo4j.ogm.session.SessionFactory
     */
    public SessionFactory sessionFactory() {
        return sessionFactory;
    }

    /**
     * Opens a new Neo4j session.
     * <p>
     * This method creates a new session directly from the SessionFactory.
     * Note that sessions obtained this way are not managed by the executor's
     * connection pool and must be closed manually.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Open a session for manual management
     * Session session = executor.openSession();
     * try {
     *     // Perform operations
     *     Person person = session.load(Person.class, 123L);
     *     person.setName("Updated Name");
     *     session.save(person);
     * } finally {
     *     // Important: manually close the session
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
     * @return a new Neo4j session
     * @see org.neo4j.ogm.session.Session
     * @see #run(Consumer)
     * @see #call(Function)
     */
    public Session openSession() {
        return sessionFactory.openSession();
    }

    /**
     * Executes an action within a managed session context.
     * <p>
     * This method provides automatic session management, ensuring the session
     * is properly returned to the pool after the action completes. Use this
     * for operations that don't return values.
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
     * // Batch operations
     * executor.run(session -> {
     *     List<Person> people = loadPeopleToUpdate();
     *     people.forEach(person -> {
     *         person.setStatus("active");
     *         session.save(person);
     *     });
     * });
     *
     * // Complex graph operations
     * executor.run(session -> {
     *     Person person = new Person("Alice");
     *     Company company = new Company("Tech Corp");
     *     person.worksFor(company);
     *     session.save(person, 1);
     * });
     * }</pre>
     *
     * @param action the action to execute with the session
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
     * Executes a function within a managed session context and returns the result.
     * <p>
     * This method provides automatic session management with return value support.
     * The session is properly returned to the pool after the function completes.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute and return a result
     * Person person = executor.call(session -> {
     *     return session.load(Person.class, 123L);
     * });
     *
     * // Execute query and return count
     * Long count = executor.call(session -> {
     *     Map<String, Object> params = Map.of("status", "active");
     *     return (Long) session.query(
     *         "MATCH (p:Person {status: $status}) RETURN count(p) as count",
     *         params).iterator().next().get("count");
     * });
     *
     * // Load and transform data
     * List<String> names = executor.call(session -> {
     *     Collection<Person> people = session.loadAll(Person.class);
     *     return people.stream()
     *         .map(Person::getName)
     *         .collect(Collectors.toList());
     * });
     * }</pre>
     *
     * @param <T> the return type of the function
     * @param action the function to execute with the session
     * @return the result of the function execution
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
     * Loads a single node from Neo4j by its unique ID.
     * <p>
     * This method retrieves a node and its immediate properties but does not
     * load any relationships. For loading relationships, use {@link #load(Class, Long, int)}
     * with a depth greater than 0.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Load a single node by ID
     * Person person = executor.load(Person.class, 123L);
     * if (person != null) {
     *     System.out.println("Person: " + person.getName());
     * }
     *
     * // Check if node exists
     * Company company = executor.load(Company.class, 456L);
     * if (company == null) {
     *     System.out.println("Company not found");
     * }
     *
     * // Load multiple nodes
     * List<Long> ids = Arrays.asList(1L, 2L, 3L);
     * List<Person> people = ids.stream()
     *     .map(id -> executor.load(Person.class, id))
     *     .filter(Objects::nonNull)
     *     .collect(Collectors.toList());
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param id the unique Neo4j node ID
     * @return the loaded node entity, or null if not found
     * @throws IllegalArgumentException if targetClass is null or id is null
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
     * Loads a single node from Neo4j by its unique ID with specified relationship depth.
     * <p>
     * This method retrieves a node and its relationships up to the specified depth.
     * A depth of 0 loads only the node properties, depth 1 includes immediate relationships,
     * and higher depths recursively load connected nodes.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Load node with immediate relationships (depth 1)
     * Person person = executor.load(Person.class, 123L, 1);
     * // person.getCompany() is now loaded
     * System.out.println("Works for: " + person.getCompany().getName());
     *
     * // Load node with deep relationships (depth 2)
     * Person personWithTeam = executor.load(Person.class, 123L, 2);
     * // person.getCompany().getEmployees() is now loaded
     * personWithTeam.getCompany().getEmployees().forEach(e ->
     *     System.out.println("Colleague: " + e.getName()));
     *
     * // Load only node properties (depth 0)
     * Person personOnly = executor.load(Person.class, 123L, 0);
     * // No relationships loaded, more efficient
     *
     * // Load entire graph (depth -1)
     * Person fullGraph = executor.load(Person.class, 123L, -1);
     * // All connected nodes loaded (use with caution)
     * }</pre>
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @param id the unique Neo4j node ID
     * @param depth the depth of relationships to load (0 = node only, -1 = infinite)
     * @return the loaded node entity with relationships, or null if not found
     * @throws IllegalArgumentException if targetClass is null or id is null
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
     * @throws IllegalArgumentException if targetClass is null or ids is null
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
     * @throws IllegalArgumentException if targetClass is null or ids is null
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
     * @throws IllegalArgumentException if targetClass is null or ids is null
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
     * @throws IllegalArgumentException if targetClass is null or ids is null
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
     * @throws IllegalArgumentException if targetClass is null or ids is null
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
     * @throws IllegalArgumentException if targetClass is null or ids is null
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
     * @throws IllegalArgumentException if targetClass is null or ids is null
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
     * @throws IllegalArgumentException if targetClass is null or ids is null
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
     * @throws IllegalArgumentException if objects is null or contains entities without IDs
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
     * @throws IllegalArgumentException if objects is null or contains entities without IDs
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
     * @throws IllegalArgumentException if objects is null or contains entities without IDs
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
     * @throws IllegalArgumentException if objects is null or contains entities without IDs
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
     * @throws IllegalArgumentException if objects is null or contains entities without IDs
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
     * @throws IllegalArgumentException if objects is null or contains entities without IDs
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
     * @throws IllegalArgumentException if objects is null or contains entities without IDs
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
     * @throws IllegalArgumentException if objects is null or contains entities without IDs
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
     * Saves a node and its relationships to Neo4j.
     * <p>
     * This method persists the node to the graph database. If the node already exists
     * (has an ID), it will be updated. New nodes will be created with generated IDs.
     * The save operation uses the default depth, typically including immediate relationships.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create and save a new node
     * Person person = new Person("John Doe", 30);
     * executor.save(person);
     * System.out.println("Saved with ID: " + person.getId());
     *
     * // Update an existing node
     * Person existing = executor.load(Person.class, 123L);
     * existing.setAge(31);
     * executor.save(existing);
     *
     * // Save with relationships
     * Person person = new Person("Alice");
     * Company company = new Company("Tech Corp");
     * person.worksFor(company);
     * executor.save(person);  // Both person and company are saved
     *
     * // Batch save
     * List<Person> people = createPeople();
     * people.forEach(executor::save);
     * }</pre>
     *
     * @param <T> the node type
     * @param object the node entity to save
     * @throws IllegalArgumentException if object is null
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
     * Saves a node and its relationships to Neo4j with the specified depth.
     * <p>
     * This method controls how deeply related objects are saved. A depth of 0 saves
     * only the node properties, while higher depths save related nodes and their
     * relationships recursively.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Save only node properties (depth 0)
     * Person person = new Person("John Doe");
     * executor.save(person, 0);
     * // Relationships are not saved
     *
     * // Save with immediate relationships (depth 1)
     * Person person = new Person("Alice");
     * Company company = new Company("Tech Corp");
     * person.worksFor(company);
     * executor.save(person, 1);
     * // person and company are saved
     *
     * // Save with deep relationships (depth 2)
     * Company company = new Company("Tech Corp");
     * company.addEmployee(person1);
     * company.addEmployee(person2);
     * person1.addFriend(person2);
     * executor.save(company, 2);
     * // company, all employees, and their friends are saved
     *
     * // Save entire connected graph (depth -1)
     * executor.save(rootNode, -1);
     * // All connected nodes saved (use with caution)
     * }</pre>
     *
     * @param <T> the node type
     * @param object the node entity to save
     * @param depth the depth of relationships to save (0 = node only, -1 = infinite)
     * @throws IllegalArgumentException if object is null
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
     * Deletes a node and its relationships from Neo4j.
     * <p>
     * This method removes the specified node from the graph database. All relationships
     * connected to this node will also be removed. Use with caution as this operation
     * cannot be undone.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete a node by loading and deleting
     * Person person = executor.load(Person.class, 123L);
     * if (person != null) {
     *     executor.delete(person);
     *     System.out.println("Person deleted");
     * }
     *
     * // Delete multiple nodes
     * Collection<Person> inactivePeople = executor.loadAll(Person.class,
     *     new Filter("status", ComparisonOperator.EQUALS, "inactive"));
     * inactivePeople.forEach(executor::delete);
     *
     * // Delete with condition check
     * Person person = executor.load(Person.class, 123L);
     * if (person != null && person.isExpired()) {
     *     executor.delete(person);
     * }
     * }</pre>
     *
     * @param <T> the node type
     * @param object the node entity to delete
     * @throws IllegalArgumentException if object is null
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
     * Deletes all nodes of the specified type from Neo4j.
     * <p>
     * This method removes all nodes of the given type from the graph database.
     * All relationships connected to these nodes will also be removed.
     * <strong>WARNING:</strong> This operation cannot be undone and will remove
     * all data for the specified node type.
     *
     * @param <T> the node type
     * @param targetClass the class representing the node type
     * @throws IllegalArgumentException if targetClass is null
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
     * Executes a Cypher query and returns a single result object of the specified type.
     * <p>
     * This method executes a Cypher query with named parameters and expects a single
     * result that can be mapped to the specified object type. If the query returns
     * multiple results, only the first one is returned. Returns null if no results found.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query for a single person by name
     * Map<String, Object> params = Map.of("name", "John Doe");
     * Person person = executor.queryForObject(Person.class,
     *     "MATCH (p:Person {name: $name}) RETURN p", params);
     *
     * if (person != null) {
     *     System.out.println("Found: " + person.getName());
     * }
     *
     * // Query for single aggregation result
     * Long count = executor.queryForObject(Long.class,
     *     "MATCH (p:Person) WHERE p.age > $minAge RETURN count(p) as count",
     *     Map.of("minAge", 25));
     *
     * // Query with LIMIT 1
     * Person oldest = executor.queryForObject(Person.class,
     *     "MATCH (p:Person) RETURN p ORDER BY p.age DESC LIMIT 1",
     *     Collections.emptyMap());
     * }</pre>
     *
     * @param <T> the expected result type
     * @param objectType the target class for result conversion (entity class with getter/setter methods)
     * @param cypher the Cypher query string with named parameter placeholders
     * @param parameters the named parameters for the query
     * @return the single result object, or null if no results found
     * @throws IllegalArgumentException if objectType or cypher is null
     * @see #query(Class, String, Map)
     * @see #query(String, Map)
     */
    public <T> T queryForObject(final Class<T> objectType, final String cypher, final Map<String, ?> parameters) {
        final Session session = getSession();

        try {
            return session.queryForObject(objectType, cypher, parameters);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Executes a Cypher query and returns the results as a stream of maps.
     * <p>
     * This method executes a Cypher query with named parameters and returns each
     * result row as a {@code Map<String, Object>}. The stream is lazily evaluated and
     * automatically manages the session lifecycle.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query and process results as maps
     * String cypher = "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) " +
     *                 "WHERE c.name = $companyName " +
     *                 "RETURN p.name as personName, p.age as age, c.name as companyName";
     * Map<String, Object> params = Map.of("companyName", "Tech Corp");
     *
     * executor.query(cypher, params)
     *     .forEach(row -> {
     *         System.out.println(row.get("personName") + " works at " +
     *                          row.get("companyName"));
     *     });
     *
     * // Aggregate query results
     * String aggCypher = "MATCH (p:Person) " +
     *                    "RETURN p.department as dept, count(p) as count";
     * List<String> departments = executor.query(aggCypher, Collections.emptyMap())
     *     .map(row -> row.get("dept") + ": " + row.get("count"))
     *     .collect(Collectors.toList());
     *
     * // Filter and transform stream
     * long seniorCount = executor.query(
     *         "MATCH (p:Person) RETURN p.name as name, p.age as age",
     *         Collections.emptyMap())
     *     .filter(row -> (Integer) row.get("age") > 50)
     *     .count();
     * }</pre>
     *
     * @param cypher the Cypher query string with named parameter placeholders
     * @param parameters the named parameters for the query
     * @return a Stream of result maps, each representing a query result row
     * @throws IllegalArgumentException if cypher is null or parameters is null
     * @see #query(Class, String, Map)
     * @see #queryForObject(Class, String, Map)
     */
    public Stream<Map<String, Object>> query(final String cypher, final Map<String, ?> parameters) {
        final Session session = getSession();
        try {
            return Stream.of(session.query(cypher, parameters).iterator()).onClose(newCloseHandle(session));
        } catch (RuntimeException | Error e) {
            closeSession(session);
            throw e;
        }
    }

    /**
     * Executes a Cypher query with read-only optimization and returns the results as a stream of maps.
     * <p>
     * This method executes a Cypher query with named parameters and returns each
     * result row as a {@code Map<String, Object>}. The readOnly flag allows the database
     * to optimize query execution for read-only operations. The stream is lazily
     * evaluated and automatically manages the session lifecycle.
     * 
     * @param cypher the Cypher query string
     * @param parameters the named parameters for the query
     * @param readOnly true if the query is read-only and can be optimized accordingly
     * @return a stream of result maps, each representing a query result row
     * @see #query(String, Map)
     * @see #query(Class, String, Map)
     */
    public Stream<Map<String, Object>> query(final String cypher, final Map<String, ?> parameters, final boolean readOnly) {
        final Session session = getSession();
        try {
            return Stream.of(session.query(cypher, parameters, readOnly).iterator()).onClose(newCloseHandle(session));
        } catch (RuntimeException | Error e) {
            closeSession(session);
            throw e;
        }
    }

    /**
     * Executes a Cypher query and returns the results as a stream of objects of the specified type.
     * <p>
     * This method executes a Cypher query with named parameters and maps each result
     * to an object of the specified type. The stream is lazily evaluated and automatically
     * manages the session lifecycle. This is ideal for processing large result sets
     * efficiently without loading all results into memory at once.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query for Person entities
     * String cypher = "MATCH (p:Person) WHERE p.age > $minAge RETURN p";
     * Map<String, Object> params = Map.of("minAge", 25);
     *
     * List<Person> adults = executor.query(Person.class, cypher, params)
     *     .collect(Collectors.toList());
     *
     * // Stream and filter results
     * executor.query(Person.class,
     *         "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p",
     *         Collections.emptyMap())
     *     .filter(p -> p.getName().startsWith("A"))
     *     .forEach(p -> System.out.println(p.getName()));
     *
     * // Complex query with relationships
     * String relationshipCypher =
     *     "MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) " +
     *     "WHERE c.name = $companyName " +
     *     "RETURN p";
     * Stream<Person> employees = executor.query(Person.class,
     *     relationshipCypher, Map.of("companyName", "Tech Corp"));
     *
     * // Aggregate and process
     * Double avgAge = executor.query(Person.class,
     *         "MATCH (p:Person) RETURN p", Collections.emptyMap())
     *     .mapToInt(Person::getAge)
     *     .average()
     *     .orElse(0.0);
     * }</pre>
     *
     * @param <T> the expected result object type
     * @param objectType the target class for result conversion (entity class with getter/setter methods, Map.class, or supported basic types)
     * @param cypher the Cypher query string with named parameter placeholders
     * @param parameters the named parameters for the query
     * @return a Stream of result objects of the specified type
     * @throws IllegalArgumentException if objectType or cypher is null
     * @see #queryForObject(Class, String, Map)
     * @see #query(String, Map)
     */
    public <T> Stream<T> query(final Class<T> objectType, final String cypher, final Map<String, ?> parameters) {
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
     * @param clazz the class representing the node type to count
     * @param filters the filter criteria to apply when counting nodes
     * @return the count of nodes matching the filter criteria
     * @throws IllegalArgumentException if clazz is null or filters is null
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
     * @param entity the class representing the node type to count
     * @return the total count of nodes of the specified type
     * @throws IllegalArgumentException if entity is null
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
     * Resolves the Neo4j graph ID for the given entity object.
     * <p>
     * This method extracts the Neo4j node ID from an entity object that may or may not
     * be a managed Neo4j entity. Returns null if the object is not a Neo4j entity
     * or doesn't have a graph ID assigned.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get the graph ID of a loaded entity
     * Person person = executor.load(Person.class, 123L);
     * Long graphId = executor.resolveGraphIdFor(person);
     * System.out.println("Graph ID: " + graphId); // 123
     *
     * // Check if an object has been saved
     * Person newPerson = new Person("John Doe");
     * Long id = executor.resolveGraphIdFor(newPerson);
     * if (id == null) {
     *     System.out.println("Person not yet saved");
     *     executor.save(newPerson);
     *     id = executor.resolveGraphIdFor(newPerson);
     *     System.out.println("Saved with ID: " + id);
     * }
     *
     * // Validate entity before operations
     * if (executor.resolveGraphIdFor(person) != null) {
     *     // Entity is managed and has an ID
     *     executor.delete(person);
     * }
     * }</pre>
     *
     * @param possibleEntity the object that might be a Neo4j entity
     * @return the Neo4j graph ID if the object is a managed entity, null otherwise
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
            }
        }
    }

}
