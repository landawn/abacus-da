/*
 * Copyright (C) 2022 HaiYang Li
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

package com.landawn.abacus.da.gcp;

import static com.landawn.abacus.query.Dsl.PAC;
import static com.landawn.abacus.query.Dsl.PLC;
import static com.landawn.abacus.query.Dsl.PSC;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.query.SqlBuilder;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.ImmutableSet;
import com.landawn.abacus.util.IntFunctions;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.Numbers;
import com.landawn.abacus.util.RowDataset;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple2;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.stream.Stream;

/**
 * A high-level executor on top of the Google Cloud {@link BigQuery} client. It provides POJO- and
 * map-driven INSERT/UPDATE/DELETE, parameterised SELECTs, and {@link Stream}/{@link Dataset}
 * result handling against BigQuery's <i>query jobs</i> API.
 *
 * <h2>Query-Job Semantics (no streaming insert)</h2>
 * <p>Every method on this executor ultimately submits work as a synchronous BigQuery <i>query job</i>
 * via {@link BigQuery#query(QueryJobConfiguration, com.google.cloud.bigquery.BigQuery.JobOption...)}.
 * That includes the DML helpers ({@link #insert(Object)}, {@link #update(Object)},
 * {@link #delete(Object)}), which build {@code INSERT}/{@code UPDATE}/{@code DELETE} statements
 * with {@link SqlBuilder} and submit them as ordinary query jobs. As a result, every call:</p>
 * <ul>
 *   <li>Counts against BigQuery's <i>bytes processed</i> billing model (DML on partitioned tables
 *       processes whole partitions; consider partition filters for cost control).</li>
 *   <li>Is governed by BigQuery's per-table DML concurrency limits.</li>
 *   <li>Blocks the calling thread until the job completes; an {@link InterruptedException} during
 *       the wait is rethrown as a {@link RuntimeException} after the thread's interrupt flag is
 *       reset.</li>
 * </ul>
 * <p>This executor <b>does not</b> wrap the BigQuery <i>streaming insert</i> API
 * ({@code tabledata.insertAll}), the load-job API, the dataset/table administrative API, or
 * dry-run cost estimation. For those, obtain the underlying client via {@link #bigQuery()} and
 * build the appropriate {@code JobConfiguration} (e.g.
 * {@code QueryJobConfiguration.newBuilder(sql).setDryRun(true).build()}) yourself, then submit it
 * with {@code bigQuery.query(...)} or {@code bigQuery.create(JobInfo.of(...))}.</p>
 *
 * <h2>Dataset and Table Naming</h2>
 * <p>The {@link SqlBuilder}-driven helpers derive the BigQuery table name from the target Java
 * class (via the configured {@link NamingPolicy}); the resulting SQL therefore uses an
 * <i>unqualified</i> table name. To target a specific dataset or project you must either:</p>
 * <ul>
 *   <li>Set the default dataset on the underlying {@link BigQuery} client (e.g. via
 *       {@code BigQueryOptions} or a {@code QueryJobConfiguration#setDefaultDataset(DatasetId)}
 *       on a custom configuration), or</li>
 *   <li>Pass a fully-qualified SQL string ({@code `project.dataset.table`}) to the
 *       {@link #execute(String, Object...)}, {@link #list(Class, String, Object...)},
 *       {@link #query(Class, String, Object...)}, or {@link #stream(Class, String, Object...)}
 *       overloads.</li>
 * </ul>
 *
 * <h2>Parameter Binding</h2>
 * <p>Custom SQL methods use positional ({@code ?}) parameters; values are converted to
 * {@link QueryParameterValue} by {@link #buildQueryParameterValue(Object...)} (e.g. {@code String},
 * {@code Boolean}, integer/floating-point primitives and their boxed equivalents,
 * {@link BigDecimal}, {@code java.util.Date}/{@code java.sql.*}, {@code com.google.cloud.Date},
 * {@code com.google.cloud.Timestamp}, and {@code byte[]} are mapped to their native BigQuery
 * types; arrays, collections, maps, and beans are serialised to JSON; any other scalar &mdash;
 * including {@code java.time} types such as {@code LocalDate}/{@code LocalDateTime}/{@code Instant},
 * enums, and {@code UUID} &mdash; is bound as a {@code STRING} parameter, so use
 * {@code java.sql.Date}/{@code java.sql.Timestamp} to bind native DATE/TIMESTAMP values).
 * {@code null} parameters must be wrapped in a {@link QueryParameterValue} so the SQL type is
 * known.</p>
 *
 * <h2>Result Set Pagination</h2>
 * <p>Pagination is delegated to BigQuery's {@link TableResult}: {@link #list}/{@link #query}
 * eagerly materialise every page (via {@link TableResult#iterateAll()}); {@link #stream} returns a
 * lazy {@link Stream} backed by the same iterator. Use {@link #stream} for large result sets to
 * keep memory bounded.</p>
 *
 * <h2>Naming Policy</h2>
 * <p>Property-to-column translation is governed by the {@link NamingPolicy} passed at
 * construction time:</p>
 * <ul>
 *   <li>{@link NamingPolicy#SNAKE_CASE} (default) &mdash; e.g. {@code firstName} &rarr; {@code first_name}</li>
 *   <li>{@link NamingPolicy#SCREAMING_SNAKE_CASE} &mdash; e.g. {@code firstName} &rarr; {@code FIRST_NAME}</li>
 *   <li>{@link NamingPolicy#CAMEL_CASE} &mdash; e.g. {@code firstName} &rarr; {@code firstName}</li>
 * </ul>
 * <p>Any other naming policy raises {@link IllegalStateException} from the DML helpers.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize executor
 * BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
 * BigQueryExecutor executor = new BigQueryExecutor(bigQuery);
 *
 * // Entity operations
 * Customer customer = new Customer("123", "John Doe");
 * executor.insert(customer);
 *
 * // SQL queries
 * List<Customer> customers = executor.list(Customer.class,
 *     "SELECT * FROM customers WHERE status = ?", "active");
 *
 * // Streaming operations
 * Stream<Customer> customerStream = executor.stream(Customer.class,
 *     "SELECT * FROM customers WHERE created_date > ?", yesterday);
 *
 * // Complex queries with conditions (Java property names; the naming policy maps them to columns)
 * Dataset results = executor.query(Customer.class,
 *     Filters.and(Filters.eq("status", "active"), Filters.gt("createdDate", yesterday)));
 * }</pre>
 *
 * @see com.google.cloud.bigquery.BigQuery
 * @see com.google.cloud.bigquery.QueryJobConfiguration
 * @see com.google.cloud.bigquery.TableResult
 */
public class BigQueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryExecutor.class);

    private static volatile java.lang.reflect.Field schemaFieldOfFieldList;

    static {
        java.lang.reflect.Field tmp = null;

        try {
            tmp = FieldValueList.class.getDeclaredField("schema");
        } catch (final Throwable e) {
            // ignore: optional optimization, falls back to the public API below.
            if (logger.isDebugEnabled()) {
                logger.debug("Unable to access FieldValueList.schema via reflection; falling back to public API", e);
            }
        }

        if (tmp != null && tmp.getType().equals(FieldList.class)) {
            if (ClassUtil.setAccessibleQuietly(tmp, true)) {
                // Good!
            } else {
                tmp = null;
            }
        }

        schemaFieldOfFieldList = tmp != null && tmp.getType().equals(FieldList.class) ? tmp : null;
    }

    static {
        final BiFunction<FieldValueList, Class<?>, Object> converter = BigQueryExecutor::readRow;

        N.registerConverter(FieldValueList.class, converter);
    }

    private final BigQuery bigQuery;
    private final NamingPolicy namingPolicy;

    /**
     * Constructs a {@code BigQueryExecutor} backed by the supplied {@link BigQuery} client with
     * the default {@link NamingPolicy#SNAKE_CASE} translation policy (e.g.
     * {@code firstName} &rarr; {@code first_name}).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
     * BigQueryExecutor executor = new BigQueryExecutor(bigQuery);
     * executor.bigQuery();                       // returns the same bigQuery instance
     *
     * // Edge: a null client is rejected eagerly
     * new BigQueryExecutor(null);                // throws IllegalArgumentException
     * }</pre>
     *
     * @param bigQuery the Google Cloud BigQuery client this executor will delegate to
     * @throws IllegalArgumentException if {@code bigQuery} is {@code null}
     * @see #BigQueryExecutor(BigQuery, NamingPolicy)
     * @see NamingPolicy#SNAKE_CASE
     */
    public BigQueryExecutor(final BigQuery bigQuery) {
        this(bigQuery, NamingPolicy.SNAKE_CASE);
    }

    /**
     * Constructs a {@code BigQueryExecutor} backed by the supplied {@link BigQuery} client with an
     * explicit property-to-column {@link NamingPolicy}.
     * <p>
     * Only {@link NamingPolicy#SNAKE_CASE}, {@link NamingPolicy#SCREAMING_SNAKE_CASE}, and
     * {@link NamingPolicy#CAMEL_CASE} are honoured by the DML helpers; passing any other naming
     * policy will cause the first INSERT/UPDATE/DELETE call to fail with
     * {@link IllegalStateException}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
     * BigQueryExecutor executor =
     *     new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);  // firstName -> FIRST_NAME
     * executor.bigQuery();                                                    // returns the same bigQuery instance
     *
     * // Edge: both arguments are validated eagerly
     * new BigQueryExecutor(null, NamingPolicy.SNAKE_CASE);                    // throws IllegalArgumentException
     * new BigQueryExecutor(bigQuery, null);                                   // throws IllegalArgumentException
     *
     * // Note: an unsupported policy is accepted here but fails on the first DML call
     * BigQueryExecutor bad = new BigQueryExecutor(bigQuery, NamingPolicy.NO_CHANGE);
     * bad.insert(someEntity);                                                 // throws IllegalStateException
     * }</pre>
     *
     * @param bigQuery the Google Cloud BigQuery client this executor will delegate to
     * @param namingPolicy the convention used when translating Java property names to BigQuery
     *                     column/table names
     * @throws IllegalArgumentException if {@code bigQuery} or {@code namingPolicy} is {@code null}
     * @see NamingPolicy
     */
    public BigQueryExecutor(final BigQuery bigQuery, final NamingPolicy namingPolicy) {
        if (bigQuery == null) {
            throw new IllegalArgumentException("bigQuery cannot be null");
        }
        if (namingPolicy == null) {
            throw new IllegalArgumentException("namingPolicy cannot be null");
        }
        this.bigQuery = bigQuery;
        this.namingPolicy = namingPolicy;
    }

    /**
     * Returns the underlying {@link BigQuery} client this executor was constructed with.
     * <p>
     * Use this to reach BigQuery features not surfaced by this executor &mdash; for example
     * streaming inserts via {@code bigQuery().insertAll(...)}, dry-run cost estimation via a
     * custom {@link QueryJobConfiguration} with {@code setDryRun(true)}, or dataset/table
     * administration.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
     * BigQueryExecutor executor = new BigQueryExecutor(bigQuery);
     *
     * BigQuery client = executor.bigQuery();   // returns the exact same instance passed to the constructor
     * client.listDatasets();                   // reach an admin API not surfaced by this executor
     * }</pre>
     *
     * @return the underlying BigQuery client; never {@code null}
     * @see com.google.cloud.bigquery.BigQuery
     */
    public BigQuery bigQuery() {
        return bigQuery;
    }

    /**
     * Converts a BigQuery {@link FieldValueList} (a single result row) to an instance of
     * {@code entityClass}, mapping column names to bean properties according to the supplied
     * {@link Schema}.
     * <p>
     * Convenience overload that immediately extracts {@code schema.getFields()} and delegates to
     * {@link #toEntity(FieldList, FieldValueList, Class)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
     *                                 Field.of("name", StandardSQLTypeName.STRING));
     * Schema schema = Schema.of(fields);
     * FieldValueList row = FieldValueList.of(Arrays.asList(
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"),
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "John")), fields);
     *
     * Customer c = BigQueryExecutor.toEntity(schema, row, Customer.class);
     * c.getId();                                                  // returns 123
     * c.getName();                                                // returns "John"
     *
     * // Edge: a null schema is rejected
     * BigQueryExecutor.toEntity((Schema) null, row, Customer.class);  // throws IllegalArgumentException
     *
     * // Edge: a non-bean target class is rejected
     * BigQueryExecutor.toEntity(schema, row, String.class);          // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the entity type
     * @param schema the schema describing {@code fieldValueList}; must not be {@code null}
     * @param fieldValueList the row to convert
     * @param entityClass the bean class to instantiate; must declare standard getter/setter methods
     * @return a new {@code entityClass} instance with properties populated from {@code fieldValueList}
     * @throws IllegalArgumentException if {@code schema} is {@code null} or {@code entityClass} is
     *                                  not a recognised bean class
     * @see #toEntity(FieldList, FieldValueList, Class)
     * @see #toEntity(FieldValueList, Class)
     */
    public static <T> T toEntity(final Schema schema, final FieldValueList fieldValueList, final Class<T> entityClass) {
        N.checkArgNotNull(schema, "schema");

        return toEntity(schema.getFields(), fieldValueList, entityClass);
    }

    /**
     * Converts a BigQuery {@link FieldValueList} (a single result row) to an instance of
     * {@code entityClass}, mapping field names to bean properties via the supplied
     * {@link FieldList}.
     * <p>
     * Column-to-property resolution proceeds in this order: an exact match on the bean's property
     * name, then a column-to-property name map derived from the bean's annotations
     * ({@code @Column}, etc.). Columns whose names contain a period are written via
     * {@code BeanInfo#setPropValue(..., true)} so dotted paths can populate nested beans even when
     * no top-level property matches. Nested {@link FieldValueList} values are converted recursively
     * into the property's declared type.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
     *                                 Field.of("name", StandardSQLTypeName.STRING));
     * FieldValueList row = FieldValueList.of(Arrays.asList(
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "456"),
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Jane")), fields);
     *
     * Customer c = BigQueryExecutor.toEntity(fields, row, Customer.class);
     * c.getId();                                                  // returns 456
     * c.getName();                                                // returns "Jane"
     *
     * // Edge: a non-bean target class is rejected
     * BigQueryExecutor.toEntity(fields, row, String.class);      // throws IllegalArgumentException
     *
     * // Edge: a null field list is rejected
     * BigQueryExecutor.toEntity((FieldList) null, row, Customer.class);  // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the entity type
     * @param fields the field list describing {@code fieldValueList}; must not be {@code null}
     * @param fieldValueList the row to convert; must not be {@code null}
     * @param entityClass the bean class to instantiate; must declare standard getter/setter methods
     * @return a new {@code entityClass} instance with properties populated from {@code fieldValueList}
     * @throws IllegalArgumentException if {@code entityClass} is not a recognised bean class, or if
     *                                  {@code fields} or {@code fieldValueList} is {@code null}
     * @see #toEntity(Schema, FieldValueList, Class)
     * @see #toEntity(FieldValueList, Class)
     */
    public static <T> T toEntity(final FieldList fields, final FieldValueList fieldValueList, final Class<T> entityClass) {
        N.checkArgument(Beans.isBeanClass(entityClass), "{} is not a valid entity class with getter/setter method", entityClass);
        N.checkArgNotNull(fields, "fields");
        N.checkArgNotNull(fieldValueList, "fieldValueList");

        final Map<String, String> column2FieldNameMap = QueryUtil.getColumn2PropNameMap(entityClass);
        final BeanInfo entityInfo = ParserUtil.getBeanInfo(entityClass);
        final Object entity = entityInfo.createBeanResult();

        PropInfo propInfo = null;
        String propName = null;
        Object propValue = null;
        Class<?> parameterType = null;
        String fieldName = null;

        for (int i = 0, size = fieldValueList.size(); i < size; i++) {
            propName = fields.get(i).getName();
            propValue = fieldValueList.get(i).getValue();

            propInfo = entityInfo.getPropInfo(propName);

            if (propInfo == null && (fieldName = column2FieldNameMap.get(propName)) != null) {
                propName = fieldName;
                propInfo = entityInfo.getPropInfo(propName);
            }

            if (propInfo == null) {
                if (propName.indexOf(SK._PERIOD) > 0) { //NOSONAR
                    entityInfo.setPropValue(entity, propName, propValue, true);
                }

                continue;
            }

            parameterType = propInfo.clazz;

            if (propValue instanceof FieldValueList) {
                // RECORD value. Note a FieldValueList IS a List, so a plain assignability check against a
                // List-typed property would store the raw FieldValue wrappers; convert through readRow
                // unless the property explicitly wants the raw FieldValueList.
                if (FieldValueList.class.isAssignableFrom(parameterType)) {
                    propInfo.setPropValue(entity, propValue);
                } else {
                    propInfo.setPropValue(entity, readRow((FieldValueList) propValue, parameterType));
                }
            } else if (propValue instanceof List) {
                // REPEATED column: FieldValue.getValue() returns List<FieldValue>. Unwrap the wrapper
                // elements (mirroring the toMap path's toMapValue) and convert through the property's
                // FULL generic type so e.g. a List<Long> property holds typed values. N.convert can't be
                // used here: it short-circuits when the raw container class is assignable
                // (ArrayList -> List) and would keep the raw String elements, so the value is rebuilt
                // through the parameterized Type's JSON codec instead.
                final Object unwrapped = unwrapRepeatedValue(fields.get(i), (List<?>) propValue);
                propInfo.setPropValue(entity, propInfo.jsonXmlType.isParameterizedType() ? propInfo.jsonXmlType.valueOf(N.toJson(unwrapped)) : unwrapped);
            } else {
                propInfo.setPropValue(entity, propValue);
            }
        }

        return entityInfo.finishBeanResult(entity);
    }

    // Unwraps a REPEATED column's value — a List of FieldValue wrappers — into a List of plain values,
    // converting nested RECORD elements via the field's sub-fields. This is the entity/array/Dataset
    // sibling of the map path's toMapValue.
    private static Object unwrapRepeatedValue(final Field field, final List<?> values) {
        final List<Object> list = new ArrayList<>(values.size());
        final FieldList subFields = field == null ? null : field.getSubFields();

        for (final Object element : values) {
            if (element instanceof final FieldValue repeatedFieldValue) {
                final Object repeatedValue = repeatedFieldValue.getValue();

                if (repeatedValue instanceof FieldValueList) {
                    list.add(subFields == null ? readRow((FieldValueList) repeatedValue, Object[].class)
                            : toMap(subFields, (FieldValueList) repeatedValue, IntFunctions.ofMap()));
                } else {
                    list.add(repeatedValue);
                }
            } else {
                list.add(element);
            }
        }

        return list;
    }

    /**
     * Converts a {@link FieldValueList} to an instance of {@code entityClass}, extracting the
     * schema from the row itself via {@link #getSchema(FieldValueList)} (reflection-based access
     * to {@code FieldValueList.schema}).
     * <p>
     * This is a convenience overload of {@link #toEntity(FieldList, FieldValueList, Class)} for
     * callers who don't have a separate {@link FieldList} handy. Note that the underlying
     * reflection-based schema extraction will fail if the BigQuery client library hides the
     * {@code schema} field; in long-running processes prefer the
     * {@link #toEntity(FieldList, FieldValueList, Class)} overload.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
     *                                 Field.of("name", StandardSQLTypeName.STRING));
     * FieldValueList row = FieldValueList.of(Arrays.asList(
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "789"),
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Bob")), fields);
     *
     * Customer c = BigQueryExecutor.toEntity(row, Customer.class);   // schema read reflectively from the row
     * c.getId();                                                     // returns 789
     * c.getName();                                                   // returns "Bob"
     *
     * // Edge: a non-bean target class is rejected
     * BigQueryExecutor.toEntity(row, String.class);                 // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the entity type
     * @param fieldValueList the row to convert
     * @param entityClass the bean class to instantiate; must declare standard getter/setter methods
     * @return a new {@code entityClass} instance with properties populated from {@code fieldValueList}
     * @throws IllegalArgumentException if {@code entityClass} is not a recognised bean class, or if
     *                                  the schema cannot be retrieved from {@code fieldValueList}
     * @see #toEntity(FieldList, FieldValueList, Class)
     */
    public static <T> T toEntity(final FieldValueList fieldValueList, final Class<T> entityClass) {
        N.checkArgNotNull(fieldValueList, "fieldValueList");

        return toEntity(getSchema(fieldValueList), fieldValueList, entityClass);
    }

    /**
     * Converts a BigQuery {@link FieldValueList} (a single result row) to a {@code Map}, using the
     * supplied {@link Schema} to provide the column names that become the map's keys.
     * <p>
     * Returns a {@link HashMap}. Nested {@link FieldValueList} values (BigQuery {@code STRUCT}s)
     * are recursively converted to nested {@link Map}s.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
     *                                 Field.of("name", StandardSQLTypeName.STRING));
     * Schema schema = Schema.of(fields);
     * FieldValueList row = FieldValueList.of(Arrays.asList(
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"),
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "John")), fields);
     *
     * Map<String, Object> map = BigQueryExecutor.toMap(schema, row);  // a HashMap
     * map.get("id");                                                  // returns "123"
     * map.get("name");                                                // returns "John"
     *
     * // Edge: a null schema is rejected
     * BigQueryExecutor.toMap((Schema) null, row);                    // throws IllegalArgumentException
     * }</pre>
     *
     * @param schema the schema describing {@code fieldValueList}; must not be {@code null}
     * @param fieldValueList the row to convert
     * @return a new {@code HashMap} with one entry per field in {@code schema}
     * @throws IllegalArgumentException if {@code schema} or {@code fieldValueList} is {@code null}
     * @see #toMap(FieldList, FieldValueList)
     */
    public static Map<String, Object> toMap(final Schema schema, final FieldValueList fieldValueList) {
        N.checkArgNotNull(schema, "schema");

        return toMap(schema.getFields(), fieldValueList);
    }

    /**
     * Converts a BigQuery FieldValueList to a Map using the provided field list.
     * <p>
     * This method creates a HashMap where keys are column names from the field list and values
     * are the corresponding data from the FieldValueList. This is equivalent to calling
     * {@code toMap(fields, fieldValueList, IntFunctions.ofMap())}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
     *                                 Field.of("name", StandardSQLTypeName.STRING));
     * FieldValueList row = FieldValueList.of(Arrays.asList(
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "456"),
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Jane")), fields);
     *
     * Map<String, Object> map = BigQueryExecutor.toMap(fields, row);  // a HashMap
     * map.get("id");                                                  // returns "456"
     * map.get("name");                                                // returns "Jane"
     *
     * // Edge: a null field list is rejected
     * BigQueryExecutor.toMap((FieldList) null, row);                 // throws IllegalArgumentException
     * }</pre>
     *
     * @param fields the BigQuery field list defining field structure and names
     * @param fieldValueList the row data from BigQuery containing the values
     * @return a HashMap containing column names as keys and corresponding values
     * @throws IllegalArgumentException if fields or fieldValueList is null
     * @see #toMap(FieldList, FieldValueList, IntFunction)
     */
    public static Map<String, Object> toMap(final FieldList fields, final FieldValueList fieldValueList) {
        return toMap(fields, fieldValueList, IntFunctions.ofMap());
    }

    /**
     * Converts a BigQuery {@link FieldValueList} to a {@code Map} of caller-chosen type, using the
     * supplied {@link FieldList} for the column names that become the map's keys.
     * <p>
     * {@code supplier} is invoked with the expected entry count and must return a writable map of
     * the desired implementation type (e.g. {@link IntFunctions#ofMap()},
     * {@link IntFunctions#ofLinkedHashMap()}). Nested {@link FieldValueList} values are recursively
     * converted into nested maps, with each recursion also using {@code supplier} (the inner
     * schemas are extracted via {@link #getSchema(FieldValueList)}).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
     *                                 Field.of("name", StandardSQLTypeName.STRING));
     * FieldValueList row = FieldValueList.of(Arrays.asList(
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9"),
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Z")), fields);
     *
     * Map<String, Object> map = BigQueryExecutor.toMap(fields, row, IntFunctions.ofLinkedHashMap());
     * (map instanceof LinkedHashMap);                                // true
     * map.get("id");                                                 // returns "9"
     *
     * // Edge: a null supplier is rejected
     * BigQueryExecutor.toMap(fields, row, null);                     // throws IllegalArgumentException
     * }</pre>
     *
     * @param fields the field list describing {@code fieldValueList}; must not be {@code null}
     * @param fieldValueList the row to convert; must not be {@code null}
     * @param supplier creates the outer {@link Map} instance given the expected size; must not be
     *                 {@code null}
     * @return the map produced by {@code supplier}, populated with one entry per field
     * @throws IllegalArgumentException if any argument is {@code null}
     * @see IntFunctions#ofMap()
     * @see IntFunctions#ofLinkedHashMap()
     */
    public static Map<String, Object> toMap(final FieldList fields, final FieldValueList fieldValueList,
            final IntFunction<? extends Map<String, Object>> supplier) {
        N.checkArgNotNull(fields, "fields");
        N.checkArgNotNull(fieldValueList, "fieldValueList");
        N.checkArgNotNull(supplier, "supplier");

        final Map<String, Object> map = supplier.apply(fieldValueList.size());

        for (int i = 0, size = fieldValueList.size(); i < size; i++) {
            map.put(fields.get(i).getName(), toMapValue(fields.get(i), fieldValueList.get(i), supplier));
        }

        return map;
    }

    private static Object toMapValue(final Field field, final FieldValue fieldValue, final IntFunction<? extends Map<String, Object>> supplier) {
        final Object value = fieldValue.getValue();

        if (value instanceof FieldValueList) {
            return toMap((FieldValueList) value, supplier);
        }

        if (value instanceof final List<?> values) {
            final List<Object> list = new ArrayList<>(values.size());
            final FieldList subFields = field.getSubFields();

            for (final Object element : values) {
                if (element instanceof final FieldValue repeatedFieldValue) {
                    final Object repeatedValue = repeatedFieldValue.getValue();
                    list.add(repeatedValue instanceof FieldValueList ? toMap(subFields, (FieldValueList) repeatedValue, supplier) : repeatedValue);
                } else {
                    list.add(element);
                }
            }

            return list;
        }

        return value;
    }

    /**
     * Converts a BigQuery FieldValueList to a HashMap by automatically extracting the schema.
     * <p>
     * This is a convenience method that automatically extracts the schema information from the
     * FieldValueList and creates a HashMap containing the column-value mappings.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
     *                                 Field.of("name", StandardSQLTypeName.STRING));
     * FieldValueList row = FieldValueList.of(Arrays.asList(
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "111"),
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Amy")), fields);
     *
     * Map<String, Object> map = BigQueryExecutor.toMap(row);  // schema read reflectively from the row
     * map.get("id");                                          // returns "111"
     * map.get("name");                                        // returns "Amy"
     * }</pre>
     *
     * @param fieldValueList the row data from BigQuery containing both schema and values
     * @return a HashMap containing column names as keys and corresponding values
     * @throws IllegalArgumentException if fieldValueList is null
     * @see #toMap(Schema, FieldValueList)
     */
    public static Map<String, Object> toMap(final FieldValueList fieldValueList) {
        N.checkArgNotNull(fieldValueList, "fieldValueList");

        return toMap(getSchema(fieldValueList), fieldValueList);
    }

    /**
     * Converts a BigQuery FieldValueList to a Map using the provided Map supplier.
     * <p>
     * This is a convenience method that automatically extracts the schema information from the
     * FieldValueList and creates a Map of the specified type containing the column-value mappings.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
     *                                 Field.of("name", StandardSQLTypeName.STRING));
     * FieldValueList row = FieldValueList.of(Arrays.asList(
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "222"),
     *         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Sam")), fields);
     *
     * Map<String, Object> map = BigQueryExecutor.toMap(row, IntFunctions.ofLinkedHashMap());
     * (map instanceof LinkedHashMap);                        // true
     * map.get("id");                                         // returns "222"
     * map.get("name");                                       // returns "Sam"
     * }</pre>
     *
     * @param fieldValueList the row data from BigQuery containing both schema and values
     * @param supplier a function that creates Map instances given the expected size
     * @return a Map of the supplier's type containing column names as keys and corresponding values
     * @throws IllegalArgumentException if fieldValueList or supplier is null
     * @see #toMap(FieldList, FieldValueList, IntFunction)
     */
    public static Map<String, Object> toMap(final FieldValueList fieldValueList, final IntFunction<? extends Map<String, Object>> supplier) {
        N.checkArgNotNull(fieldValueList, "fieldValueList");

        return toMap(getSchema(fieldValueList), fieldValueList, supplier);
    }

    @SuppressWarnings({ "rawtypes", "null" })
    private static <T> T readRow(final FieldValueList row, final Class<T> rowClass) {
        if (row == null) {
            return rowClass == null ? null : N.defaultValueOf(rowClass);
        }

        final Type<?> rowType = rowClass == null ? null : N.typeOf(rowClass);
        final FieldList fields = getSchema(row);
        final int fieldCount = fields.size();
        Object res = null;
        Object value = null;

        if (rowType == null || rowType.isObjectArray()) {
            final Object[] a = rowClass == null ? new Object[fieldCount] : N.newArray(rowClass.getComponentType(), fieldCount);

            for (int i = 0; i < fieldCount; i++) {
                value = row.get(i).getValue();

                if (value instanceof FieldValueList) {
                    a[i] = readRow((FieldValueList) value, Object[].class);
                } else if (value instanceof List) {
                    a[i] = unwrapRepeatedValue(fields.get(i), (List<?>) value);
                } else {
                    a[i] = value;
                }
            }

            res = a;
        } else if (rowType.isCollection()) {
            final Collection<Object> c = N.newCollection((Class<Collection>) rowClass);

            for (int i = 0; i < fieldCount; i++) {
                value = row.get(i).getValue();

                if (value instanceof FieldValueList) {
                    c.add(readRow((FieldValueList) value, List.class));
                } else if (value instanceof List) {
                    c.add(unwrapRepeatedValue(fields.get(i), (List<?>) value));
                } else {
                    c.add(value);
                }
            }

            res = c;
        } else if (rowType.isMap()) {
            res = toMap(row, IntFunctions.ofMap((Class<Map>) rowClass));
        } else if (rowType.isBean()) {
            res = toEntity(row, rowClass);
        } else if (fieldCount == 1) {
            value = row.get(0).getValue();

            if (value == null || rowClass.isAssignableFrom(value.getClass())) {
                res = value;
            } else {
                res = N.convert(value, rowClass);
            }

        } else {
            throw new IllegalArgumentException("Field count must be 1 to map a row to the single-value type: " + ClassUtil.getCanonicalClassName(rowClass)
                    + ", but the row has " + fieldCount + " columns");
        }

        return (T) res;
    }

    private static <T> Function<? super FieldValueList, ? extends T> createRowMapper(final Class<T> rowClass, final FieldList fields) {
        final Type<?> rowType = rowClass == null ? null : Type.of(rowClass);
        Function<? super FieldValueList, ? extends T> mapper = null;

        if (rowType == null || rowType.isObjectArray()) {
            mapper = new Function<>() {
                private FieldList rowFields = fields;
                private int fieldCount = fields == null ? 0 : fields.size();

                @Override
                public T apply(final FieldValueList row) throws RuntimeException {
                    if (rowFields == null) {
                        rowFields = getSchema(row);
                        fieldCount = rowFields.size();
                    }

                    final Object[] a = rowClass == null ? new Object[fieldCount] : N.newArray(rowClass.getComponentType(), fieldCount);
                    Object value = null;

                    for (int i = 0; i < fieldCount; i++) {
                        value = row.get(i).getValue();

                        if (value instanceof FieldValueList) {
                            a[i] = readRow((FieldValueList) value, Object[].class);
                        } else if (value instanceof List) {
                            a[i] = unwrapRepeatedValue(rowFields.get(i), (List<?>) value);
                        } else {
                            a[i] = value;
                        }
                    }

                    return (T) a;
                }
            };

        } else if (rowType.isCollection()) {
            mapper = new Function<>() {
                private FieldList rowFields = fields;
                private int fieldCount = fields == null ? 0 : fields.size();

                @Override
                public T apply(final FieldValueList row) throws RuntimeException {
                    if (rowFields == null) {
                        rowFields = getSchema(row);
                        fieldCount = rowFields.size();
                    }

                    @SuppressWarnings("rawtypes")
                    final Collection<Object> c = N.newCollection((Class<Collection>) rowClass);
                    Object value = null;

                    for (int i = 0; i < fieldCount; i++) {
                        value = row.get(i).getValue();

                        if (value instanceof FieldValueList) {
                            c.add(readRow((FieldValueList) value, List.class));
                        } else if (value instanceof List) {
                            c.add(unwrapRepeatedValue(rowFields.get(i), (List<?>) value));
                        } else {
                            c.add(value);
                        }
                    }

                    return (T) c;
                }
            };
        } else if (Map.class.isAssignableFrom(rowClass)) {
            mapper = new Function<>() {
                private FieldList rowFields = fields;

                @SuppressWarnings("rawtypes")
                @Override
                public T apply(final FieldValueList row) throws RuntimeException {
                    if (rowFields == null) {
                        rowFields = getSchema(row);
                    }

                    return (T) toMap(rowFields, row, IntFunctions.ofMap((Class<Map>) rowClass));
                }
            };
        } else if (Beans.isBeanClass(rowClass)) {
            mapper = new Function<>() {
                private FieldList rowFields = fields;

                @Override
                public T apply(final FieldValueList row) throws RuntimeException {
                    if (rowFields == null) {
                        rowFields = getSchema(row);
                    }

                    return toEntity(rowFields, row, rowClass);
                }
            };
        } else {
            mapper = new Function<>() {
                private boolean isAssignable = false;
                private Class<?> valueClass = null;
                int fieldCount = -1;

                @SuppressWarnings("null")
                @Override
                public T apply(final FieldValueList row) throws RuntimeException {
                    if (fieldCount < 0) {
                        fieldCount = row.size();

                        if (fieldCount != 1) {
                            throw new IllegalArgumentException("Field count must be 1 for type: " + rowClass);
                        }
                    }

                    if (isAssignable) {
                        return (T) row.get(0).getValue();
                    }

                    final Object value = row.get(0).getValue();

                    if (valueClass == null && value != null) {
                        valueClass = value.getClass();
                        isAssignable = rowClass.isAssignableFrom(valueClass);

                        if (isAssignable) {
                            return (T) value;
                        } else {
                            return N.convert(value, rowClass);
                        }
                    } else {
                        return N.convert(value, rowClass);
                    }
                }
            };
        }

        return mapper;
    }

    /**
     * Materialises every row of a BigQuery {@link TableResult} into a {@link List}.
     * <p>
     * The row mapper used internally is chosen by {@code targetClass}:
     * <ul>
     *   <li>An entity bean class &rarr; each row is mapped via
     *       {@link #toEntity(FieldList, FieldValueList, Class)}.</li>
     *   <li>{@link Map} or a {@link Map} subclass &rarr; each row becomes a map of column-to-value.</li>
     *   <li>An object-array class (e.g. {@code Object[].class}) or {@code null} &rarr; each row is
     *       returned as an object array sized to the row width.</li>
     *   <li>A {@link Collection} subclass &rarr; each row becomes a collection of values in column
     *       order.</li>
     *   <li>Any other class &rarr; the result is treated as a single-column scalar; the first
     *       column's value is converted to {@code targetClass}. Rows with more than one column
     *       raise {@link IllegalArgumentException} during iteration.</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // tableResult holds two rows: (1,"Name1") and (2,"Name2")
     * List<Customer> customers = BigQueryExecutor.toList(tableResult, Customer.class);
     * customers.size();                                  // returns 2
     * customers.get(0).getId();                          // returns 1
     *
     * // Map rows: each row becomes a column-to-value map
     * List<Map<String, Object>> maps = BigQueryExecutor.toList(tableResult, Map.class);
     *
     * // Edge: an empty result (getTotalRows() == 0) yields an empty list
     * BigQueryExecutor.toList(emptyResult, Customer.class).isEmpty();        // returns true
     *
     * // Edge: a DML TableResult (null schema, getTotalRows() > 0) also yields an empty list
     * BigQueryExecutor.toList(dmlResult, Customer.class).isEmpty();          // returns true
     * }</pre>
     *
     * @param <T> the element type of the returned list
     * @param tableResult the BigQuery query result to materialise
     * @param targetClass selects the per-row mapping strategy as described above; may be
     *                    {@code null} to fall back to {@code Object[]} rows
     * @return a {@link List} containing one element per data row, sized to
     *         {@link TableResult#getTotalRows()}; an empty list if the result has no rows or no
     *         schema (e.g. a DML statement)
     * @throws IllegalArgumentException if {@code targetClass} is incompatible with the row width
     *                                  (see scalar case above)
     * @see #toEntity(FieldValueList, Class)
     * @see #stream(Class, String, Object...)
     */
    public static <T> List<T> toList(final TableResult tableResult, final Class<T> targetClass) {
        final Schema schema = tableResult.getSchema();

        // A null schema (e.g. a DML statement's TableResult) has no result columns regardless of the
        // affected-row count — checked before toIntExact, which would overflow on a DML statement that
        // touched more than Integer.MAX_VALUE rows.
        if (schema == null) {
            return new ArrayList<>(0);
        }

        final int rowCount = Numbers.toIntExact(tableResult.getTotalRows());

        if (rowCount == 0) {
            return new ArrayList<>(0);
        }

        final FieldList fields = schema.getFields();
        final Iterable<FieldValueList> rows = tableResult.iterateAll();

        final List<T> resultList = new ArrayList<>(rowCount);
        final Function<? super FieldValueList, ? extends T> mapper = createRowMapper(targetClass, fields);

        for (final FieldValueList row : rows) {
            resultList.add(mapper.apply(row));
        }

        return resultList;
    }

    /**
     * Materialises a BigQuery {@link TableResult} into a columnar {@link Dataset}.
     * <p>
     * The result is column-oriented: each output column holds every value for that field across
     * all rows. {@code targetClass} drives per-column type conversion:
     * <ul>
     *   <li>If {@code targetClass} is an entity bean, each output column is converted to the
     *       declared type of the matching bean property (resolved via the column-to-property name
     *       map). Columns without a matching property are left unconverted.</li>
     *   <li>If {@code targetClass} is assignable to {@link Map}, nested BigQuery {@code STRUCT}
     *       values are converted to {@link Map}s; primitive values are left as-is.</li>
     *   <li>For any other non-{@code null} {@code targetClass} (and for {@code null}), nested
     *       {@code STRUCT} values are converted to {@code Object[]} and primitive values are left
     *       as-is.</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // tableResult holds one row: (1,"Name1") with columns id, name
     * Dataset ds = BigQueryExecutor.extractData(tableResult, Customer.class);
     * ds.columnCount();                                  // returns 2
     * ds.size();                                         // returns 1 (row count)
     * ds.columnNames().contains("name");                 // returns true
     *
     * // Map hint keeps nested STRUCTs as Maps
     * Dataset asMaps = BigQueryExecutor.extractData(tableResult, Map.class);
     *
     * // Edge: a DML TableResult (null schema) yields an empty Dataset
     * Dataset empty = BigQueryExecutor.extractData(dmlResult, Customer.class);
     * empty.size();                                      // returns 0
     * empty.columnCount();                               // returns 0
     * }</pre>
     *
     * @param tableResult the BigQuery query result to materialise
     * @param targetClass type hint used to choose per-column conversion as described above; may be
     *                    {@code null}
     * @return a {@link RowDataset} backed by per-column lists, or {@link N#newEmptyDataset()} if
     *         {@code tableResult} has no schema (e.g. it came from a DML statement)
     * @see RowDataset
     * @see #query(Class, String, Object...)
     */
    @SuppressWarnings({ "null", "deprecation" })
    public static Dataset extractData(final TableResult tableResult, final Class<?> targetClass) {
        final Schema schema = tableResult.getSchema();

        // Null schema (DML statement) checked before toIntExact: getTotalRows() is the affected-row
        // count there and could overflow the int conversion.
        if (schema == null) {
            return N.newEmptyDataset();
        }

        final int rowCount = Numbers.toIntExact(tableResult.getTotalRows());

        final FieldList fields = schema.getFields();
        final Iterable<FieldValueList> rows = tableResult.iterateAll();
        final int fieldCount = fields.size();

        final List<String> columnNameList = new ArrayList<>(fieldCount);
        final List<List<Object>> columnList = new ArrayList<>(fieldCount);
        final Class<?>[] columnClasses = new Class<?>[fieldCount];
        final boolean isEntity = Beans.isBeanClass(targetClass);
        final boolean isMap = targetClass != null && Map.class.isAssignableFrom(targetClass);
        final Map<String, String> column2FieldNameMap = isEntity ? QueryUtil.getColumn2PropNameMap(targetClass) : null;
        final BeanInfo entityInfo = isEntity ? ParserUtil.getBeanInfo(targetClass) : null;
        String columnName = null;
        String propName = null;
        PropInfo propInfo = null;

        for (int i = 0; i < fieldCount; i++) {
            columnName = fields.get(i).getName();
            columnNameList.add(columnName);
            columnList.add(new ArrayList<>(rowCount));

            if (isEntity) {
                propInfo = entityInfo.getPropInfo(columnName);

                if (propInfo == null && (propName = column2FieldNameMap.get(columnName)) != null) {
                    propInfo = entityInfo.getPropInfo(propName);
                }

                columnClasses[i] = propInfo == null ? null : propInfo.clazz;
            } else {
                // null = no per-column conversion (raw values), per the documented contract for non-bean
                // targets; nested STRUCTs still flatten to Object[] through the readRow branch below.
                // Object[].class here would force every scalar through N.convert(value, Object[].class).
                columnClasses[i] = isMap ? Map.class : null;
            }
        }

        Object value = null;

        for (final FieldValueList row : rows) {
            for (int i = 0; i < fieldCount; i++) {
                value = row.get(i).getValue();

                if (value instanceof FieldValueList && (columnClasses[i] == null || !columnClasses[i].isAssignableFrom(FieldValueList.class))) {
                    columnList.get(i).add(readRow((FieldValueList) value, columnClasses[i]));
                } else if (value instanceof List && !(value instanceof FieldValueList)) {
                    // REPEATED column: unwrap the FieldValue wrapper elements (mirrors the toMap path).
                    columnList.get(i).add(unwrapRepeatedValue(fields.get(i), (List<?>) value));
                } else if (value == null || targetClass == null || isMap || columnClasses[i] == null || columnClasses[i].isAssignableFrom(value.getClass())) {
                    columnList.get(i).add(value);
                } else {
                    columnList.get(i).add(N.convert(value, columnClasses[i]));
                }
            }
        }

        return new RowDataset(columnNameList, columnList);
    }

    /**
     * Inserts a Java entity into the corresponding BigQuery table.
     * <p>
     * This method converts the entity to the appropriate BigQuery format and executes
     * an INSERT statement. The table name is derived from the entity class name using
     * the configured naming policy. All non-null properties of the entity will be
     * included in the INSERT statement.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Define entity class
     * public class Customer {
     *     private String customerId;
     *     private String name;
     *     private String email;
     *     // getters and setters...
     * }
     *
     * // Insert entity
     * Customer customer = new Customer();
     * customer.setCustomerId("CUST123");
     * customer.setName("John Doe");
     * customer.setEmail("john@example.com");
     *
     * TableResult result = executor.insert(customer);   // returns a TableResult; result.getTotalRows() is the affected-row count
     *
     * // Edge: a null entity is rejected before any query is built
     * executor.insert((Object) null);                   // throws IllegalArgumentException
     * }</pre>
     *
     * @param entity the entity instance to insert; the table name is resolved from its class via the configured naming policy
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if entity is null
     * @see #insert(Class, Map)
     */
    public TableResult insert(final Object entity) {
        N.checkArgNotNull(entity, "entity");

        final SP sp = prepareInsert(entity);

        return execute(sp);
    }

    private SP prepareInsert(final Object entity) {
        final Class<?> targetClass = entity.getClass();

        switch (namingPolicy) {
            case SNAKE_CASE:
                return PSC.insert(entity).into(targetClass).build();

            case SCREAMING_SNAKE_CASE:
                return PAC.insert(entity).into(targetClass).build();

            case CAMEL_CASE:
                return PLC.insert(entity).into(targetClass).build();

            default:
                throw new IllegalStateException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Inserts data into a BigQuery table using property-value mappings.
     * <p>
     * This method creates and executes an INSERT statement using the provided properties map.
     * The table name is derived from the target class name using the configured naming policy.
     * Column names are determined by the map keys, and values are properly converted to BigQuery types.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Insert using a Map
     * Map<String, Object> customerData = new HashMap<>();
     * customerData.put("customerId", "CUST456");
     * customerData.put("name", "Jane Smith");
     * customerData.put("email", "jane@example.com");
     * customerData.put("createdDate", LocalDate.now());
     *
     * TableResult result = executor.insert(Customer.class, customerData);   // returns a TableResult; getTotalRows() is the affected-row count
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name resolution)
     * @param props a Map containing column names as keys and values to insert
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if props is null or empty
     * @see #insert(Object)
     */
    public TableResult insert(final Class<?> targetClass, final Map<String, Object> props) {
        N.checkArgument(N.notEmpty(props), "props cannot be null or empty");

        final SP sp = prepareInsert(targetClass, props);

        return execute(sp);
    }

    private SP prepareInsert(final Class<?> targetClass, final Map<String, Object> props) {
        switch (namingPolicy) {
            case SNAKE_CASE:
                return PSC.insert(props).into(targetClass).build();

            case SCREAMING_SNAKE_CASE:
                return PAC.insert(props).into(targetClass).build();

            case CAMEL_CASE:
                return PLC.insert(props).into(targetClass).build();

            default:
                throw new IllegalStateException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Updates a BigQuery table record using an entity's primary key fields.
     * <p>
     * This method performs an UPDATE operation using the entity's primary key fields (identified
     * by @Id annotations or naming conventions) as the WHERE clause criteria. All non-key
     * properties of the entity are included in the SET clause.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Entity with @Id annotation
     * public class Customer {
     *     @Id
     *     private String customerId;
     *     private String name;
     *     private String email;
     *     // getters and setters...
     * }
     *
     * // Load and update entity
     * Customer customer = executor.list(Customer.class,
     *     "SELECT * FROM customers WHERE customer_id = ?", "CUST123").get(0);
     * customer.setEmail("newemail@example.com");
     *
     * TableResult result = executor.update(customer);   // returns a TableResult; getTotalRows() is the affected-row count
     *
     * // Edge: a null entity is rejected before any query is built
     * executor.update((Object) null);                   // throws IllegalArgumentException
     * }</pre>
     *
     * @param entity the entity instance containing updated values and primary key values
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if entity is null, or if no primary key fields are found for the entity class
     * @see #update(Object, Set)
     */
    public TableResult update(final Object entity) {
        N.checkArgNotNull(entity, "entity");

        return update(entity, getKeyNameSet(entity.getClass()));
    }

    /**
     * Updates a BigQuery table record using specified primary key fields.
     * <p>
     * This method performs an UPDATE operation using the specified primary key field names
     * as the WHERE clause criteria. All properties except the primary keys are included
     * in the SET clause with values from the entity.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Entity with composite key
     * public class OrderItem {
     *     private String orderId;
     *     private String itemId;
     *     private int quantity;
     *     private BigDecimal price;
     *     // getters and setters...
     * }
     *
     * // Update with custom key fields
     * OrderItem item = new OrderItem();
     * item.setOrderId("ORD123");
     * item.setItemId("ITEM456");
     * item.setQuantity(5);
     * item.setPrice(new BigDecimal("99.99"));
     *
     * Set<String> keyFields = N.asSet("orderId", "itemId");
     * TableResult result = executor.update(item, keyFields);   // returns a TableResult; getTotalRows() is the affected-row count
     *
     * // Edge: an empty (or null) key set is rejected
     * executor.update(item, N.<String> asSet());               // throws IllegalArgumentException
     * }</pre>
     *
     * @param entity the entity instance containing updated values and primary key values
     * @param primaryKeyNames the set of property names to use as primary key fields in the WHERE clause
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if entity is null, or if primaryKeyNames is null or empty
     * @see #update(Class, Map, Condition)
     */
    public TableResult update(final Object entity, final Set<String> primaryKeyNames) {
        N.checkArgNotNull(entity, "entity");

        final SP sp = prepareUpdate(entity, primaryKeyNames);

        return execute(sp);
    }

    private SP prepareUpdate(final Object entity, final Set<String> primaryKeyNames) {
        N.checkArgument(N.notEmpty(primaryKeyNames), "primaryKeyNames cannot be null or empty");

        final Class<?> targetClass = entity.getClass();
        final BeanInfo entityInfo = ParserUtil.getBeanInfo(targetClass);
        final List<Condition> conds = new ArrayList<>(primaryKeyNames.size());

        for (final String keyName : primaryKeyNames) {
            conds.add(Filters.eq(keyName, entityInfo.getPropValue(entity, keyName)));
        }

        switch (namingPolicy) {
            case SNAKE_CASE:
                return PSC.update(targetClass).setEntity(entity, primaryKeyNames).where(Filters.and(conds)).build();

            case SCREAMING_SNAKE_CASE:
                return PAC.update(targetClass).setEntity(entity, primaryKeyNames).where(Filters.and(conds)).build();

            case CAMEL_CASE:
                return PLC.update(targetClass).setEntity(entity, primaryKeyNames).where(Filters.and(conds)).build();

            default:
                throw new IllegalStateException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Updates BigQuery table records using property-value mappings and a condition.
     * <p>
     * This method creates and executes an UPDATE statement using the provided properties map
     * for the SET clause and the specified condition for the WHERE clause. This allows for
     * flexible updates based on any criteria.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Update multiple records with a condition
     * Map<String, Object> updates = new HashMap<>();
     * updates.put("status", "archived");
     * updates.put("archivedDate", LocalDate.now());
     *
     * Condition condition = Filters.and(
     *     Filters.eq("status", "inactive"),
     *     Filters.lt("lastAccessDate", LocalDate.now().minusYears(1))
     * );
     *
     * TableResult result = executor.update(Customer.class, updates, condition);   // returns a TableResult; getTotalRows() is the affected-row count
     *
     * // Edge: empty (or null) props are rejected
     * executor.update(Customer.class, new HashMap<>(), condition);                // throws IllegalArgumentException
     *
     * // Edge: a null whereClause is rejected by the SQL builder (it does NOT update all rows)
     * executor.update(Customer.class, updates, (Condition) null);                 // throws IllegalArgumentException
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name resolution)
     * @param props a Map containing column names as keys and new values to set
     * @param whereClause the condition specifying which records to update; must not be {@code null}
     *                    (a {@code null} condition is rejected by the SQL builder with
     *                    {@link IllegalArgumentException})
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if props is null or empty, or if whereClause is {@code null}
     * @see com.landawn.abacus.query.Filters
     */
    public TableResult update(final Class<?> targetClass, final Map<String, Object> props, final Condition whereClause) {
        N.checkArgument(N.notEmpty(props), "props cannot be null or empty");

        final SP sp = prepareUpdate(targetClass, props, whereClause);

        return execute(sp);
    }

    private SP prepareUpdate(final Class<?> targetClass, final Map<String, Object> props, final Condition whereClause) {
        switch (namingPolicy) {
            case SNAKE_CASE:
                return PSC.update(targetClass).set(props).where(whereClause).build();

            case SCREAMING_SNAKE_CASE:
                return PAC.update(targetClass).set(props).where(whereClause).build();

            case CAMEL_CASE:
                return PLC.update(targetClass).set(props).where(whereClause).build();

            default:
                throw new IllegalStateException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Deletes a BigQuery table record using an entity's primary key fields.
     * <p>
     * This method performs a DELETE operation using the entity's primary key fields as the
     * WHERE clause criteria. The entity must have valid primary key values set.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Entity with @Id annotation
     * Customer customer = new Customer();
     * customer.setCustomerId("CUST123");
     *
     * TableResult result = executor.delete(customer);   // returns a TableResult; getTotalRows() is the affected-row count
     *
     * // Or delete by loading first
     * Customer existing = executor.list(Customer.class,
     *     "SELECT * FROM customers WHERE customer_id = ?", "CUST456").get(0);
     * executor.delete(existing);                         // returns a TableResult
     *
     * // Edge: a null entity is rejected before any query is built
     * executor.delete((Object) null);                    // throws IllegalArgumentException
     *
     * // Edge: an entity whose only key is blank/null has no usable WHERE value
     * Customer blank = new Customer();                   // customerId left null
     * executor.delete(blank);                            // throws IllegalArgumentException
     * }</pre>
     *
     * @param entity the entity instance containing primary key values for deletion
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if entity is null, if no primary key fields are defined, or if no key value is set on the entity
     * @see #delete(Class, Object...)
     */
    public TableResult delete(final Object entity) {
        N.checkArgNotNull(entity, "entity");

        return delete(entity.getClass(), entityToCondition(entity));
    }

    /**
     * Deletes BigQuery table records using primary key values.
     * <p>
     * This method performs a DELETE operation using the provided ID values as criteria.
     * The IDs are matched against the primary key fields of the target class in order.
     * For single primary key tables, provide one ID; for composite keys, provide values
     * in the same order as the key fields.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete by single primary key
     * TableResult result = executor.delete(Customer.class, "CUST123");   // returns a TableResult; getTotalRows() is the affected-row count
     *
     * // Delete by composite key (values in key-field order)
     * TableResult result2 = executor.delete(OrderItem.class, "ORD123", "ITEM456");   // returns a TableResult
     *
     * // Delete multiple records one by one
     * List<String> customerIds = Arrays.asList("CUST001", "CUST002", "CUST003");
     * for (String id : customerIds) {
     *     executor.delete(Customer.class, id);
     * }
     *
     * // Edge: an empty id array is rejected
     * executor.delete(Customer.class, new Object[0]);                    // throws IllegalArgumentException
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name and key resolution)
     * @param ids the primary key values to identify records for deletion
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if ids is null or empty,
     *                                  or the number of IDs doesn't match the primary key structure
     * @throws NullPointerException if targetClass is null
     * @see #delete(Class, Condition)
     */
    public final TableResult delete(final Class<?> targetClass, final Object... ids) {
        return delete(targetClass, idsToCondition(targetClass, ids));
    }

    /**
     * Deletes BigQuery table records using a custom condition.
     * <p>
     * This method creates and executes a DELETE statement using the specified condition
     * for the WHERE clause. This allows for flexible deletion based on any criteria.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete with simple condition
     * Condition condition = Filters.eq("status", "inactive");
     * TableResult result = executor.delete(Customer.class, condition);   // returns a TableResult; getTotalRows() is the affected-row count
     *
     * // Delete with complex condition
     * Condition complexCondition = Filters.and(
     *     Filters.eq("status", "pending"),
     *     Filters.lt("createdDate", LocalDate.now().minusDays(30)),
     *     Filters.isNull("processedDate")
     * );
     * TableResult result2 = executor.delete(Order.class, complexCondition);   // returns a TableResult
     *
     * // Edge: a null whereClause is rejected by the SQL builder (it does NOT delete all rows)
     * executor.delete(Customer.class, (Condition) null);                 // throws IllegalArgumentException
     * }</pre>
     *
     * @param targetClass the target class representing the table (used for table name resolution)
     * @param whereClause the condition specifying which records to delete; must not be {@code null}
     *                    (a {@code null} condition is rejected by the SQL builder with
     *                    {@link IllegalArgumentException})
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if targetClass is null, or if whereClause is {@code null}
     * @see com.landawn.abacus.query.Filters
     */
    public TableResult delete(final Class<?> targetClass, final Condition whereClause) {
        final SP sp = prepareDelete(targetClass, whereClause);

        return execute(sp);
    }

    private SP prepareDelete(final Class<?> targetClass, final Condition whereClause) {
        switch (namingPolicy) {
            case SNAKE_CASE:
                return PSC.deleteFrom(targetClass).where(whereClause).build();

            case SCREAMING_SNAKE_CASE:
                return PAC.deleteFrom(targetClass).where(whereClause).build();

            case CAMEL_CASE:
                return PLC.deleteFrom(targetClass).where(whereClause).build();

            default:
                throw new IllegalStateException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Checks if a record exists in a BigQuery table using primary key values.
     * <p>
     * This method performs a SELECT query to determine if any records exist with the
     * specified primary key values. For single primary key tables, provide one ID;
     * for composite keys, provide values in the same order as the key fields.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Check existence by single primary key
     * boolean exists = executor.exists(Customer.class, "CUST123");   // returns true if a matching row exists
     *
     * // Check existence by composite key (values in key-field order)
     * boolean itemExists = executor.exists(OrderItem.class, "ORD123", "ITEM456");   // returns true if a matching row exists
     *
     * // Conditional logic based on existence
     * if (!executor.exists(Customer.class, customerId)) {
     *     executor.insert(newCustomer);
     * } else {
     *     executor.update(existingCustomer);
     * }
     *
     * // Edge: an empty id array is rejected
     * executor.exists(Customer.class, new Object[0]);                // throws IllegalArgumentException
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name and key resolution)
     * @param ids the primary key values to check for existence
     * @return {@code true} if a record exists with the specified key values, {@code false} otherwise
     * @throws IllegalArgumentException if ids is null or empty,
     *                                  or the number of IDs doesn't match the primary key structure
     * @throws NullPointerException if targetClass is null
     * @see #exists(Class, Condition)
     */
    public final boolean exists(final Class<?> targetClass, final Object... ids) {
        return exists(targetClass, idsToCondition(targetClass, ids));
    }

    private static final Map<Class<?>, Tuple2<ImmutableList<String>, ImmutableSet<String>>> entityKeyNamesMap = new ConcurrentHashMap<>();

    private static ImmutableList<String> getKeyNames(final Class<?> entityClass) {
        Tuple2<ImmutableList<String>, ImmutableSet<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            final List<String> idPropNames = QueryUtil.getIdPropNames(entityClass);
            tp = Tuple.of(ImmutableList.copyOf(idPropNames), ImmutableSet.copyOf(idPropNames));
            entityKeyNamesMap.put(entityClass, tp);
        }

        return tp._1;
    }

    private static Set<String> getKeyNameSet(final Class<?> entityClass) {
        Tuple2<ImmutableList<String>, ImmutableSet<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            final List<String> idPropNames = QueryUtil.getIdPropNames(entityClass);
            tp = Tuple.of(ImmutableList.copyOf(idPropNames), ImmutableSet.copyOf(idPropNames));
            entityKeyNamesMap.put(entityClass, tp);
        }

        return tp._2;
    }

    /**
     * Converts primary key ID values into a Condition for WHERE clauses.
     *
     * <p>This utility method creates appropriate conditions based on the primary key structure
     * of the target class. For single primary key tables, it creates a simple equality condition.
     * For composite primary keys, it creates an AND condition with multiple equality checks.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition condition = BigQueryExecutor.idsToCondition(Customer.class, "CUST123");
     * executor.delete(Customer.class, condition);
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for key name resolution), must not be {@code null}
     * @param ids the primary key values in order corresponding to the key fields, must not be {@code null} or empty
     * @return a Condition suitable for WHERE clauses
     * @throws IllegalArgumentException if ids is null or empty, or if the number of IDs
     *                                  doesn't match the primary key structure
     * @see #entityToCondition(Object)
     */
    static Condition idsToCondition(final Class<?> targetClass, final Object... ids) {
        N.checkArgNotEmpty(ids, "ids");

        final ImmutableList<String> keyNames = getKeyNames(targetClass);

        if (ids.length != keyNames.size()) {
            throw new IllegalArgumentException("ID count mismatch: provided " + ids.length + " IDs but expected "
                    + (N.isEmpty(keyNames) ? "1 key [id]" : keyNames.size() + " keys " + N.toString(keyNames)) + " for class "
                    + ClassUtil.getCanonicalClassName(targetClass));
        }

        if (keyNames.size() == 1) {
            return Filters.eq(keyNames.get(0), ids[0]);
        }

        final List<Condition> conds = new ArrayList<>();
        final Iterator<String> iter = keyNames.iterator();

        for (final Object id : ids) {
            conds.add(Filters.eq(iter.next(), id));
        }

        return Filters.and(conds);
    }

    /**
     * Converts an entity's primary key values into a Condition for WHERE clauses.
     *
     * <p>This utility method extracts primary key values from an entity and creates appropriate
     * conditions for database operations. For single primary key entities, it creates a simple
     * equality condition. For composite primary keys, it creates an AND condition with
     * equality checks for each non-null/non-empty key field.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Customer customer = new Customer("CUST123", "Electronics");
     * Condition condition = BigQueryExecutor.entityToCondition(customer);
     * }</pre>
     *
     * @param entity the entity instance containing primary key values, must not be {@code null}
     * @return a Condition suitable for WHERE clauses based on the entity's key values
     * @throws IllegalArgumentException if entity is null or no valid primary key values are found
     * @see #idsToCondition(Class, Object...)
     */
    static Condition entityToCondition(final Object entity) {
        N.checkArgNotNull(entity, "entity");

        final Class<?> targetClass = entity.getClass();
        final ImmutableList<String> keyNames = getKeyNames(targetClass);

        if (keyNames.isEmpty()) {
            throw new IllegalArgumentException("No key names defined for entity class: " + targetClass.getSimpleName());
        }

        if (keyNames.size() == 1) {
            final Object propVal = Beans.getPropValue(entity, keyNames.get(0));

            if (propVal == null || (propVal instanceof CharSequence) && Strings.isEmpty(((CharSequence) propVal))) {
                throw new IllegalArgumentException("No property value specified in entity for key names: " + keyNames);
            }

            return Filters.eq(keyNames.get(0), propVal);
        } else {
            final List<Condition> conds = new ArrayList<>(keyNames.size());
            Object propVal = null;

            for (final String keyName : keyNames) {
                propVal = Beans.getPropValue(entity, keyName);

                if (propVal == null || (propVal instanceof CharSequence) && Strings.isEmpty(((CharSequence) propVal))) {
                    throw new IllegalArgumentException("No property value specified in entity for key names: " + keyNames);
                }

                conds.add(Filters.eq(keyName, propVal));
            }

            if (N.isEmpty(conds)) {
                throw new IllegalArgumentException("No property value specified in entity for key names: " + keyNames);
            }

            return Filters.and(conds);
        }
    }

    /**
     * Returns whether the table for {@code targetClass} contains at least one row matching
     * {@code whereClause}.
     * <p>
     * Implemented as a parameterised {@code SELECT <keys> FROM <table> WHERE ... LIMIT 1} query
     * job; the primary-key columns of {@code targetClass} (or all columns, if it declares no key) are
     * projected to keep bytes processed small. Note that even with {@code LIMIT 1} BigQuery still <i>scans</i> the qualifying
     * data before applying the limit, so this is not free on large unpartitioned tables.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Check if any active customers exist
     * Condition condition = Filters.eq("status", "active");
     * boolean hasActiveCustomers = executor.exists(Customer.class, condition);   // returns true if at least one row matches
     *
     * // Check for records matching complex criteria
     * Condition complexCond = Filters.and(
     *     Filters.gt("orderAmount", new BigDecimal("1000")),
     *     Filters.eq("paymentStatus", "pending"),
     *     Filters.between("orderDate", startDate, endDate)
     * );
     * boolean hasPendingLargeOrders = executor.exists(Order.class, complexCond);   // returns true if at least one row matches
     *
     * // A null whereClause checks whether the table has any rows at all
     * boolean tableHasRows = executor.exists(Customer.class, (Condition) null);    // returns true if the table is non-empty
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name resolution)
     * @param whereClause the condition to check for matching records, or {@code null} to check whether the table has any rows
     * @return {@code true} if at least one record matches the condition, {@code false} otherwise
     * @throws IllegalArgumentException if targetClass is null
     * @see com.landawn.abacus.query.Filters
     */
    public boolean exists(final Class<?> targetClass, final Condition whereClause) {
        final ImmutableList<String> keyNames = getKeyNames(targetClass);
        final SP sp = prepareQuery(targetClass, keyNames, whereClause, 1);
        final TableResult resultSet = execute(sp);

        return resultSet.getTotalRows() > 0;
    }

    /**
     * Builds a SELECT SQL query against the BigQuery table mapped to {@code targetClass} for the single
     * column named by {@code propName}, applies {@code whereClause}, executes it, and returns the value
     * of that column from the first row converted to {@code valueClass}.
     *
     * <p>Only the first column of the first row of the {@link TableResult} is read; any remaining rows
     * or columns are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL} in BigQuery, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}). {@link Nullable} preserves
     * the distinction between "no row matched" and "row matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition condition = Filters.eq("customerId", "CUST123");
     * Nullable<String> email = executor.queryForSingleValue(
     *         Customer.class, String.class, "email", condition);   // present with the email when a row matches
     *
     * email.isPresent();          // returns true when at least one row matched
     * email.orElse("n/a");        // returns the column value, or "n/a" when no row matched
     *
     * // Edge: no matching row -> Nullable.empty()
     * Nullable<String> none = executor.queryForSingleValue(
     *         Customer.class, String.class, "email", Filters.eq("customerId", "MISSING"));
     * none.isPresent();           // returns false
     * }</pre>
     *
     * @param <V> the expected value type
     * @param targetClass the class representing the target BigQuery table
     * @param valueClass the expected class of the returned value, used for type conversion
     * @param propName the property/column name to select
     * @param whereClause the condition to filter records, or {@code null} to omit the WHERE clause
     * @return a <i>present</i> {@code Nullable<V>} holding the column value (possibly {@code null} for
     *         a BigQuery {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when
     *         the query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or otherwise rejected by
     *         the SQL builder
     * @throws RuntimeException if the underlying BigQuery query execution fails or the calling thread
     *         is interrupted
     * @see #queryForSingleValue(Class, String, Object...)
     * @see #execute(String, Object...)
     */
    public <V> Nullable<V> queryForSingleValue(final Class<?> targetClass, final Class<V> valueClass, final String propName, final Condition whereClause) {
        final SP sp = prepareQuery(targetClass, N.asList(propName), whereClause);

        return queryForSingleValue(valueClass, sp.query(), sp.parameters().toArray());
    }

    /**
     * Executes a custom BigQuery SQL query with positional parameters and returns the value of the
     * first column of the first row, converted to {@code valueClass}.
     *
     * <p>Only the first column of the first row of the returned {@link TableResult} is read; any
     * remaining rows or columns are ignored. This method is designed for queries that return a single
     * scalar, such as {@code COUNT(*)}, {@code MAX(column)}, or single-field lookups.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows (including DML statements where BigQuery surfaces an affected-row count
     * but {@code getValues()} is empty). If a row exists but the column is {@code NULL} in BigQuery,
     * the returned {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}).
     * {@link Nullable} preserves the distinction between "no row matched" and "row matched but value
     * is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<Long> count = executor.queryForSingleValue(Long.class,
     *         "SELECT COUNT(*) FROM customers WHERE status = ?", "active");
     * count.orElse(0L);           // returns the COUNT(*) value (e.g. 5L), or 0L if the query returned no rows
     *
     * Nullable<String> name = executor.queryForSingleValue(String.class,
     *         "SELECT name FROM customers WHERE customer_id = ?", "CUST123");
     * name.isPresent();           // returns true when a row matched (even if the column value is null)
     *
     * // Edge: a query with no rows yields Nullable.empty()
     * Nullable<String> none = executor.queryForSingleValue(String.class,
     *         "SELECT name FROM customers WHERE customer_id = ?", "MISSING");
     * none.isPresent();           // returns false
     * }</pre>
     *
     * @param <V> the expected value type
     * @param valueClass the expected class of the returned value, used for type conversion
     * @param query the BigQuery SQL query string with {@code ?} positional parameter placeholders
     * @param parameters the parameter values to bind to the query, in positional order
     * @return a <i>present</i> {@code Nullable<V>} holding the column value converted to
     *         {@code valueClass} (possibly {@code null} for a BigQuery {@code NULL}) when at least one
     *         row is returned; {@code Nullable.empty()} when the query returns no rows
     * @throws IllegalArgumentException if {@code valueClass} or {@code query} is rejected by the
     *         conversion / BigQuery client layer
     * @throws RuntimeException if the underlying BigQuery query execution fails or the calling thread
     *         is interrupted
     * @see #queryForSingleValue(Class, Class, String, Condition)
     * @see #execute(String, Object...)
     */
    public final <V> Nullable<V> queryForSingleValue(final Class<V> valueClass, final String query, final Object... parameters) {
        final TableResult tableResult = execute(query, parameters);
        // For DML statements BigQuery reports the affected-row count via getTotalRows() while
        // getValues() is empty. Drive off the iterator rather than the row count so we don't
        // throw NoSuchElementException in that case.
        if (tableResult.getTotalRows() <= 0) {
            return (Nullable<V>) Nullable.empty();
        }

        final Iterator<FieldValueList> iter = tableResult.getValues().iterator();
        if (!iter.hasNext()) {
            return (Nullable<V>) Nullable.empty();
        }

        final FieldValueList row = iter.next();
        return Nullable.of(N.convert(row.get(0).getValue(), valueClass));
    }

    /**
     * Queries a BigQuery table and returns results as a Dataset using a condition.
     * <p>
     * This method performs a SELECT * query on the table corresponding to the target class
     * and returns the results in a columnar Dataset format. All columns are selected by default.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query for dataset with condition
     * Condition condition = Filters.eq("status", "active");
     * Dataset dataset = executor.query(Customer.class, condition);   // returns a columnar Dataset of matching rows
     * dataset.size();                                                // returns the number of matching rows
     *
     * // Access columns by name
     * ImmutableList<String> names = dataset.getColumn("name");
     * ImmutableList<String> emails = dataset.getColumn("email");
     *
     * // A null condition selects all rows (the read path omits the WHERE clause)
     * Dataset all = executor.query(Customer.class, (Condition) null);   // returns every row in the table
     * }</pre>
     *
     * @param targetClass the class representing the target table
     * @param whereClause the condition to filter records, or {@code null} to select all rows
     * @return a Dataset containing all matching records in columnar format
     * @throws IllegalArgumentException if targetClass is null
     * @see #query(Class, Collection, Condition)
     * @see Dataset
     */
    public Dataset query(final Class<?> targetClass, final Condition whereClause) {
        return query(targetClass, null, whereClause);
    }

    /**
     * Queries specific columns from a BigQuery table and returns results as a Dataset.
     * <p>
     * This method performs a SELECT query on specified columns of the table corresponding
     * to the target class and returns the results in a columnar Dataset format. If
     * selectPropNames is null or empty, all columns are selected.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query specific columns
     * Collection<String> columns = Arrays.asList("customerId", "name", "email");
     * Condition condition = Filters.eq("status", "active");
     * Dataset dataset = executor.query(Customer.class, columns, condition);   // returns a Dataset with exactly those 3 columns
     * dataset.columnCount();                                                  // returns 3
     * dataset.size();                                                         // returns the number of matching rows
     *
     * // Access specific columns by name
     * ImmutableList<String> customerIds = dataset.getColumn("customerId");
     * ImmutableList<String> names = dataset.getColumn("name");
     *
     * // null selectPropNames selects all columns (equivalent to query(Class, Condition))
     * Dataset full = executor.query(Customer.class, (Collection<String>) null, condition);
     * }</pre>
     *
     * @param targetClass the class representing the target table
     * @param selectPropNames the collection of property/column names to select, or null for all columns
     * @param whereClause the condition to filter records, or {@code null} to select all rows
     * @return a Dataset containing matching records for the selected columns in columnar format
     * @throws IllegalArgumentException if targetClass is null
     * @see #query(Class, String, Object...)
     */
    public Dataset query(final Class<?> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP sp = prepareQuery(targetClass, selectPropNames, whereClause);

        return query(targetClass, sp.query(), sp.parameters().toArray());
    }

    /**
     * Executes a custom BigQuery SQL query and returns results as a Dataset.
     * <p>
     * This method executes the provided SQL query with parameter binding and returns
     * the results in a columnar Dataset format. The target class is used to determine
     * appropriate column types and conversion strategies.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute custom SQL query
     * String sql = "SELECT customer_id, name, email FROM customers " +
     *              "WHERE status = ? AND created_date > ?";
     * Dataset dataset = executor.query(Customer.class, sql,
     *     "active", java.sql.Date.valueOf(LocalDate.now().minusMonths(6)));   // java.sql.Date binds as a native DATE parameter
     *
     * // Process results
     * dataset.size();                                  // returns the number of rows
     * dataset.getColumn("name").forEach(System.out::println);
     *
     * // Complex query with aggregation
     * String aggQuery = "SELECT status, COUNT(*) as count, AVG(order_amount) as avg_amount " +
     *                   "FROM orders WHERE order_date >= ? GROUP BY status";
     * Dataset aggDataset = executor.query(Map.class, aggQuery,
     *     java.sql.Date.valueOf(LocalDate.now().minusMonths(1)));   // returns a Dataset
     * }</pre>
     *
     * @param targetClass the target class for result conversion (entity class with getter/setter methods or Map.class)
     *                   used for type information and conversion
     * @param query the SQL query string with ? parameter placeholders
     * @param parameters the parameter values to bind to the query
     * @return a Dataset containing query results in columnar format
     * @throws NullPointerException if query is null
     * @see #execute(String, Object...)
     * @see Dataset
     */
    public final Dataset query(final Class<?> targetClass, final String query, final Object... parameters) {
        return extractData(execute(query, parameters), targetClass);
    }

    /**
     * Queries a BigQuery table and returns results as a List using a condition.
     * <p>
     * This method performs a SELECT * query on the table corresponding to the target class
     * and returns the results as a List of objects. All columns are selected by default.
     * The target class determines the format of returned objects.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // List all active customers
     * Condition condition = Filters.eq("status", "active");
     * List<Customer> customers = executor.list(Customer.class, condition);   // returns a List of matching entities (empty if none)
     *
     * customers.forEach(customer -> {
     *     System.out.println(customer.getName() + ": " + customer.getEmail());
     * });
     *
     * // List with complex condition
     * Condition complexCond = Filters.and(
     *     Filters.eq("status", "active"),
     *     Filters.gt("orderCount", 10),
     *     Filters.between("lastOrderDate", startDate, endDate)
     * );
     * List<Customer> vipCustomers = executor.list(Customer.class, complexCond);   // returns a List of matching entities
     *
     * // A null condition selects all rows
     * List<Customer> everyone = executor.list(Customer.class, (Condition) null);  // returns every row in the table
     * }</pre>
     *
     * @param <T> the target type for list elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   Object[].class, Collection classes, or supported basic types)
     * @param whereClause the condition to filter records, or {@code null} to select all rows
     * @return a List containing all matching records converted to the target type, empty list if no matches
     * @throws IllegalArgumentException if targetClass is null
     * @see com.landawn.abacus.query.Filters
     * @see #list(Class, Collection, Condition)
     */
    public <T> List<T> list(final Class<T> targetClass, final Condition whereClause) {
        return list(targetClass, null, whereClause);
    }

    /**
     * Queries specific columns from a BigQuery table and returns results as a List.
     * <p>
     * This method performs a SELECT query on specified columns of the table corresponding
     * to the target class and returns the results as a List of objects. If selectPropNames
     * is null or empty, all columns are selected.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Select specific columns
     * Collection<String> columns = Arrays.asList("customerId", "name", "email");
     * Condition condition = Filters.eq("status", "active");
     * List<Customer> customers = executor.list(Customer.class, columns, condition);   // returns a List of entities populated from the 3 columns
     *
     * // Select to Map for flexibility
     * List<Map<String, Object>> results = executor.list(Map.class, columns, condition);   // returns one Map per row
     * results.forEach(row -> {
     *     System.out.println(row.get("customerId") + ": " + row.get("name"));
     * });
     *
     * // Select a single column into scalar values
     * Collection<String> idColumn = Arrays.asList("customerId");
     * List<String> customerIds = executor.list(String.class, idColumn, condition);   // returns one String per row
     * }</pre>
     *
     * @param <T> the target type for list elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   Object[].class, Collection classes, or basic single value types)
     * @param selectPropNames the collection of property/column names to select, or null for all columns
     * @param whereClause the condition to filter records, or {@code null} to select all rows
     * @return a List containing matching records for the selected columns converted to the target type
     * @throws IllegalArgumentException if targetClass is null
     * @see com.landawn.abacus.query.Filters
     * @see #list(Class, String, Object...)
     */
    public <T> List<T> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP sp = prepareQuery(targetClass, selectPropNames, whereClause);

        return list(targetClass, sp.query(), sp.parameters().toArray());
    }

    /**
     * Executes a custom BigQuery SQL query and returns results as a List.
     * <p>
     * This method executes the provided SQL query with parameter binding and returns
     * the results as a List of objects of the specified target type. Each row is
     * converted according to the target class specifications.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute SQL query with parameters
     * String sql = "SELECT * FROM customers WHERE status = ? AND created_date > ?";
     * List<Customer> customers = executor.list(Customer.class, sql,
     *     "active", java.sql.Date.valueOf(LocalDate.now().minusMonths(6)));   // java.sql.Date binds as a native DATE parameter
     *
     * // Complex JOIN query
     * String joinSql = "SELECT c.customer_id, c.name, COUNT(o.order_id) as order_count " +
     *                  "FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id " +
     *                  "WHERE c.status = ? GROUP BY c.customer_id, c.name";
     * List<Map<String, Object>> results = executor.list(Map.class, joinSql, "active");   // returns one Map per row
     *
     * // Query returning single values (single-column result -> scalar conversion)
     * String idQuery = "SELECT customer_id FROM customers WHERE status = ?";
     * List<String> activeIds = executor.list(String.class, idQuery, "active");   // returns one String per row
     * }</pre>
     *
     * @param <T> the target type for list elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or supported basic types)
     * @param query the SQL query string with ? parameter placeholders
     * @param parameters the parameter values to bind to the query
     * @return a List containing all query results converted to the target type, empty list if no results
     * @throws NullPointerException if query is null
     * @see #execute(String, Object...)
     * @see #toList(TableResult, Class)
     */
    public final <T> List<T> list(final Class<T> targetClass, final String query, final Object... parameters) {
        return toList(execute(query, parameters), targetClass);
    }

    /**
     * Queries a BigQuery table and returns results as a Stream using a condition.
     * <p>
     * This method performs a SELECT * query on the table corresponding to the target class
     * and returns the results as a Stream of objects. All columns are selected by default.
     * The Stream allows for lazy evaluation and processing of large result sets.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Stream and process records
     * Condition condition = Filters.eq("status", "active");
     * Stream<Customer> customerStream = executor.stream(Customer.class, condition);   // returns a lazy Stream of matching entities
     *
     * customerStream
     *     .filter(c -> c.getOrderCount() > 10)
     *     .map(Customer::getEmail)
     *     .forEach(email -> sendEmail(email));
     *
     * // Stream with aggregation (abacus Stream)
     * BigDecimal totalAmount = executor.stream(Order.class, Filters.eq("status", "completed"))
     *     .map(Order::getAmount)
     *     .reduce(BigDecimal.ZERO, BigDecimal::add);   // returns the summed amount
     *
     * // Stream for large datasets (memory efficient)
     * long count = executor.stream(Customer.class, Filters.between("createdDate", start, end))
     *     .count();                                    // returns the number of matching rows
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or basic single value types)
     * @param whereClause the condition to filter records, or {@code null} to select all rows
     * @return a Stream containing all matching records converted to the target type
     * @throws IllegalArgumentException if targetClass is null
     * @see com.landawn.abacus.query.Filters
     * @see #stream(Class, Collection, Condition)
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Condition whereClause) {
        return stream(targetClass, null, whereClause);
    }

    /**
     * Queries specific columns from a BigQuery table and returns results as a Stream.
     * <p>
     * This method performs a SELECT query on specified columns of the table corresponding
     * to the target class and returns the results as a Stream of objects. If selectPropNames
     * is null or empty, all columns are selected. The Stream allows for lazy evaluation.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Stream specific columns
     * Collection<String> columns = Arrays.asList("customerId", "email");
     * Condition condition = Filters.eq("status", "active");
     *
     * List<String> emails = executor.stream(Customer.class, columns, condition)
     *     .map(Customer::getEmail)
     *     .filter(email -> email.endsWith("@example.com"))
     *     .toList();                                   // returns the matching emails as a List
     *
     * // Stream and group by (abacus Stream collector)
     * Map<String, List<Customer>> byStatus = executor.stream(Customer.class,
     *         Arrays.asList("customerId", "status"), Filters.isNotNull("status"))
     *     .groupTo(Customer::getStatus);              // returns a Map keyed by status
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or basic single value types)
     * @param selectPropNames the collection of property/column names to select, or null for all columns
     * @param whereClause the condition to filter records, or {@code null} to select all rows
     * @return a Stream containing matching records for the selected columns converted to the target type
     * @throws IllegalArgumentException if targetClass is null
     * @see com.landawn.abacus.query.Filters
     * @see #stream(Class, String, Object...)
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP sp = prepareQuery(targetClass, selectPropNames, whereClause);

        return stream(targetClass, sp.query(), sp.parameters().toArray());
    }

    /**
     * Executes a custom BigQuery SQL query and returns results as a Stream.
     * <p>
     * This method executes the provided SQL query with parameter binding and returns
     * the results as a Stream of objects of the specified target type. The Stream allows
     * for lazy evaluation and efficient processing of large result sets.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Stream query results
     * String sql = "SELECT * FROM customers WHERE status = ? AND created_date > ?";
     * Stream<Customer> stream = executor.stream(Customer.class, sql,
     *     "active", java.sql.Date.valueOf(LocalDate.now().minusMonths(6)));   // java.sql.Date binds as a native DATE parameter
     *
     * // Process stream with filtering and mapping (abacus Stream)
     * List<String> premiumEmails = stream
     *     .filter(c -> c.getOrderCount() > 100)
     *     .map(Customer::getEmail)
     *     .toList();                                   // returns the matching emails as a List
     *
     * // Stream for analytics
     * String analyticsSql = "SELECT order_date, SUM(amount) as total FROM orders " +
     *                       "WHERE order_date >= ? GROUP BY order_date";
     * executor.stream(Map.class, analyticsSql, java.sql.Date.valueOf(LocalDate.now().minusDays(30)))
     *     .forEach(row -> System.out.println(row.get("order_date") + ": " + row.get("total")));
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or supported basic types)
     * @param query the SQL query string with ? parameter placeholders
     * @param parameters the parameter values to bind to the query
     * @return a Stream containing all query results converted to the target type
     * @throws NullPointerException if query is null
     * @see #execute(String, Object...)
     * @see Stream
     */
    public final <T> Stream<T> stream(final Class<T> targetClass, final String query, final Object... parameters) {
        final TableResult tableResult = execute(query, parameters);
        // Pass the result schema instead of null: the null path falls back to the reflective
        // FieldValueList.schema accessor, which is unavailable on some library versions.
        final FieldList fields = tableResult.getSchema() == null ? null : tableResult.getSchema().getFields();
        final Function<? super FieldValueList, ? extends T> mapper = createRowMapper(targetClass, fields);

        return Stream.of(tableResult.iterateAll()).map(mapper);
    }

    /**
     * Executes a BigQuery job using the provided configuration and returns results as a Stream.
     * <p>
     * This method provides direct access to BigQuery's QueryJobConfiguration for advanced
     * query scenarios requiring specific job settings like query priority, labels, or
     * custom timeout configurations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Advanced query with custom job configuration
     * QueryJobConfiguration config = QueryJobConfiguration.newBuilder(
     *     "SELECT * FROM customers WHERE status = 'active'")
     *     .setUseLegacySql(false)
     *     .setPriority(QueryJobConfiguration.Priority.INTERACTIVE)
     *     .setMaximumBytesBilled(10000000L)
     *     .setLabels(Map.of("environment", "production"))
     *     .build();
     *
     * Stream<Customer> stream = executor.stream(Customer.class, config);   // returns a lazy Stream of entities
     * long count = stream.count();                                         // returns the number of rows produced by the job
     *
     * // Query with caching disabled
     * QueryJobConfiguration noCacheConfig = QueryJobConfiguration.newBuilder(
     *     "SELECT * FROM realtime_data")
     *     .setUseQueryCache(false)
     *     .build();
     *
     * executor.stream(Map.class, noCacheConfig)
     *     .forEach(row -> processRealtimeData(row));
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or basic single value types)
     * @param queryConfig the BigQuery QueryJobConfiguration with query and job settings
     * @return a Stream containing all query results converted to the target type
     * @throws RuntimeException if the query job fails or the calling thread is interrupted
     * @see QueryJobConfiguration
     * @see #stream(QueryJobConfiguration)
     */
    public final <T> Stream<T> stream(final Class<T> targetClass, final QueryJobConfiguration queryConfig) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing query: {}", queryConfig.getQuery());
        }

        try {
            final TableResult tableResult = bigQuery.query(queryConfig);
            // Pass the result schema instead of null: the null path falls back to the reflective
            // FieldValueList.schema accessor, which is unavailable on some library versions.
            final FieldList fields = tableResult.getSchema() == null ? null : tableResult.getSchema().getFields();
            final Function<? super FieldValueList, ? extends T> mapper = createRowMapper(targetClass, fields);

            return Stream.of(tableResult.iterateAll()).map(mapper);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.toRuntimeException(e, true);
        } catch (final JobException e) {
            throw ExceptionUtil.toRuntimeException(e, true);
        }
    }

    /**
     * Executes a BigQuery job and returns raw results as a Stream of FieldValueList.
     * <p>
     * This method provides direct access to BigQuery's native result format without
     * automatic conversion. This is useful when you need full control over result
     * processing or when working with complex nested data structures.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Stream raw FieldValueList for custom processing
     * QueryJobConfiguration config = QueryJobConfiguration.newBuilder(
     *     "SELECT * FROM customers")
     *     .build();
     *
     * Stream<FieldValueList> rawStream = executor.stream(config);   // returns a lazy Stream of raw rows (no entity conversion)
     *
     * rawStream.forEach(row -> {
     *     // Access fields by index or name
     *     String id = row.get("customer_id").getStringValue();
     *     String name = row.get("name").getStringValue();
     *     System.out.println(id + ": " + name);
     * });
     *
     * // Handle complex nested structures
     * QueryJobConfiguration nestedConfig = QueryJobConfiguration.newBuilder(
     *     "SELECT id, STRUCT(name, email) as contact FROM customers")
     *     .build();
     *
     * executor.stream(nestedConfig)
     *     .forEach(row -> {
     *         FieldValueList contactStruct = (FieldValueList) row.get("contact").getValue();
     *         // Process nested structure
     *     });
     * }</pre>
     *
     * @param queryConfig the BigQuery QueryJobConfiguration with query and job settings
     * @return a Stream containing raw FieldValueList objects from the query results
     * @throws RuntimeException if the query job fails or the calling thread is interrupted
     * @see QueryJobConfiguration
     * @see FieldValueList
     * @see #stream(Class, QueryJobConfiguration)
     */
    public final Stream<FieldValueList> stream(final QueryJobConfiguration queryConfig) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing query: {}", queryConfig.getQuery());
        }

        try {
            return Stream.of(bigQuery.query(queryConfig).iterateAll());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.toRuntimeException(e, true);
        } catch (final JobException e) {
            throw ExceptionUtil.toRuntimeException(e, true);
        }
    }

    private TableResult execute(final SP sp) {
        return execute(sp.query(), sp.parameters().toArray());
    }

    /**
     * Submits an arbitrary BigQuery SQL statement (SELECT/DML/DDL) as a query job with positional
     * parameters and blocks the calling thread until it completes.
     * <p>
     * Parameters are bound positionally to {@code ?} placeholders and converted via
     * {@link #buildQueryParameterValue(Object...)}. The job runs synchronously through
     * {@link BigQuery#query(QueryJobConfiguration, com.google.cloud.bigquery.BigQuery.JobOption...)};
     * an {@link InterruptedException} during the wait is rethrown as a {@link RuntimeException}
     * after the thread's interrupt flag is restored, and a {@link JobException} (failed/cancelled
     * job) is wrapped via {@link ExceptionUtil#toRuntimeException(Throwable, boolean)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // SELECT (java.sql.Date binds as a native DATE parameter; a raw LocalDate would bind as STRING)
     * TableResult result = executor.execute(
     *     "SELECT * FROM customers WHERE status = ? AND created_date > ?",
     *     "active", java.sql.Date.valueOf(LocalDate.now().minusMonths(6)));   // returns a TableResult holding the selected rows
     *
     * // DML (returned TableResult exposes the affected-row count via getTotalRows())
     * TableResult updated = executor.execute(
     *     "UPDATE customers SET status = ? WHERE last_order_date < ?",
     *     "inactive", java.sql.Date.valueOf(LocalDate.now().minusYears(1)));   // returns a TableResult; getTotalRows() is the affected-row count
     *
     * // DDL (no parameters; returned TableResult has no schema)
     * executor.execute("CREATE TABLE IF NOT EXISTS test_table (id STRING, name STRING)");   // returns a TableResult
     * }</pre>
     *
     * @param query the SQL statement with {@code ?} positional placeholders; must not be {@code null}
     * @param parameters values bound to the placeholders, in order; may be omitted entirely if the
     *                   statement has no parameters
     * @return the {@link TableResult} returned by BigQuery; for SELECT statements this contains the
     *         rows, for DML the affected-row count via {@link TableResult#getTotalRows()}, for DDL
     *         a result with no schema
     * @throws NullPointerException if {@code query} is null
     * @throws RuntimeException if the underlying BigQuery call throws {@link JobException} or the
     *                          calling thread is interrupted while waiting for the job
     * @see #stream(Class, String, Object...)
     * @see #list(Class, String, Object...)
     */
    public final TableResult execute(final String query, final Object... parameters) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing query: {}", query);
        }

        final List<QueryParameterValue> values = buildQueryParameterValue(parameters);
        final QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setPositionalParameters(values).build();

        try {
            return bigQuery.query(queryConfig);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.toRuntimeException(e, true);
        } catch (final JobException e) {
            throw ExceptionUtil.toRuntimeException(e, true);
        }
    }

    private SP prepareQuery(final Class<?> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        return prepareQuery(targetClass, selectPropNames, whereClause, 0);
    }

    private SP prepareQuery(final Class<?> targetClass, final Collection<String> selectPropNames, final Condition whereClause, final int count) {
        final boolean isNonNullCond = whereClause != null;
        SqlBuilder sqlBuilder = null;

        switch (namingPolicy) {
            case SNAKE_CASE:
                if (N.isEmpty(selectPropNames)) {
                    sqlBuilder = PSC.selectFrom(targetClass);
                } else {
                    sqlBuilder = PSC.select(selectPropNames).from(targetClass);
                }

                break;

            case SCREAMING_SNAKE_CASE:
                if (N.isEmpty(selectPropNames)) {
                    sqlBuilder = PAC.selectFrom(targetClass);
                } else {
                    sqlBuilder = PAC.select(selectPropNames).from(targetClass);
                }

                break;

            case CAMEL_CASE:
                if (N.isEmpty(selectPropNames)) {
                    sqlBuilder = PLC.selectFrom(targetClass);
                } else {
                    sqlBuilder = PLC.select(selectPropNames).from(targetClass);
                }

                break;

            default:
                throw new IllegalStateException("Unsupported naming policy: " + namingPolicy);
        }

        if (isNonNullCond) {
            sqlBuilder.where(whereClause);
        }

        if (count > 0) {
            sqlBuilder.limit(count);
        }

        final SP sp = sqlBuilder.build();

        // BigQuery (GoogleSQL) quotes identifiers with backticks; a double-quoted token is a string literal.
        // SqlBuilder emits ANSI double-quoted column aliases (e.g. SELECT id AS "id"), which BigQuery rejects
        // with "Unexpected string literal". Every value in the generated SELECT is a positional '?' parameter
        // (no string literals), so the only double quotes are alias delimiters; rewrite them to backticks.
        return sp.query().indexOf('"') < 0 ? sp : new SP(sp.query().replace('"', '`'), sp.parameters());
    }

    private static final Function<Object, QueryParameterValue> defaultQueryParameterCreator = value -> {
        final Type<?> type = Type.of(value.getClass());

        if (type.isArray() || type.isCollection() || type.isMap() || type.isBean()) {
            return QueryParameterValue.json(N.toJson(value));
        } else {
            return QueryParameterValue.string(N.stringOf(value));
        }
    };

    private static final Map<Class<?>, Function<Object, QueryParameterValue>> queryParameterMap = new HashMap<>();

    static {
        queryParameterMap.put(String.class, value -> QueryParameterValue.string((String) value));
        queryParameterMap.put(Boolean.class, value -> QueryParameterValue.bool((Boolean) value));
        queryParameterMap.put(Character.class, value -> QueryParameterValue.string(value.toString()));
        queryParameterMap.put(Byte.class, value -> QueryParameterValue.int64(((Number) value).intValue()));
        queryParameterMap.put(Short.class, value -> QueryParameterValue.int64(((Number) value).intValue()));
        queryParameterMap.put(Integer.class, value -> QueryParameterValue.int64(((Number) value).intValue()));
        queryParameterMap.put(Long.class, value -> QueryParameterValue.int64(((Number) value).longValue()));
        queryParameterMap.put(Float.class, value -> QueryParameterValue.float64(Numbers.toFloat(value)));
        queryParameterMap.put(Double.class, value -> QueryParameterValue.float64(Numbers.toDouble(value)));
        queryParameterMap.put(BigDecimal.class, value -> QueryParameterValue.bigNumeric((BigDecimal) value));

        queryParameterMap.put(java.util.Date.class, value -> QueryParameterValue.timestamp(TimeUnit.MILLISECONDS.toMicros(((java.util.Date) value).getTime())));

        queryParameterMap.put(java.sql.Date.class, value -> QueryParameterValue.date(value.toString()));
        queryParameterMap.put(java.sql.Time.class, value -> QueryParameterValue.time(value.toString() + ".000000"));
        queryParameterMap.put(java.sql.Timestamp.class, value -> QueryParameterValue.timestamp(toMicros((java.sql.Timestamp) value)));

        queryParameterMap.put(com.google.cloud.Date.class, value -> QueryParameterValue.date(value.toString()));
        queryParameterMap.put(com.google.cloud.Timestamp.class, value -> {
            // Preserve microsecond precision: toDate() truncates to milliseconds, silently losing the
            // micro component that BigQuery TIMESTAMP carries (a WHERE equality on a value read back
            // from BigQuery could then fail to match).
            final com.google.cloud.Timestamp ts = (com.google.cloud.Timestamp) value;
            return QueryParameterValue.timestamp(TimeUnit.SECONDS.toMicros(ts.getSeconds()) + ts.getNanos() / 1_000L);
        });

        queryParameterMap.put(byte[].class, value -> QueryParameterValue.bytes((byte[]) value));
    }

    private static long toMicros(final java.sql.Timestamp timestamp) {
        return TimeUnit.MILLISECONDS.toMicros(timestamp.getTime()) + (timestamp.getNanos() % 1_000_000) / 1000;
    }

    /**
     * Converts an array of Java values into a list of BigQuery {@link QueryParameterValue}s
     * suitable for positional parameter binding in {@link QueryJobConfiguration#setPositionalParameters}.
     *
     * <p>Conversion rules (applied per element, in order):</p>
     * <ol>
     *   <li>Elements that are already {@link QueryParameterValue} instances are passed through
     *       unchanged.</li>
     *   <li>{@code null} elements raise {@link IllegalArgumentException} &mdash; BigQuery requires
     *       the SQL type of a {@code NULL} parameter to be known, so callers must wrap nulls in a
     *       {@code QueryParameterValue} of the appropriate type.</li>
     *   <li>Strings, primitives and their boxed counterparts, {@link BigDecimal},
     *       {@code java.util.Date}, {@code java.sql.Date}/{@code Time}/{@code Timestamp},
     *       {@code com.google.cloud.Date}, {@code com.google.cloud.Timestamp}, and {@code byte[]}
     *       are mapped to their canonical BigQuery types.</li>
     *   <li>Arrays, collections, maps, and beans are serialised to JSON via
     *       {@link QueryParameterValue#json(String)}.</li>
     *   <li>Any other scalar (e.g. {@code java.time} types such as {@code LocalDate}, enums,
     *       {@code UUID}) is bound as a {@code STRING} parameter from its string form.</li>
     * </ol>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<QueryParameterValue> params = BigQueryExecutor.buildQueryParameterValue(
     *     "John", 25, LocalDate.now());   // LocalDate binds as STRING; use java.sql.Date for a native DATE
     * }</pre>
     *
     * @param parameters the values to convert; may be {@code null} or empty
     * @return a list of {@link QueryParameterValue}s in the same order as {@code parameters}, or
     *         {@link N#emptyList()} when {@code parameters} is {@code null} or empty
     * @throws IllegalArgumentException if any element of {@code parameters} is {@code null} and not
     *                                  already a {@link QueryParameterValue}
     * @see QueryParameterValue
     * @see #execute(String, Object...)
     */
    static List<QueryParameterValue> buildQueryParameterValue(final Object... parameters) {
        if (N.isEmpty(parameters)) {
            return N.emptyList();
        }

        final List<QueryParameterValue> queryParameterValues = new ArrayList<>(parameters.length);

        for (final Object param : parameters) {
            if (param instanceof final QueryParameterValue qpv) {
                queryParameterValues.add(qpv);
            } else if (param == null) {
                throw new IllegalArgumentException("Null query parameters must be wrapped in QueryParameterValue so the SQL type is known");
            } else {
                queryParameterValues.add(queryParameterMap.getOrDefault(param.getClass(), defaultQueryParameterCreator).apply(param));
            }
        }

        return queryParameterValues;
    }

    /**
     * Extracts the {@link FieldList} backing a {@link FieldValueList} by reflectively reading the
     * library-private {@code FieldValueList.schema} field.
     *
     * <p>The reflective accessor is resolved once in a static initialiser; if the BigQuery client
     * library hides or removes the field, that initialiser silently disables the optimisation and
     * <i>this method then fails</i> with {@link IllegalArgumentException} on the
     * {@code N.checkArgNotNull(schemaFieldOfFieldList, ...)} guard.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * TableResult result = executor.execute("SELECT * FROM dataset.table");
     * FieldValueList row = result.getValues().iterator().next();
     * FieldList schema = BigQueryExecutor.getSchema(row);
     * }</pre>
     *
     * @param fieldValueList the row whose schema to extract; must not be {@code null}
     * @return the {@link FieldList} describing the columns of {@code fieldValueList}
     * @throws IllegalArgumentException if the reflective accessor was not available at class-init
     *                                  time (e.g. on a future BigQuery client library version that
     *                                  removes the {@code schema} field)
     * @throws RuntimeException if the accessor was available at class-init but became inaccessible
     *                          at call time (the accessor is also disabled for subsequent calls in
     *                          that case)
     * @see FieldList
     * @see FieldValueList
     */
    static FieldList getSchema(final FieldValueList fieldValueList) {
        N.checkArgNotNull(schemaFieldOfFieldList, "Can't get schema of 'FieldValueList' by java reflect api");

        FieldList fields = null;

        try {
            fields = (FieldList) schemaFieldOfFieldList.get(fieldValueList);
        } catch (final IllegalAccessException e) {
            logger.warn("Unexpected: schema field of 'FieldValueList' became inaccessible; disabling reflection-based schema access", e);
            schemaFieldOfFieldList = null;
            throw ExceptionUtil.toRuntimeException(e, true);
        }
        return fields;
    }

}
