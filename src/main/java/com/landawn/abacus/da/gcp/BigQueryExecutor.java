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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.query.SqlBuilder;
import com.landawn.abacus.query.SqlBuilder.NAC;
import com.landawn.abacus.query.SqlBuilder.NLC;
import com.landawn.abacus.query.SqlBuilder.NSC;
import com.landawn.abacus.query.SqlBuilder.PAC;
import com.landawn.abacus.query.SqlBuilder.PLC;
import com.landawn.abacus.query.SqlBuilder.PSC;
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
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple2;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.stream.Stream;

/**
 * A comprehensive executor for Google Cloud BigQuery operations, providing high-level abstractions
 * for SQL query execution, data manipulation, and BigQuery-specific features.
 * <p>
 * This executor supports BigQuery's SQL dialect and provides seamless integration with Google Cloud BigQuery
 * services including dataset operations, table management, job execution, and streaming inserts.
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li>Standard SQL query execution with parameter binding</li>
 *   <li>Dataset and table operations (create, read, update, delete)</li>
 *   <li>Job management and asynchronous query execution</li>
 *   <li>Streaming data inserts and batch processing</li>
 *   <li>Support for BigQuery data types and nested structures</li>
 *   <li>Automatic conversion between Java objects and BigQuery data</li>
 *   <li>Multiple naming policy support (snake_case, camelCase, etc.)</li>
 *   <li>Partitioned table support and query optimization</li>
 * </ul>
 * 
 * <h3>Naming Policy Support:</h3>
 * <p>The executor supports different naming conventions for mapping Java properties to BigQuery columns:</p>
 * <ul>
 *   <li>{@code SNAKE_CASE} - snake_case (default)</li>
 *   <li>{@code SCREAMING_SNAKE_CASE} - SCREAMING_SNAKE_CASE (e.g., firstName → FIRST_NAME)</li>
 *   <li>{@code CAMEL_CASE} - camelCase</li>
 * </ul>
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
 * // Complex queries with conditions
 * Dataset results = executor.query(Customer.class, 
 *     Filters.and(Filters.eq("status", "active"), Filters.gt("created_date", yesterday)));
 * }</pre>
 * 
 * <h3>BigQuery-Specific Features:</h3>
 * <ul>
 *   <li>Support for ARRAY and STRUCT data types</li>
 *   <li>Partitioned and clustered table operations</li>
 *   <li>Query job configuration and optimization</li>
 *   <li>Streaming insert capabilities</li>
 *   <li>DML and DDL statement execution</li>
 * </ul>
 * 
 * @see com.google.cloud.bigquery.BigQuery
 * @see com.google.cloud.bigquery.QueryJobConfiguration
 * @see com.google.cloud.bigquery.TableResult
 */
@SuppressWarnings("java:S1192")
public class BigQueryExecutor {

    private static volatile java.lang.reflect.Field schemaFieldOfFieldList;

    static {
        java.lang.reflect.Field tmp = null;

        try {
            tmp = FieldValueList.class.getDeclaredField("schema");
        } catch (final Throwable e) {
            // ignore.
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
     * Constructs a BigQueryExecutor with the specified BigQuery instance using default naming policy.
     * <p>
     * This constructor uses the default naming policy of SNAKE_CASE, which maps
     * Java camelCase property names to BigQuery snake_case column names (e.g., firstName → first_name).
     * 
     * @param bigQuery the Google Cloud BigQuery service instance
     * @throws IllegalArgumentException if bigQuery is null
     * @see #BigQueryExecutor(BigQuery, NamingPolicy)
     * @see NamingPolicy#SNAKE_CASE
     */
    public BigQueryExecutor(final BigQuery bigQuery) {
        this(bigQuery, NamingPolicy.SNAKE_CASE);
    }

    /**
     * Constructs a BigQueryExecutor with the specified BigQuery instance and naming policy.
     * <p>
     * The naming policy determines how Java property names are mapped to BigQuery column names.
     *
     * @param bigQuery the Google Cloud BigQuery service instance
     * @param namingPolicy the naming convention for property-to-column mapping
     * @throws IllegalArgumentException if bigQuery or namingPolicy is null
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
     * Returns the underlying Google Cloud BigQuery service instance.
     * <p>
     * This method provides access to the BigQuery service for advanced operations
     * not directly supported by this executor.
     * 
     * @return the BigQuery service instance
     * @see com.google.cloud.bigquery.BigQuery
     */
    public BigQuery bigQuery() {
        return bigQuery;
    }

    /**
     * Converts a BigQuery FieldValueList to a Java entity using the provided schema.
     * <p>
     * This method maps BigQuery query results to Java objects based on the schema definition.
     * The entity class must have appropriate getter/setter methods for the fields.
     * 
     * @param <T> the target entity type
     * @param schema the BigQuery schema defining field structure
     * @param fieldValueList the row data from BigQuery
     * @param entityClass the target entity class with getter/setter methods
     * @return the converted entity instance
     * @throws IllegalArgumentException if schema, fieldValueList, or entityClass is null,
     *                                  or if entityClass is not a valid entity
     * @see #toEntity(FieldValueList, Class)
     */
    public static <T> T toEntity(final Schema schema, final FieldValueList fieldValueList, final Class<T> entityClass) {
        N.checkArgNotNull(schema, "schema");

        return toEntity(schema.getFields(), fieldValueList, entityClass);
    }

    /**
     * Converts a BigQuery FieldValueList to a Java entity using the provided field list.
     * <p>
     * This method maps BigQuery query results to Java objects based on the field list definition.
     * The entity class must have appropriate getter/setter methods for the fields. Property names
     * are matched using column-to-property mapping conventions.
     * 
     * @param <T> the target entity type
     * @param fields the BigQuery field list defining field structure and names
     * @param fieldValueList the row data from BigQuery containing the actual values
     * @param entityClass the target entity class with getter/setter methods
     * @return the converted entity instance with mapped values
     * @throws IllegalArgumentException if entityClass is not a valid bean class with getter/setter methods,
     *                                  or if fields or fieldValueList is null
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

            if ((propValue == null || parameterType.isAssignableFrom(propValue.getClass())) || !(propValue instanceof FieldValueList)) {
                propInfo.setPropValue(entity, propValue);
            } else {
                propInfo.setPropValue(entity, readRow((FieldValueList) propValue, parameterType));
            }
        }

        return entityInfo.finishBeanResult(entity);
    }

    /**
     * Converts a BigQuery FieldValueList to a Java entity by automatically extracting the schema.
     * <p>
     * This is a convenience method that automatically extracts the schema information from the
     * FieldValueList and then performs the entity conversion. The entity class must have
     * appropriate getter/setter methods for the fields.
     * 
     * @param <T> the target entity type
     * @param fieldValueList the row data from BigQuery containing both schema and values
     * @param entityClass the target entity class with getter/setter methods
     * @return the converted entity instance with mapped values
     * @throws IllegalArgumentException if entityClass is not a valid bean class,
     *                                  or if fieldValueList is null
     * @see #toEntity(FieldList, FieldValueList, Class)
     */
    public static <T> T toEntity(final FieldValueList fieldValueList, final Class<T> entityClass) {
        return toEntity(getSchema(fieldValueList), fieldValueList, entityClass);
    }

    /**
     * Converts a BigQuery FieldValueList to a Map using the provided schema.
     * <p>
     * This method creates a Map where keys are column names from the schema and values
     * are the corresponding data from the FieldValueList. Nested structures are recursively
     * converted to nested Maps.
     * 
     * @param schema the BigQuery schema defining field structure and names
     * @param fieldValueList the row data from BigQuery containing the values
     * @return a Map containing column names as keys and corresponding values
     * @throws IllegalArgumentException if schema or fieldValueList is null
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
     * Converts a BigQuery FieldValueList to a Map using the provided field list and Map supplier.
     * <p>
     * This method creates a Map of the specified type where keys are column names from the field list
     * and values are the corresponding data from the FieldValueList. Nested FieldValueList structures
     * are recursively converted to nested Maps using the same supplier function.
     * 
     * @param fields the BigQuery field list defining field structure and names
     * @param fieldValueList the row data from BigQuery containing the values
     * @param supplier a function that creates Map instances given the expected size
     * @return a Map of the supplier's type containing column names as keys and corresponding values
     * @throws IllegalArgumentException if fields, fieldValueList, or supplier is null
     * @see IntFunctions#ofMap()
     * @see IntFunctions#ofLinkedHashMap()
     */
    public static Map<String, Object> toMap(final FieldList fields, final FieldValueList fieldValueList,
            final IntFunction<? extends Map<String, Object>> supplier) {
        N.checkArgNotNull(fields, "fields");
        N.checkArgNotNull(fieldValueList, "fieldValueList");
        N.checkArgNotNull(supplier, "supplier");

        final Map<String, Object> map = supplier.apply(fieldValueList.size());
        Object propValue = null;

        for (int i = 0, size = fieldValueList.size(); i < size; i++) {
            propValue = fieldValueList.get(i).getValue();

            if (propValue instanceof FieldValueList) {
                propValue = toMap((FieldValueList) propValue, supplier);
            }

            map.put(fields.get(i).getName(), propValue);
        }

        return map;
    }

    /**
     * Converts a BigQuery FieldValueList to a HashMap by automatically extracting the schema.
     * <p>
     * This is a convenience method that automatically extracts the schema information from the
     * FieldValueList and creates a HashMap containing the column-value mappings.
     * 
     * @param fieldValueList the row data from BigQuery containing both schema and values
     * @return a HashMap containing column names as keys and corresponding values
     * @throws IllegalArgumentException if fieldValueList is null
     * @see #toMap(Schema, FieldValueList)
     */
    public static Map<String, Object> toMap(final FieldValueList fieldValueList) {
        return toMap(getSchema(fieldValueList), fieldValueList);
    }

    /**
     * Converts a BigQuery FieldValueList to a Map using the provided Map supplier.
     * <p>
     * This is a convenience method that automatically extracts the schema information from the
     * FieldValueList and creates a Map of the specified type containing the column-value mappings.
     * 
     * @param fieldValueList the row data from BigQuery containing both schema and values
     * @param supplier a function that creates Map instances given the expected size
     * @return a Map of the supplier's type containing column names as keys and corresponding values
     * @throws IllegalArgumentException if fieldValueList or supplier is null
     * @see #toMap(FieldList, FieldValueList, IntFunction)
     */
    public static Map<String, Object> toMap(final FieldValueList fieldValueList, final IntFunction<? extends Map<String, Object>> supplier) {
        return toMap(getSchema(fieldValueList), fieldValueList, supplier);
    }

    @SuppressWarnings("rawtypes")
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
            throw new IllegalArgumentException("Unsupported row/column type: " + ClassUtil.getCanonicalClassName(rowClass));
        }

        return (T) res;
    }

    private static <T> Function<? super FieldValueList, ? extends T> createRowMapper(final Class<T> rowClass, final FieldList fields) {
        final Type<?> rowType = rowClass == null ? null : Type.of(rowClass);
        Function<? super FieldValueList, ? extends T> mapper = null;

        if (rowType == null || rowType.isObjectArray()) {
            mapper = new Function<>() {
                private FieldList rowFields = fields;
                private int fieldCount = 0;

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
                private int fieldCount = 0;

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
     * Converts a BigQuery TableResult to a List of Java objects.
     * <p>
     * This method processes all rows from a BigQuery query result and converts them to a list
     * of Java objects of the specified type. The target class can be an entity class with
     * getter/setter methods, a Map class, Object/Collection arrays, or basic value types.
     *
     * @param <T> the target type for list elements
     * @param tableResult the BigQuery table result containing query data
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   Object[].class, Collection classes, or supported basic types)
     * @return a List containing the converted objects from all result rows, empty list if no results
     * @throws IllegalArgumentException if targetClass is not supported for conversion
     * @see #toEntity(FieldValueList, Class)
     * @see #stream(Class, String, Object...)
     */
    public static <T> List<T> toList(final TableResult tableResult, final Class<T> targetClass) {
        final int rowCount = Numbers.toIntExact(tableResult.getTotalRows());

        if (rowCount == 0) {
            return new ArrayList<>(0);
        }

        final Schema schema = tableResult.getSchema();
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
     * Extracts data from a BigQuery TableResult into a Dataset structure.
     * <p>
     * This method converts a BigQuery TableResult into a columnar Dataset format where each
     * column contains all values for that field across all rows. This is useful for data
     * analysis operations where columnar access is preferred.
     *
     * @param tableResult the BigQuery table result containing query data
     * @param targetClass the target class for result conversion (entity class with getter/setter methods or Map.class)
     *                   used to determine column types and structure
     * @return a Dataset with column-oriented data from the table result
     * @throws IllegalArgumentException if targetClass is not a valid entity or Map class
     * @see RowDataset
     * @see #query(Class, String, Object...)
     */
    public static Dataset extractData(final TableResult tableResult, final Class<?> targetClass) {
        final int rowCount = Numbers.toIntExact(tableResult.getTotalRows());
        final Schema schema = tableResult.getSchema();

        if (schema == null && rowCount == 0) {
            return N.newEmptyDataset();
        }

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
                columnClasses[i] = isMap ? Map.class : Object[].class;
            }
        }

        Object value = null;

        for (final FieldValueList row : rows) {
            for (int i = 0; i < fieldCount; i++) {
                value = row.get(i).getValue();

                if (value instanceof FieldValueList && (columnClasses[i] == null || !columnClasses[i].isAssignableFrom(FieldValueList.class))) {
                    columnList.get(i).add(readRow((FieldValueList) value, columnClasses[i]));
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
     * TableResult result = executor.insert(customer);
     * System.out.println("Rows inserted: " + result.getTotalRows());
     * }</pre>
     *
     * @param entity the entity instance to insert (must have @Table annotation or class name matching table)
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if entity is null or invalid
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
                return NSC.insert(entity).into(targetClass).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.insert(entity).into(targetClass).build();

            case CAMEL_CASE:
                return NLC.insert(entity).into(targetClass).build();

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
     * TableResult result = executor.insert(Customer.class, customerData);
     * System.out.println("Inserted: " + result.getTotalRows() + " rows");
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name resolution)
     * @param props a Map containing column names as keys and values to insert
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if targetClass or props is null, or if props is empty
     * @see #insert(Object)
     */
    public TableResult insert(final Class<?> targetClass, final Map<String, Object> props) {
        final SP sp = prepareInsert(targetClass, props);

        return execute(sp);
    }

    private SP prepareInsert(final Class<?> targetClass, final Map<String, Object> props) {
        switch (namingPolicy) {
            case SNAKE_CASE:
                return NSC.insert(props).into(targetClass).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.insert(props).into(targetClass).build();

            case CAMEL_CASE:
                return NLC.insert(props).into(targetClass).build();

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
     * Customer customer = executor.queryForObject(Customer.class,
     *     "SELECT * FROM customers WHERE customer_id = ?", "CUST123");
     * customer.setEmail("newemail@example.com");
     *
     * TableResult result = executor.update(customer);
     * System.out.println("Updated rows: " + result.getTotalRows());
     * }</pre>
     *
     * @param entity the entity instance containing updated values and primary key values
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if entity is null or no primary key fields are found
     * @see #update(Object, Set)
     */
    public TableResult update(final Object entity) {
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
     * TableResult result = executor.update(item, keyFields);
     * }</pre>
     *
     * @param entity the entity instance containing updated values and primary key values
     * @param primaryKeyNames the set of property names to use as primary key fields in the WHERE clause
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if entity or primaryKeyNames is null or empty
     * @see #update(Class, Map, Condition)
     */
    public TableResult update(final Object entity, final Set<String> primaryKeyNames) {
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
                return NSC.update(targetClass).set(entity, primaryKeyNames).where(Filters.and(conds)).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.update(targetClass).set(entity, primaryKeyNames).where(Filters.and(conds)).build();

            case CAMEL_CASE:
                return NLC.update(targetClass).set(entity, primaryKeyNames).where(Filters.and(conds)).build();

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
     * TableResult result = executor.update(Customer.class, updates, condition);
     * System.out.println("Updated " + result.getTotalRows() + " records");
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name resolution)
     * @param props a Map containing column names as keys and new values to set
     * @param whereClause the condition specifying which records to update
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if targetClass, props, or whereClause is null, or if props is empty
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
                return NSC.update(targetClass).set(props).where(whereClause).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.update(targetClass).set(props).where(whereClause).build();

            case CAMEL_CASE:
                return NLC.update(targetClass).set(props).where(whereClause).build();

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
     * TableResult result = executor.delete(customer);
     * System.out.println("Deleted rows: " + result.getTotalRows());
     *
     * // Or delete by loading first
     * Customer existing = executor.queryForObject(Customer.class,
     *     "SELECT * FROM customers WHERE customer_id = ?", "CUST456");
     * executor.delete(existing);
     * }</pre>
     *
     * @param entity the entity instance containing primary key values for deletion
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if entity is null or no primary key values are found
     * @see #delete(Class, Object...)
     */
    public TableResult delete(final Object entity) {
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
     * TableResult result = executor.delete(Customer.class, "CUST123");
     * System.out.println("Deleted: " + result.getTotalRows() + " rows");
     *
     * // Delete by composite key
     * TableResult result2 = executor.delete(OrderItem.class, "ORD123", "ITEM456");
     *
     * // Delete multiple records one by one
     * List<String> customerIds = Arrays.asList("CUST001", "CUST002", "CUST003");
     * for (String id : customerIds) {
     *     executor.delete(Customer.class, id);
     * }
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name and key resolution)
     * @param ids the primary key values to identify records for deletion
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if targetClass is null, ids is null or empty,
     *                                  or the number of IDs doesn't match the primary key structure
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
     * TableResult result = executor.delete(Customer.class, condition);
     *
     * // Delete with complex condition
     * Condition complexCondition = Filters.and(
     *     Filters.eq("status", "pending"),
     *     Filters.lt("createdDate", LocalDate.now().minusDays(30)),
     *     Filters.isNull("processedDate")
     * );
     * TableResult result2 = executor.delete(Order.class, complexCondition);
     * System.out.println("Deleted " + result2.getTotalRows() + " expired orders");
     * }</pre>
     *
     * @param targetClass the target class representing the table (used for table name resolution)
     * @param whereClause the condition specifying which records to delete
     * @return the TableResult containing execution statistics including number of rows affected
     * @throws IllegalArgumentException if targetClass or whereClause is null
     * @see com.landawn.abacus.query.Filters
     */
    public TableResult delete(final Class<?> targetClass, final Condition whereClause) {
        final SP sp = prepareDelete(targetClass, whereClause);

        return execute(sp);
    }

    private SP prepareDelete(final Class<?> targetClass, final Condition whereClause) {
        switch (namingPolicy) {
            case SNAKE_CASE:
                return NSC.deleteFrom(targetClass).where(whereClause).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.deleteFrom(targetClass).where(whereClause).build();

            case CAMEL_CASE:
                return NLC.deleteFrom(targetClass).where(whereClause).build();

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
     * boolean exists = executor.exists(Customer.class, "CUST123");
     * if (exists) {
     *     System.out.println("Customer exists");
     * }
     *
     * // Check existence by composite key
     * boolean itemExists = executor.exists(OrderItem.class, "ORD123", "ITEM456");
     *
     * // Conditional logic based on existence
     * if (!executor.exists(Customer.class, customerId)) {
     *     executor.insert(newCustomer);
     * } else {
     *     executor.update(existingCustomer);
     * }
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name and key resolution)
     * @param ids the primary key values to check for existence
     * @return {@code true} if a record exists with the specified key values, {@code false} otherwise
     * @throws IllegalArgumentException if targetClass is null, ids is null or empty,
     *                                  or the number of IDs doesn't match the primary key structure
     * @see #exists(Class, Condition)
     */
    public final boolean exists(final Class<?> targetClass, final Object... ids) {
        return exists(targetClass, idsToCondition(targetClass, ids));
    }

    private static final Map<Class<?>, Tuple2<ImmutableList<String>, ImmutableSet<String>>> entityKeyNamesMap = new ConcurrentHashMap<>();

    private static ImmutableList<String> getKeyNames(final Class<?> entityClass) {
        Tuple2<ImmutableList<String>, ImmutableSet<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            @SuppressWarnings("deprecation")
            final List<String> idPropNames = QueryUtil.getIdPropNames(entityClass);
            tp = Tuple.of(ImmutableList.copyOf(idPropNames), ImmutableSet.copyOf(idPropNames));
            entityKeyNamesMap.put(entityClass, tp);
        }

        return tp._1;
    }

    private static Set<String> getKeyNameSet(final Class<?> entityClass) {
        Tuple2<ImmutableList<String>, ImmutableSet<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            @SuppressWarnings("deprecation")
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
     * Checks if records exist in a BigQuery table using a custom condition.
     * <p>
     * This method performs a SELECT query with LIMIT 1 to efficiently determine if any records
     * exist matching the specified condition. Only the primary key fields are selected to
     * minimize data transfer.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Check if any active customers exist
     * Condition condition = Filters.eq("status", "active");
     * boolean hasActiveCustomers = executor.exists(Customer.class, condition);
     *
     * // Check for records matching complex criteria
     * Condition complexCond = Filters.and(
     *     Filters.gt("orderAmount", new BigDecimal("1000")),
     *     Filters.eq("paymentStatus", "pending"),
     *     Filters.between("orderDate", startDate, endDate)
     * );
     * boolean hasPendingLargeOrders = executor.exists(Order.class, complexCond);
     *
     * if (hasPendingLargeOrders) {
     *     // Process pending large orders
     * }
     * }</pre>
     *
     * @param targetClass the class representing the target table (used for table name resolution)
     * @param whereClause the condition to check for matching records
     * @return {@code true} if at least one record matches the condition, {@code false} otherwise
     * @throws IllegalArgumentException if targetClass or whereClause is null
     * @see com.landawn.abacus.query.Filters
     */
    public boolean exists(final Class<?> targetClass, final Condition whereClause) {
        final ImmutableList<String> keyNames = getKeyNames(targetClass);
        final SP sp = prepareQuery(targetClass, keyNames, whereClause, 1);
        final TableResult resultSet = execute(sp);

        return resultSet.getTotalRows() > 0;
    }

    /**
     * Queries for a single property value from a BigQuery table using a condition.
     * <p>
     * This method executes a SELECT query for a specific property/column and returns the first
     * matching value wrapped in a Nullable. This is useful for retrieving single values like
     * counts, sums, or specific field values.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query for a single property value
     * Condition condition = Filters.eq("customerId", "CUST123");
     * Nullable<String> email = executor.queryForSingleResult(
     *     Customer.class, String.class, "email", condition);
     *
     * if (email.isPresent()) {
     *     System.out.println("Customer email: " + email.get());
     * }
     *
     * // Get maximum order amount
     * Condition activeOrders = Filters.eq("status", "active");
     * Nullable<BigDecimal> maxAmount = executor.queryForSingleResult(
     *     Order.class, BigDecimal.class, "orderAmount", activeOrders);
     * }</pre>
     *
     * @param <T> the target table entity type
     * @param <V> the expected value type
     * @param targetClass the class representing the target table
     * @param valueClass the expected class of the returned value
     * @param propName the property/column name to select
     * @param whereClause the condition to filter records
     * @return a Nullable containing the first matching value, or empty if no match found
     * @throws IllegalArgumentException if any parameter is null
     * @see #queryForSingleResult(Class, String, Object...)
     */
    public <T, V> Nullable<V> queryForSingleResult(final Class<T> targetClass, final Class<V> valueClass, final String propName, final Condition whereClause) {
        final SP sp = prepareQuery(targetClass, N.asList(propName), whereClause);

        return queryForSingleResult(valueClass, sp.query(), sp.parameters().toArray());
    }

    /**
     * Executes a custom BigQuery SQL query and returns the first column of the first row.
     * <p>
     * This method is designed for queries that return a single value, such as COUNT(*),
     * MAX(column), or single field lookups. If the query returns no rows, an empty
     * Nullable is returned. The result is automatically converted to the specified value class.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get count of records
     * Nullable<Long> count = executor.queryForSingleResult(Long.class,
     *     "SELECT COUNT(*) FROM customers WHERE status = ?", "active");
     * System.out.println("Active customers: " + count.orElse(0L));
     *
     * // Get maximum value
     * Nullable<BigDecimal> maxAmount = executor.queryForSingleResult(BigDecimal.class,
     *     "SELECT MAX(order_amount) FROM orders WHERE order_date = ?",
     *     LocalDate.now());
     *
     * // Get single field value
     * Nullable<String> customerName = executor.queryForSingleResult(String.class,
     *     "SELECT name FROM customers WHERE customer_id = ?", "CUST123");
     *
     * customerName.ifPresent(name -> System.out.println("Customer: " + name));
     * }</pre>
     *
     * @param <V> the expected value type
     * @param valueClass the expected class of the returned value
     * @param query the SQL query string with ? parameter placeholders
     * @param parameters the parameter values to bind to the query
     * @return a Nullable containing the converted value from the first column of the first row,
     *         or empty if the query returns no rows
     * @throws IllegalArgumentException if valueClass or query is null
     * @see #execute(String, Object...)
     */
    public final <V> Nullable<V> queryForSingleResult(final Class<V> valueClass, final String query, final Object... parameters) {
        final TableResult tableResult = execute(query, parameters);
        final FieldValueList row = tableResult.getTotalRows() > 0 ? tableResult.getValues().iterator().next() : null;

        return row == null ? (Nullable<V>) Nullable.empty() : Nullable.of(N.convert(row.get(0).getValue(), valueClass));
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
     * Dataset dataset = executor.query(Customer.class, condition);
     *
     * // Access columns
     * List<String> names = dataset.getColumn("name");
     * List<String> emails = dataset.getColumn("email");
     *
     * // Iterate through rows
     * dataset.forEach(row -> {
     *     System.out.println("Customer: " + row.get("name"));
     * });
     * }</pre>
     *
     * @param <T> the target table entity type
     * @param targetClass the class representing the target table
     * @param whereClause the condition to filter records
     * @return a Dataset containing all matching records in columnar format
     * @throws IllegalArgumentException if targetClass or whereClause is null
     * @see #query(Class, Collection, Condition)
     * @see Dataset
     */
    public <T> Dataset query(final Class<T> targetClass, final Condition whereClause) {
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
     * Dataset dataset = executor.query(Customer.class, columns, condition);
     *
     * // Access specific columns
     * List<String> customerIds = dataset.getColumn("customerId");
     * List<String> names = dataset.getColumn("name");
     *
     * System.out.println("Found " + dataset.rowCount() + " active customers");
     * }</pre>
     *
     * @param <T> the target table entity type
     * @param targetClass the class representing the target table
     * @param selectPropNames the collection of property/column names to select, or null for all columns
     * @param whereClause the condition to filter records
     * @return a Dataset containing matching records for the selected columns in columnar format
     * @throws IllegalArgumentException if targetClass or whereClause is null
     * @see #query(Class, String, Object...)
     */
    public <T> Dataset query(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
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
     *     "active", LocalDate.now().minusMonths(6));
     *
     * // Process results
     * System.out.println("Total rows: " + dataset.rowCount());
     * dataset.getColumn("name").forEach(System.out::println);
     *
     * // Complex query with aggregation
     * String aggQuery = "SELECT status, COUNT(*) as count, AVG(order_amount) as avg_amount " +
     *                   "FROM orders WHERE order_date >= ? GROUP BY status";
     * Dataset aggDataset = executor.query(Map.class, aggQuery, LocalDate.now().minusMonths(1));
     * }</pre>
     *
     * @param targetClass the target class for result conversion (entity class with getter/setter methods or Map.class)
     *                   used for type information and conversion
     * @param query the SQL query string with ? parameter placeholders
     * @param parameters the parameter values to bind to the query
     * @return a Dataset containing query results in columnar format
     * @throws IllegalArgumentException if targetClass or query is null
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
     * List<Customer> customers = executor.list(Customer.class, condition);
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
     * List<Customer> vipCustomers = executor.list(Customer.class, complexCond);
     * }</pre>
     *
     * @param <T> the target type for list elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   Object[].class, Collection classes, or supported basic types)
     * @param whereClause the condition to filter records
     * @return a List containing all matching records converted to the target type, empty list if no matches
     * @throws IllegalArgumentException if targetClass or whereClause is null
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
     * List<Customer> customers = executor.list(Customer.class, columns, condition);
     *
     * // Select to Map for flexibility
     * List<Map<String, Object>> results = executor.list(Map.class, columns, condition);
     * results.forEach(row -> {
     *     System.out.println(row.get("customerId") + ": " + row.get("name"));
     * });
     *
     * // Select single column to extract values
     * Collection<String> idColumn = Arrays.asList("customerId");
     * List<String> customerIds = executor.list(String.class, idColumn, condition);
     * }</pre>
     *
     * @param <T> the target type for list elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   Object[].class, Collection classes, or basic single value types)
     * @param selectPropNames the collection of property/column names to select, or null for all columns
     * @param whereClause the condition to filter records
     * @return a List containing matching records for the selected columns converted to the target type
     * @throws IllegalArgumentException if targetClass or whereClause is null
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
     *     "active", LocalDate.now().minusMonths(6));
     *
     * // Complex JOIN query
     * String joinSql = "SELECT c.customer_id, c.name, COUNT(o.order_id) as order_count " +
     *                  "FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id " +
     *                  "WHERE c.status = ? GROUP BY c.customer_id, c.name";
     * List<Map<String, Object>> results = executor.list(Map.class, joinSql, "active");
     *
     * // Query returning single values
     * String idQuery = "SELECT customer_id FROM customers WHERE status = ?";
     * List<String> activeIds = executor.list(String.class, idQuery, "active");
     * }</pre>
     *
     * @param <T> the target type for list elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or supported basic types)
     * @param query the SQL query string with ? parameter placeholders
     * @param parameters the parameter values to bind to the query
     * @return a List containing all query results converted to the target type, empty list if no results
     * @throws IllegalArgumentException if targetClass or query is null
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
     * Stream<Customer> customerStream = executor.stream(Customer.class, condition);
     *
     * customerStream
     *     .filter(c -> c.getOrderCount() > 10)
     *     .map(Customer::getEmail)
     *     .forEach(email -> sendEmail(email));
     *
     * // Stream with aggregation
     * BigDecimal totalAmount = executor.stream(Order.class, Filters.eq("status", "completed"))
     *     .map(Order::getAmount)
     *     .reduce(BigDecimal.ZERO, BigDecimal::add);
     *
     * // Stream for large datasets (memory efficient)
     * long count = executor.stream(Customer.class, Filters.between("createdDate", start, end))
     *     .count();
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or basic single value types)
     * @param whereClause the condition to filter records
     * @return a Stream containing all matching records converted to the target type
     * @throws IllegalArgumentException if targetClass or whereClause is null
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
     *     .collect(Collectors.toList());
     *
     * // Stream and group by
     * Map<String, List<Customer>> byStatus = executor.stream(Customer.class,
     *         Arrays.asList("customerId", "status"), Filters.notNull("status"))
     *     .collect(Collectors.groupingBy(Customer::getStatus));
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or basic single value types)
     * @param selectPropNames the collection of property/column names to select, or null for all columns
     * @param whereClause the condition to filter records
     * @return a Stream containing matching records for the selected columns converted to the target type
     * @throws IllegalArgumentException if targetClass or whereClause is null
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
     *     "active", LocalDate.now().minusMonths(6));
     *
     * // Process stream with filtering and mapping
     * List<String> premiumEmails = stream
     *     .filter(c -> c.getOrderCount() > 100)
     *     .map(Customer::getEmail)
     *     .collect(Collectors.toList());
     *
     * // Stream for analytics
     * String analyticsSql = "SELECT order_date, SUM(amount) as total FROM orders " +
     *                       "WHERE order_date >= ? GROUP BY order_date";
     * executor.stream(Map.class, analyticsSql, LocalDate.now().minusDays(30))
     *     .forEach(row -> System.out.println(row.get("order_date") + ": " + row.get("total")));
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param targetClass the target class for result conversion (entity class with getter/setter methods, Map.class,
     *                   or supported basic types)
     * @param query the SQL query string with ? parameter placeholders
     * @param parameters the parameter values to bind to the query
     * @return a Stream containing all query results converted to the target type
     * @throws IllegalArgumentException if targetClass or query is null
     * @see #execute(String, Object...)
     * @see Stream
     */
    public final <T> Stream<T> stream(final Class<T> targetClass, final String query, final Object... parameters) {
        final Function<? super FieldValueList, ? extends T> mapper = createRowMapper(targetClass, null);

        return Stream.of(execute(query, parameters).iterateAll()).map(mapper);
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
     * Stream<Customer> stream = executor.stream(Customer.class, config);
     * long count = stream.count();
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
     * @throws JobException if the query job fails
     * @see QueryJobConfiguration
     * @see #stream(QueryJobConfiguration)
     */
    public final <T> Stream<T> stream(final Class<T> targetClass, final QueryJobConfiguration queryConfig) {
        final Function<? super FieldValueList, ? extends T> mapper = createRowMapper(targetClass, null);

        try {
            return Stream.of(bigQuery.query(queryConfig).iterateAll()).map(mapper);
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
     * Stream<FieldValueList> rawStream = executor.stream(config);
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
     * @throws JobException if the query job fails
     * @see QueryJobConfiguration
     * @see FieldValueList
     * @see #stream(Class, QueryJobConfiguration)
     */
    public final Stream<FieldValueList> stream(final QueryJobConfiguration queryConfig) {
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
     * Executes a BigQuery SQL statement with positional parameters.
     * <p>
     * This method executes any SQL statement (SELECT, INSERT, UPDATE, DELETE, DDL)
     * and returns the results. Parameters are bound positionally using ? placeholders.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute SELECT query
     * TableResult result = executor.execute(
     *     "SELECT * FROM customers WHERE status = ? AND created_date > ?",
     *     "active", LocalDate.now().minusMonths(6));
     *
     * System.out.println("Rows returned: " + result.getTotalRows());
     *
     * // Execute DML statement
     * TableResult updateResult = executor.execute(
     *     "UPDATE customers SET status = ? WHERE last_order_date < ?",
     *     "inactive", LocalDate.now().minusYears(1));
     *
     * System.out.println("Rows updated: " + updateResult.getTotalRows());
     *
     * // Execute DDL statement
     * TableResult ddlResult = executor.execute(
     *     "CREATE TABLE IF NOT EXISTS test_table (id STRING, name STRING)");
     *
     * // Execute aggregation query
     * TableResult aggResult = executor.execute(
     *     "SELECT status, COUNT(*) as count FROM customers GROUP BY status");
     * }</pre>
     *
     * @param query the SQL query string with ? parameter placeholders
     * @param parameters the parameter values to bind to the query
     * @return the TableResult containing query results and execution statistics
     * @throws JobException if the query execution fails
     * @see #stream(Class, String, Object...)
     * @see #list(Class, String, Object...)
     */
    public final TableResult execute(final String query, final Object... parameters) {
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

    private <T> SP prepareQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        return prepareQuery(targetClass, selectPropNames, whereClause, 0);
    }

    private <T> SP prepareQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause, final int count) {
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

        return sqlBuilder.build();
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

        queryParameterMap.put(java.util.Date.class, value -> QueryParameterValue.timestamp(((java.util.Date) value).getTime()));

        queryParameterMap.put(java.sql.Date.class, value -> QueryParameterValue.timestamp(((java.sql.Date) value).getTime()));
        queryParameterMap.put(java.sql.Time.class, value -> QueryParameterValue.timestamp(((java.sql.Time) value).getTime()));
        queryParameterMap.put(java.sql.Timestamp.class, value -> QueryParameterValue.timestamp(((java.sql.Timestamp) value).getTime()));

        queryParameterMap.put(com.google.cloud.Date.class, value -> QueryParameterValue.date(value.toString()));
        queryParameterMap.put(com.google.cloud.Timestamp.class,
                value -> QueryParameterValue.timestamp(((com.google.cloud.Timestamp) value).toDate().getTime()));

        queryParameterMap.put(byte[].class, value -> QueryParameterValue.bytes((byte[]) value));
    }

    /**
     * Converts Java objects to BigQuery QueryParameterValue objects for parameter binding.
     *
     * <p>This method handles the conversion of various Java types to their corresponding
     * BigQuery parameter representations. It supports primitive types, dates, BigDecimal,
     * and complex objects (which are converted to JSON). The conversion ensures type
     * safety and proper formatting for BigQuery's parameter binding system.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<QueryParameterValue> params = BigQueryExecutor.buildQueryParameterValue(
     *     "John", 25, LocalDate.now());
     * }</pre>
     *
     * @param parameters the Java objects to convert to BigQuery parameters
     * @return a List of QueryParameterValue objects ready for BigQuery parameter binding
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
     * Extracts the schema (FieldList) from a FieldValueList using reflection.
     *
     * <p>This utility method uses Java reflection to access the internal schema field
     * of a FieldValueList object. This is necessary because the Google Cloud BigQuery
     * client library doesn't provide a direct public method to access the schema
     * from FieldValueList objects.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * TableResult result = executor.execute("SELECT * FROM dataset.table");
     * FieldValueList row = result.getValues().iterator().next();
     * FieldList schema = BigQueryExecutor.getSchema(row);
     * }</pre>
     *
     * @param fieldValueList the FieldValueList containing schema information, must not be {@code null}
     * @return the FieldList schema defining the structure of the row data
     * @see FieldList
     * @see FieldValueList
     */
    static FieldList getSchema(final FieldValueList fieldValueList) {
        N.checkArgNotNull(schemaFieldOfFieldList, "Can't get schema of 'FieldValueList' by java reflect api");

        FieldList fields = null;

        try {
            fields = (FieldList) schemaFieldOfFieldList.get(fieldValueList);
        } catch (final IllegalAccessException e) {
            schemaFieldOfFieldList = null;
            throw ExceptionUtil.toRuntimeException(e, true);
        }
        return fields;
    }

    //    public static void main(String[] args) {
    //        FieldValueList fieldValueList = FieldValueList.of(N.asList(FieldValue.of(Attribute.PRIMITIVE, 2)),
    //                FieldList.of(Field.of("order", StandardSQLTypeName.INT64)));
    //        FieldList fields = null;
    //
    //        try {
    //            fields = (FieldList) schemaFieldOfFieldList.get(fieldValueList);
    //        } catch (IllegalAccessException e) {
    //            schemaFieldOfFieldList = null;
    //            throw ExceptionUtil.toRuntimeException(e, true);
    //        }
    //
    //        N.println(fields);
    //    }

}
