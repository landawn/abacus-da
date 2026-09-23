/*
 * Copyright (c) 2026, Haiyang Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.da;

import com.landawn.abacus.annotation.Internal;

/**
 * Internal constants container providing the standardized parameter-name string literals
 * used by argument-validation calls (e.g., {@code N.checkArgNotNull(tableName, cs.tableName)})
 * throughout the Abacus data-access framework.
 *
 * <p>This utility class centralizes the textual identifiers that appear in error messages
 * and diagnostics, ensuring that the parameter name reported when a validation fails
 * matches the actual method-parameter name in the source code, and reducing the risk
 * of typos when the same name is referenced across many call sites.</p>
 *
 * <p><b>Contract:</b> each constant's name and its string value are identical. Renaming a
 * constant therefore requires changing its value in the same change, and vice versa.</p>
 *
 * <p>The constants are primarily used in:</p>
 * <ul>
 *   <li>{@code N.checkArgNotNull / checkArgNotEmpty / checkArgNotBlank / checkArgNotNegative / checkArgPositive}
 *       validation calls inside framework methods</li>
 *   <li>Argument labels for exception messages thrown by framework methods</li>
 * </ul>
 *
 * <p><b>Usage Example (framework-internal):</b></p>
 * <pre>{@code
 * public boolean exists(final String tableName, final AnyGet anyGet) {
 *     N.checkArgNotEmpty(tableName, cs.tableName);
 *     N.checkArgNotNull(anyGet, cs.anyGet);
 *     // ...
 * }
 * }</pre>
 */
@Internal
public final class cs { // NOSONAR
    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private cs() {
        // utility class - prevent instantiation
    }

    /**
     * Parameter name for action callbacks and operations.
     */
    public static final String action = "action";

    /**
     * Parameter name for a single HBase {@code AnyDelete} operation.
     */
    public static final String anyDelete = "anyDelete";

    /**
     * Parameter name for a collection of HBase {@code AnyDelete} operations.
     */
    public static final String anyDeletes = "anyDeletes";

    /**
     * Parameter name for a single HBase {@code AnyGet} operation.
     */
    public static final String anyGet = "anyGet";

    /**
     * Parameter name for a collection of HBase {@code AnyGet} operations.
     */
    public static final String anyGets = "anyGets";

    /**
     * Parameter name for a single HBase {@code AnyPut} operation.
     */
    public static final String anyPut = "anyPut";

    /**
     * Parameter name for a collection of HBase {@code AnyPut} operations.
     */
    public static final String anyPuts = "anyPuts";

    /**
     * Parameter name for an HBase {@code AnyScan} operation.
     */
    public static final String anyScan = "anyScan";

    /**
     * Parameter name for an HBase {@code Append} operation.
     */
    public static final String append = "append";

    /**
     * Parameter name for the executor that runs asynchronous tasks.
     */
    public static final String asyncExecutor = "asyncExecutor";

    /**
     * Parameter name for an asynchronous Cassandra result set.
     */
    public static final String asyncResultSet = "asyncResultSet";

    /**
     * Parameter name for an attribute, property, or column name.
     */
    public static final String attrName = "attrName";

    /**
     * Parameter name for the second attribute, property, or column name.
     */
    public static final String attrName2 = "attrName2";

    /**
     * Parameter name for the third attribute, property, or column name.
     */
    public static final String attrName3 = "attrName3";

    /**
     * Parameter name for a collection of attribute values.
     */
    public static final String attrValues = "attrValues";

    /**
     * Parameter name for a DynamoDB batch-get-item request.
     */
    public static final String batchGetItemRequest = "batchGetItemRequest";

    /**
     * Parameter name for a DynamoDB batch-write-item request.
     */
    public static final String batchWriteItemRequest = "batchWriteItemRequest";

    /**
     * Parameter name for a task that returns a result.
     */
    public static final String callable = "callable";

    /**
     * Parameter name for a callback invoked when an operation completes.
     */
    public static final String callback = "callback";

    /**
     * Parameter name for Class objects (camelCase {@code cls} form).
     */
    public static final String cls = "cls";

    /**
     * Parameter name for a collection of elements.
     */
    public static final String collection = "collection";

    /**
     * Parameter name for the name of a MongoDB collection.
     */
    public static final String collectionName = "collectionName";

    /**
     * Parameter name for a condition used in query filtering.
     */
    public static final String cond = "cond";

    /**
     * Parameter name for a connection or client object.
     */
    public static final String conn = "conn";

    /**
     * Parameter name for count values or counting operations.
     */
    public static final String count = "count";

    /**
     * Parameter name for a CQL statement string.
     */
    public static final String cql = "cql";

    /**
     * Parameter name for a DynamoDB delete-item request.
     */
    public static final String deleteItemRequest = "deleteItemRequest";

    /**
     * Parameter name for the class that documents are decoded into.
     */
    public static final String documentClass = "documentClass";

    /**
     * Parameter name for an HBase mutation durability level.
     */
    public static final String durability = "durability";

    /**
     * Parameter name for a {@code DynamoDBExecutor} instance.
     */
    public static final String dynamoDBExecutor = "dynamoDBExecutor";

    /**
     * Parameter name for a collection of entity objects.
     */
    public static final String entities = "entities";

    /**
     * Parameter name for an entity object.
     */
    public static final String entity = "entity";

    /**
     * Parameter name for Class objects representing entity types.
     */
    public static final String entityClass = "entityClass";

    /**
     * Parameter name for a query or table expression.
     */
    public static final String expr = "expr";

    /**
     * Parameter name for a map from HBase column families to their cells or qualifiers.
     */
    public static final String familyMap = "familyMap";

    /**
     * Parameter name for a field name.
     */
    public static final String fieldName = "fieldName";

    /**
     * Parameter name for a collection of field names.
     */
    public static final String fieldNames = "fieldNames";

    /**
     * Parameter name for a collection of fields.
     */
    public static final String fields = "fields";

    /**
     * Parameter name for a list of field values.
     */
    public static final String fieldValueList = "fieldValueList";

    /**
     * Parameter name for a {@link java.io.File} to read from or write to.
     */
    public static final String file = "file";

    /**
     * Parameter name for a file path.
     */
    public static final String filePath = "filePath";

    /**
     * Parameter name for the collection of files to read from.
     */
    public static final String files = "files";

    /**
     * Parameter name for a query filter or condition.
     */
    public static final String filter = "filter";

    /**
     * Parameter name for the complete text emitted after a FROM keyword.
     */
    public static final String fromClause = "fromClause";

    /**
     * Parameter name for a DynamoDB get-item request.
     */
    public static final String getItemRequest = "getItemRequest";

    /**
     * Parameter name for an {@code HBaseExecutor} instance.
     */
    public static final String hbaseExecutor = "hbaseExecutor";

    /**
     * Parameter name for an identifier.
     */
    public static final String id = "id";

    /**
     * Parameter name for a collection of identifiers.
     */
    public static final String ids = "ids";

    /**
     * Parameter name for an HBase {@code Increment} operation.
     */
    public static final String increment = "increment";

    /**
     * Parameter name for an input stream.
     */
    public static final String is = "is";

    /**
     * Parameter name for a single item.
     */
    public static final String item = "item";

    /**
     * Parameter name for the identifier of an item.
     */
    public static final String itemId = "itemId";

    /**
     * Parameter name for a list of item identities.
     */
    public static final String itemIdentityList = "itemIdentityList";

    /**
     * Parameter name for a key.
     */
    public static final String key = "key";

    /**
     * Parameter name for a key name.
     */
    public static final String keyName = "keyName";

    /**
     * Parameter name for the second key name.
     */
    public static final String keyName2 = "keyName2";

    /**
     * Parameter name for a map function.
     */
    public static final String mapFunction = "mapFunction";

    /**
     * Parameter name for suppliers that create map instances for collecting results.
     */
    public static final String mapSupplier = "mapSupplier";

    /**
     * Parameter name for a {@code MongoDB} instance.
     */
    public static final String mongoDB = "mongoDB";

    /**
     * Parameter name for an array of alternating names and values.
     */
    public static final String nameValuePairs = "nameValuePairs";

    /**
     * Parameter name for the naming policy applied to property or column names.
     */
    public static final String namingPolicy = "namingPolicy";

    /**
     * Parameter name for an object.
     */
    public static final String obj = "obj";

    /**
     * Parameter name for a MongoDB object identifier.
     */
    public static final String objectId = "objectId";

    /**
     * Parameter name for a list of objects.
     */
    public static final String objList = "objList";

    /**
     * Parameter name for offset values used in pagination and result limiting.
     */
    public static final String offset = "offset";

    /**
     * Parameter name for the identifier of an existing item.
     */
    public static final String oldItemId = "oldItemId";

    /**
     * Parameter name for an output stream.
     */
    public static final String os = "os";

    /**
     * Parameter name for a parsed CQL statement.
     */
    public static final String parsedCql = "parsedCql";

    /**
     * Parameter name for a partition key.
     */
    public static final String partitionKey = "partitionKey";

    /**
     * Parameter name for an aggregation pipeline.
     */
    public static final String pipeline = "pipeline";

    /**
     * Parameter name for a single entity property name.
     */
    public static final String propName = "propName";

    /**
     * Parameter name for the map of property names to values.
     */
    public static final String props = "props";

    /**
     * Parameter name for a DynamoDB put-item request.
     */
    public static final String putItemRequest = "putItemRequest";

    /**
     * Parameter name for a query object or query string.
     */
    public static final String query = "query";

    /**
     * Parameter name for a query configuration.
     */
    public static final String queryConfig = "queryConfig";

    /**
     * Parameter name for a query request.
     */
    public static final String queryRequest = "queryRequest";

    /**
     * Parameter name for a query specification.
     */
    public static final String querySpec = "querySpec";

    /**
     * Parameter name for the type that read results are converted to.
     */
    public static final String readType = "readType";

    /**
     * Parameter name for a reduce function.
     */
    public static final String reduceFunction = "reduceFunction";

    /**
     * Parameter name for a replacement object or document.
     */
    public static final String replacement = "replacement";

    /**
     * Parameter name for a collection of requests.
     */
    public static final String requests = "requests";

    /**
     * Parameter name for an HBase {@code RowMutations} instance.
     */
    public static final String rm = "rm";

    /**
     * Parameter name for the class that rows are converted to.
     */
    public static final String rowClass = "rowClass";

    /**
     * Parameter name for the name of the row-key property.
     */
    public static final String rowKeyPropertyName = "rowKeyPropertyName";

    /**
     * Parameter name for a function that maps result rows to objects.
     */
    public static final String rowMapper = "rowMapper";

    /**
     * Parameter name for the type that rows are converted to.
     */
    public static final String rowType = "rowType";

    /**
     * Parameter name for an HBase {@code Scan} operation.
     */
    public static final String scan = "scan";

    /**
     * Parameter name for a scan request.
     */
    public static final String scanRequest = "scanRequest";

    /**
     * Parameter name for a schema definition.
     */
    public static final String schema = "schema";

    /**
     * Parameter name for the SQL dialect used to generate statements.
     */
    public static final String sqlDialect = "sqlDialect";

    /**
     * Parameter name for a statement.
     */
    public static final String statement = "statement";

    /**
     * Parameter name for supplier functions that provide objects or values on demand.
     */
    public static final String supplier = "supplier";

    /**
     * Parameter name for a table name.
     */
    public static final String tableName = "tableName";

    /**
     * Parameter name for the collection or array of table names.
     */
    public static final String tableNames = "tableNames";

    /**
     * Parameter name for a BigQuery table result.
     */
    public static final String tableResult = "tableResult";

    /**
     * Parameter name for Class objects representing target types for mapping or conversion operations.
     */
    public static final String targetClass = "targetClass";

    /**
     * Parameter name for the target entity class.
     */
    public static final String targetEntityClass = "targetEntityClass";

    /**
     * Parameter name for Type objects representing target types for generic operations.
     */
    public static final String targetType = "targetType";

    /**
     * Parameter name for a timestamp.
     */
    public static final String timestamp = "timestamp";

    /**
     * Parameter name for a time-to-live value.
     */
    public static final String ttl = "ttl";

    /**
     * Parameter name for an update document or operation.
     */
    public static final String update = "update";

    /**
     * Parameter name for a DynamoDB update-item request.
     */
    public static final String updateItemRequest = "updateItemRequest";

    /**
     * Parameter name for the class that values are converted to.
     */
    public static final String valueClass = "valueClass";

    /**
     * Parameter name for the target type values are converted to.
     */
    public static final String valueType = "valueType";

    /**
     * Parameter name for a WHERE clause.
     */
    public static final String whereClause = "whereClause";
}
