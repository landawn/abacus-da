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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.landawn.abacus.DirtyMarker;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.EntityInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.BooleanList;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.function.Supplier;
import com.landawn.abacus.util.stream.ObjIteratorEx;
import com.landawn.abacus.util.stream.Stream;

// TODO: Auto-generated Javadoc
/**
 * It's a simple wrapper of HBase Java client.
 *
 * @author Haiyang Li
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">org.apache.hadoop.hbase.client.Table</a>
 * @since 0.8
 */
public final class HBaseExecutor implements Closeable {

    /** The Constant EMPTY_QULIFIER. */
    private static final String EMPTY_QULIFIER = N.EMPTY_STRING;

    /** The Constant familyQualifierBytesPool. */
    private static final Map<String, byte[]> familyQualifierBytesPool = new ConcurrentHashMap<>();

    /** The Constant classRowkeySetMethodPool. */
    private static final Map<Class<?>, Method> classRowkeySetMethodPool = new ConcurrentHashMap<>();

    /** The admin. */
    private final Admin admin;

    /** The conn. */
    private final Connection conn;

    /** The async H base executor. */
    private final AsyncHBaseExecutor asyncHBaseExecutor;

    /**
     * Instantiates a new h base executor.
     *
     * @param conn the conn
     */
    public HBaseExecutor(final Connection conn) {
        this(conn, new AsyncExecutor(64, 300, TimeUnit.SECONDS));
    }

    /**
     * Instantiates a new h base executor.
     *
     * @param conn the conn
     * @param asyncExecutor the async executor
     */
    public HBaseExecutor(final Connection conn, final AsyncExecutor asyncExecutor) {
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        this.conn = conn;

        this.asyncHBaseExecutor = new AsyncHBaseExecutor(this, asyncExecutor);
    }

    /**
     * Admin.
     *
     * @return the admin
     */
    public Admin admin() {
        return admin;
    }

    /**
     * Connection.
     *
     * @return the connection
     */
    public Connection connection() {
        return conn;
    }

    /**
     * Async.
     *
     * @return the async H base executor
     */
    public AsyncHBaseExecutor async() {
        return asyncHBaseExecutor;
    }

    /**
     * The row key property will be read from/write to the specified property.
     *
     * @param cls entity classes with getter/setter methods
     * @param rowKeyPropertyName the row key property name
     */
    public static void registerRowKeyProperty(final Class<?> cls, final String rowKeyPropertyName) {
        if (ClassUtil.getPropGetMethod(cls, rowKeyPropertyName) == null || ClassUtil.getPropSetMethod(cls, rowKeyPropertyName) == null) {
            throw new IllegalArgumentException("The specified class: " + ClassUtil.getCanonicalClassName(cls)
                    + " doesn't have getter or setter method for the specified row key propery: " + rowKeyPropertyName);
        }

        final Method setMethod = ClassUtil.getPropSetMethod(cls, rowKeyPropertyName);
        final Class<?> parameterType = setMethod.getParameterTypes()[0];
        Class<?>[] typeArgs = ClassUtil.getTypeArgumentsByMethod(setMethod);

        if (HBaseColumn.class.equals(parameterType) || (typeArgs.length == 1 && typeArgs[0].equals(HBaseColumn.class))
                || (typeArgs.length == 2 && typeArgs[1].equals(HBaseColumn.class))) {
            throw new IllegalArgumentException(
                    "Unsupported row key property type: " + setMethod.toGenericString() + ". The row key property type can't be be HBaseColumn");
        }

        classRowkeySetMethodPool.put(cls, setMethod);
    }

    /**
     * Gets the row key set method.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @return the row key set method
     */
    private static <T> Method getRowKeySetMethod(final Class<T> targetClass) {
        Method rowKeySetMethod = classRowkeySetMethodPool.get(targetClass);

        //        if (rowKeySetMethod == null) {
        //            Method setMethod = N.getPropSetMethod(targetClass, "id");
        //            Class<?> parameterType = setMethod == null ? null : setMethod.getParameterTypes()[0];
        //
        //            if (parameterType != null && HBaseColumn.class.equals(parameterType) && String.class.equals(N.getTypeArgumentsByMethod(setMethod)[0])) {
        //                rowKeySetMethod = setMethod;
        //            }
        //
        //            if (rowKeySetMethod == null) {
        //                rowKeySetMethod = N.METHOD_MASK;
        //            }
        //
        //            classRowkeySetMethodPool.put(targetClass, rowKeySetMethod);
        //        }

        return rowKeySetMethod == ClazzUtil.METHOD_MASK ? null : rowKeySetMethod;
    }

    /**
     * To list.
     *
     * @param <T> the generic type
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param resultScanner the result scanner
     * @return the list
     */
    public static <T> List<T> toList(final Class<T> targetClass, final ResultScanner resultScanner) {
        return toList(targetClass, resultScanner, 0, Integer.MAX_VALUE);
    }

    /**
     * To list.
     *
     * @param <T> the generic type
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param resultScanner the result scanner
     * @param offset the offset
     * @param count the count
     * @return the list
     */
    public static <T> List<T> toList(final Class<T> targetClass, final ResultScanner resultScanner, int offset, int count) {
        if (offset < 0 || count < 0) {
            throw new IllegalArgumentException("Offset and count can't be negative");
        }

        final Type<T> type = N.typeOf(targetClass);
        final List<T> resultList = new ArrayList<>();

        try {
            while (offset-- > 0 && resultScanner.next() != null) {

            }

            Result result = null;

            while (count-- > 0 && (result = resultScanner.next()) != null) {
                resultList.add(toValue(type, targetClass, result));
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return resultList;
    }

    /**
     * To list.
     *
     * @param <T> the generic type
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param results the results
     * @return the list
     */
    static <T> List<T> toList(final Class<T> targetClass, final List<Result> results) {
        final Type<T> type = N.typeOf(targetClass);
        final List<T> resultList = new ArrayList<>(results.size());

        try {
            for (Result result : results) {
                resultList.add(toValue(type, targetClass, result));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return resultList;
    }

    /**
     * To entity.
     *
     * @param <T> the generic type
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param result the result
     * @return the t
     */
    public static <T> T toEntity(final Class<T> targetClass, final Result result) {
        final Type<T> type = N.typeOf(targetClass);

        try {
            return toValue(type, targetClass, result);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * To value.
     *
     * @param <T> the generic type
     * @param type the type
     * @param targetClass the target class
     * @param result the result
     * @return the t
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("deprecation")
    private static <T> T toValue(final Type<T> type, final Class<T> targetClass, final Result result) throws IOException {
        if (type.isMap()) {
            throw new IllegalArgumentException("Map is not supported");
        } else if (type.isEntity()) {
            if (result.advance() == false) {
                return null;
            }

            final T entity = N.newInstance(targetClass);
            final CellScanner cellScanner = result.cellScanner();

            final EntityInfo entityInfo = ParserUtil.getEntityInfo(targetClass);
            final Map<String, Map<String, Type<?>>> familyColumnValueTypeMap = new HashMap<>();
            final Map<String, Map<String, Collection<HBaseColumn<?>>>> familyColumnCollectionMap = new HashMap<>();
            final Map<String, Map<String, Map<Long, HBaseColumn<?>>>> familyColumnMapMap = new HashMap<>();

            final Method rowKeySetMethod = getRowKeySetMethod(targetClass);
            final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);

            Object rowKey = null;
            String family = null;
            String qualifier = null;
            PropInfo familyPropInfo = null;
            PropInfo columnPropInfo = null;
            Type<?> columnValueType = null;

            Map<String, Type<?>> columnValueTypeMap = null;
            Collection<HBaseColumn<?>> columnColl = null;
            Map<String, Collection<HBaseColumn<?>>> columnCollectionMap = null;
            Map<Long, HBaseColumn<?>> columnMap = null;
            Map<String, Map<Long, HBaseColumn<?>>> columnMapMap = null;
            HBaseColumn<?> column = null;
            Method addMethod = null;

            while (cellScanner.advance()) {
                final Cell cell = cellScanner.current();

                if (rowKeyType != null && rowKey == null) {
                    rowKey = rowKeyType.valueOf(getRowKeyString(cell));
                    ClassUtil.setPropValue(entity, rowKeySetMethod, rowKey);
                }

                family = getFamilyString(cell);
                qualifier = getQualifierString(cell);

                // .....................................................................................
                columnMapMap = familyColumnMapMap.get(family);

                if (N.notNullOrEmpty(columnMapMap)) {
                    columnMap = columnMapMap.get(qualifier);

                    if (N.notNullOrEmpty(columnMap)) {
                        columnValueType = familyColumnValueTypeMap.get(family).get(qualifier);
                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                        columnMapMap.put(qualifier, columnMap);

                        continue;
                    }
                }

                // .....................................................................................
                columnCollectionMap = familyColumnCollectionMap.get(family);
                if (N.notNullOrEmpty(columnCollectionMap)) {
                    columnColl = columnCollectionMap.get(qualifier);

                    if (N.notNullOrEmpty(columnColl)) {
                        columnValueType = familyColumnValueTypeMap.get(family).get(qualifier);
                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                        columnColl.add(column);

                        continue;
                    }
                }

                // .....................................................................................
                familyPropInfo = entityInfo.getPropInfo(family);

                // ignore the unknown property:
                if (familyPropInfo == null) {
                    continue;
                }

                columnValueTypeMap = familyColumnValueTypeMap.get(family);

                if (columnValueTypeMap == null) {
                    columnValueTypeMap = new HashMap<>();

                    familyColumnValueTypeMap.put(family, columnValueTypeMap);
                }

                if (familyPropInfo.jsonXmlType.isEntity()) {
                    final Class<?> propEntityClass = familyPropInfo.jsonXmlType.clazz();
                    final EntityInfo propEntityInfo = ParserUtil.getEntityInfo(propEntityClass);
                    Object propEntity = ClassUtil.getPropValue(entity, ClassUtil.getPropGetMethod(targetClass, family));

                    if (propEntity == null) {
                        propEntity = N.newInstance(propEntityClass);

                        ClassUtil.setPropValue(entity, familyPropInfo.setMethod, propEntity);
                    }

                    columnPropInfo = propEntityInfo.getPropInfo(qualifier);

                    // ignore the unknown property.
                    if (columnPropInfo == null) {
                        continue;
                    }

                    if (columnPropInfo.jsonXmlType.isMap() && columnPropInfo.jsonXmlType.getParameterTypes()[1].clazz().equals(HBaseColumn.class)) {
                        addMethod = ClassUtil.getDeclaredMethod(propEntityClass, getAddMethodName(columnPropInfo.setMethod), HBaseColumn.class);
                        columnValueType = N.typeOf(ClassUtil.getTypeArgumentsByMethod(addMethod)[0]);
                        columnValueTypeMap.put(qualifier, columnValueType);
                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                        ClassUtil.invokeMethod(propEntity, addMethod, column);

                        columnMap = ClassUtil.getPropValue(propEntity, ClassUtil.getPropGetMethod(propEntityClass, qualifier));

                        if (columnMapMap == null) {
                            columnMapMap = new HashMap<>();
                            familyColumnMapMap.put(family, columnMapMap);
                        }

                        columnMapMap.put(qualifier, columnMap);
                    } else if (columnPropInfo.jsonXmlType.isCollection()
                            && columnPropInfo.jsonXmlType.getParameterTypes()[0].clazz().equals(HBaseColumn.class)) {
                        addMethod = ClassUtil.getDeclaredMethod(propEntityClass, getAddMethodName(columnPropInfo.setMethod), HBaseColumn.class);
                        columnValueType = N.typeOf(ClassUtil.getTypeArgumentsByMethod(addMethod)[0]);
                        columnValueTypeMap.put(qualifier, columnValueType);
                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                        ClassUtil.invokeMethod(propEntity, addMethod, column);

                        columnColl = ClassUtil.getPropValue(propEntity, ClassUtil.getPropGetMethod(propEntityClass, qualifier));

                        if (columnCollectionMap == null) {
                            columnCollectionMap = new HashMap<>();
                            familyColumnCollectionMap.put(family, columnCollectionMap);
                        }

                        columnCollectionMap.put(qualifier, columnColl);
                    } else if (columnPropInfo.jsonXmlType.clazz().equals(HBaseColumn.class)) {
                        columnValueType = columnValueTypeMap.get(qualifier);

                        if (columnValueType == null) {
                            columnValueType = columnPropInfo.jsonXmlType.getParameterTypes()[0];
                            columnValueTypeMap.put(qualifier, columnValueType);
                        }

                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                        ClassUtil.setPropValue(propEntity, columnPropInfo.setMethod, column);
                    } else {
                        ClassUtil.setPropValue(propEntity, columnPropInfo.setMethod, columnPropInfo.jsonXmlType.valueOf(getValueString(cell)));
                    }

                } else if (familyPropInfo.jsonXmlType.isMap() && familyPropInfo.jsonXmlType.getParameterTypes()[1].clazz().equals(HBaseColumn.class)) {
                    addMethod = ClassUtil.getDeclaredMethod(targetClass, getAddMethodName(familyPropInfo.setMethod), HBaseColumn.class);
                    columnValueType = N.typeOf(ClassUtil.getTypeArgumentsByMethod(addMethod)[0]);
                    columnValueTypeMap.put(qualifier, columnValueType);
                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                    ClassUtil.invokeMethod(entity, addMethod, column);

                    columnMap = ClassUtil.getPropValue(entity, ClassUtil.getPropGetMethod(targetClass, family));

                    if (columnMapMap == null) {
                        columnMapMap = new HashMap<>();
                        familyColumnMapMap.put(family, columnMapMap);
                    }

                    columnMapMap.put(qualifier, columnMap);
                } else if (familyPropInfo.jsonXmlType.isCollection() && familyPropInfo.jsonXmlType.getParameterTypes()[0].clazz().equals(HBaseColumn.class)) {
                    addMethod = ClassUtil.getDeclaredMethod(targetClass, getAddMethodName(familyPropInfo.setMethod), HBaseColumn.class);
                    columnValueType = N.typeOf(ClassUtil.getTypeArgumentsByMethod(addMethod)[0]);
                    columnValueTypeMap.put(qualifier, columnValueType);
                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                    ClassUtil.invokeMethod(entity, addMethod, column);

                    columnColl = ClassUtil.getPropValue(entity, ClassUtil.getPropGetMethod(targetClass, family));

                    if (columnCollectionMap == null) {
                        columnCollectionMap = new HashMap<>();
                        familyColumnCollectionMap.put(family, columnCollectionMap);
                    }

                    columnCollectionMap.put(qualifier, columnColl);
                } else if (familyPropInfo.jsonXmlType.clazz().equals(HBaseColumn.class)) {
                    columnValueType = columnValueTypeMap.get(qualifier);

                    if (columnValueType == null) {
                        columnValueType = familyPropInfo.jsonXmlType.getParameterTypes()[0];
                        columnValueTypeMap.put(qualifier, columnValueType);
                    }

                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                    ClassUtil.setPropValue(entity, familyPropInfo.setMethod, column);
                } else {
                    ClassUtil.setPropValue(entity, familyPropInfo.setMethod, familyPropInfo.jsonXmlType.valueOf(getValueString(cell)));
                }
            }

            if (ClassUtil.isDirtyMarker(entity.getClass())) {
                ((DirtyMarker) entity).markDirty(false);
            }

            return entity;
        } else {
            if (result.advance() == false) {
                return type.defaultValue();
            }

            final CellScanner cellScanner = result.cellScanner();

            if (cellScanner.advance() == false) {
                return type.defaultValue();
            }

            final Cell cell = cellScanner.current();

            T value = type.valueOf(getValueString(cell));

            if (cellScanner.advance()) {
                throw new IllegalArgumentException("Can't covert result with columns: " + getFamilyString(cell) + ":" + getQualifierString(cell) + " to class: "
                        + ClassUtil.getCanonicalClassName(type.clazz()));
            }

            return value;
        }
    }

    /**
     * Check entity class.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     */
    private static <T> void checkEntityClass(final Class<T> targetClass) {
        if (!ClassUtil.isEntity(targetClass)) {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(targetClass)
                    + ". Only Entity class generated by CodeGenerator with getter/setter methods are supported");
        }
    }

    /**
     *  
     *
     * @param obj entity with getter/setter methods
     * @return the any put
     */
    public static AnyPut toAnyPut(final Object obj) {
        return toAnyPut(obj, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     * To any put.
     *
     * @param obj entity with getter/setter methods
     * @param namingPolicy the naming policy
     * @return the any put
     */
    public static AnyPut toAnyPut(final Object obj, final NamingPolicy namingPolicy) {
        return toAnyPut(null, obj, namingPolicy);
    }

    /**
     * To any put.
     *
     * @param outputAnyPut the output result if it's not null.
     * @param obj entity with getter/setter methods
     * @return the any put
     */
    static AnyPut toAnyPut(final AnyPut outputAnyPut, final Object obj) {
        return toAnyPut(outputAnyPut, obj, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     * To any put.
     *
     * @param outputAnyPut the output any put
     * @param obj entity with getter/setter methods
     * @param namingPolicy the naming policy
     * @return the any put
     */
    static AnyPut toAnyPut(final AnyPut outputAnyPut, final Object obj, final NamingPolicy namingPolicy) {
        if (obj instanceof AnyPut) {
            if (outputAnyPut != null) {
                throw new IllegalArgumentException("Merge is not supported. The specified entity object is already a AnyPut and outputAnyPut is not null");
            }

            return (AnyPut) obj;
        }

        final Class<?> cls = obj.getClass();

        checkEntityClass(cls);

        final EntityInfo entityInfo = ParserUtil.getEntityInfo(cls);
        final Method rowKeySetMethod = getRowKeySetMethod(cls);
        final Method rowKeyGetMethod = rowKeySetMethod == null ? null : ClassUtil.getPropGetMethod(cls, ClassUtil.getPropNameByMethod(rowKeySetMethod));

        if (outputAnyPut == null && rowKeySetMethod == null) {
            throw new IllegalArgumentException(
                    "Row key property is required to create AnyPut instance. But no row key property found in class: " + ClassUtil.getCanonicalClassName(cls));
        }

        final AnyPut anyPut = outputAnyPut == null ? new AnyPut(ClassUtil.<Object> getPropValue(obj, rowKeyGetMethod)) : outputAnyPut;
        final Map<String, Method> familyGetMethodMap = ClassUtil.getPropGetMethodList(cls);

        String familyName = null;
        PropInfo familyPropInfo = null;
        String columnName = null;
        PropInfo columnPropInfo = null;
        Collection<HBaseColumn<?>> columnColl = null;
        Map<Long, HBaseColumn<?>> columnMap = null;
        HBaseColumn<?> column = null;
        Object propValue = null;

        for (Map.Entry<String, Method> familyGetMethodEntry : familyGetMethodMap.entrySet()) {
            familyPropInfo = entityInfo.getPropInfo(familyGetMethodEntry.getKey());

            if (rowKeyGetMethod != null && familyPropInfo.getMethod.equals(rowKeyGetMethod)) {
                continue;
            }

            familyName = formatName(familyGetMethodEntry.getKey(), namingPolicy);
            propValue = ClassUtil.getPropValue(obj, familyPropInfo.getMethod);

            if (propValue == null) {
                continue;
            }

            if (familyPropInfo.jsonXmlType.isEntity()) {
                final Class<?> propEntityClass = familyPropInfo.jsonXmlType.clazz();
                final EntityInfo propEntityInfo = ParserUtil.getEntityInfo(propEntityClass);
                final Object propEntity = propValue;

                final Map<String, Method> columnGetMethodMap = ClassUtil.getPropGetMethodList(propEntityClass);

                for (Map.Entry<String, Method> columnGetMethodEntry : columnGetMethodMap.entrySet()) {
                    columnPropInfo = propEntityInfo.getPropInfo(columnGetMethodEntry.getKey());
                    columnName = formatName(columnGetMethodEntry.getKey(), namingPolicy);

                    propValue = ClassUtil.getPropValue(propEntity, columnPropInfo.getMethod);

                    if (propValue == null) {
                        continue;
                    }

                    if (columnPropInfo.jsonXmlType.isMap() && columnPropInfo.jsonXmlType.getParameterTypes()[1].clazz().equals(HBaseColumn.class)) {
                        columnMap = (Map<Long, HBaseColumn<?>>) propValue;

                        for (HBaseColumn<?> e : columnMap.values()) {
                            anyPut.addColumn(familyName, columnName, e.version(), e.value());

                        }
                    } else if (columnPropInfo.jsonXmlType.isCollection()
                            && columnPropInfo.jsonXmlType.getParameterTypes()[0].clazz().equals(HBaseColumn.class)) {
                        columnColl = (Collection<HBaseColumn<?>>) propValue;

                        for (HBaseColumn<?> e : columnColl) {
                            anyPut.addColumn(familyName, columnName, e.version(), e.value());

                        }
                    } else if (columnPropInfo.jsonXmlType.clazz().equals(HBaseColumn.class)) {
                        column = (HBaseColumn<?>) propValue;
                        anyPut.addColumn(familyName, columnName, column.version(), column.value());
                    } else {
                        anyPut.addColumn(familyName, columnName, propValue);
                    }
                }
            } else if (familyPropInfo.jsonXmlType.isMap() && familyPropInfo.jsonXmlType.getParameterTypes()[1].clazz().equals(HBaseColumn.class)) {
                columnMap = (Map<Long, HBaseColumn<?>>) propValue;

                for (HBaseColumn<?> e : columnMap.values()) {
                    anyPut.addColumn(familyName, EMPTY_QULIFIER, e.version(), e.value());

                }
            } else if (familyPropInfo.jsonXmlType.isCollection() && familyPropInfo.jsonXmlType.getParameterTypes()[0].clazz().equals(HBaseColumn.class)) {
                columnColl = (Collection<HBaseColumn<?>>) propValue;

                for (HBaseColumn<?> e : columnColl) {
                    anyPut.addColumn(familyName, EMPTY_QULIFIER, e.version(), e.value());

                }
            } else if (familyPropInfo.jsonXmlType.clazz().equals(HBaseColumn.class)) {
                column = (HBaseColumn<?>) propValue;
                anyPut.addColumn(familyName, EMPTY_QULIFIER, column.version(), column.value());
            } else {
                anyPut.addColumn(familyName, EMPTY_QULIFIER, propValue);
            }
        }

        return anyPut;
    }

    /**
     * Format name.
     *
     * @param name the name
     * @param namingPolicy the naming policy
     * @return the string
     */
    private static String formatName(String name, final NamingPolicy namingPolicy) {
        switch (namingPolicy) {
            case LOWER_CASE_WITH_UNDERSCORE:
                return ClassUtil.toLowerCaseWithUnderscore(name);
            case UPPER_CASE_WITH_UNDERSCORE:
                return ClassUtil.toUpperCaseWithUnderscore(name);
            case LOWER_CAMEL_CASE:
                return name;
            default:
                throw new IllegalArgumentException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * To any put.
     *
     * @param objs <code>AnyPut</code> or entity with getter/setter methods
     * @return the list
     */
    public static List<AnyPut> toAnyPut(final Collection<?> objs) {
        final List<AnyPut> anyPuts = new ArrayList<>(objs.size());

        for (Object entity : objs) {
            anyPuts.add(entity instanceof AnyPut ? (AnyPut) entity : toAnyPut(entity));
        }

        return anyPuts;
    }

    /**
     * To any put.
     *
     * @param objs <code>AnyPut</code> or entity with getter/setter methods
     * @param namingPolicy the naming policy
     * @return the list
     */
    public static List<AnyPut> toAnyPut(final Collection<?> objs, final NamingPolicy namingPolicy) {
        final List<AnyPut> anyPuts = new ArrayList<>(objs.size());

        for (Object entity : objs) {
            anyPuts.add(entity instanceof AnyPut ? (AnyPut) entity : toAnyPut(entity, namingPolicy));
        }

        return anyPuts;
    }

    /**
     * To put.
     *
     * @param objs <code>AnyPut</code> or entity with getter/setter methods
     * @return the list
     */
    public static List<Put> toPut(final Collection<?> objs) {
        final List<Put> puts = new ArrayList<>(objs.size());

        for (Object entity : objs) {
            puts.add(entity instanceof AnyPut ? ((AnyPut) entity).val() : toAnyPut(entity).val());
        }

        return puts;
    }

    /**
     * To put.
     *
     * @param objs <code>AnyPut</code> or entity with getter/setter methods
     * @param namingPolicy the naming policy
     * @return the list
     */
    public static List<Put> toPut(final Collection<?> objs, final NamingPolicy namingPolicy) {
        final List<Put> puts = new ArrayList<>(objs.size());

        for (Object entity : objs) {
            puts.add(entity instanceof AnyPut ? ((AnyPut) entity).val() : toAnyPut(entity, namingPolicy).val());
        }

        return puts;
    }

    /**
     * To get.
     *
     * @param anyGets the any gets
     * @return the list
     */
    public static List<Get> toGet(final Collection<AnyGet> anyGets) {
        final List<Get> gets = new ArrayList<>(anyGets.size());

        for (AnyGet anyGet : anyGets) {
            gets.add(anyGet.val());
        }

        return gets;
    }

    /**
     * To delete.
     *
     * @param anyDeletes the any deletes
     * @return the list
     */
    public static List<Delete> toDelete(final Collection<AnyDelete> anyDeletes) {
        final List<Delete> deletes = new ArrayList<>(anyDeletes.size());

        for (AnyDelete anyDelete : anyDeletes) {
            deletes.add(anyDelete.val());
        }

        return deletes;
    }

    /**
     * Gets the table.
     *
     * @param tableName the table name
     * @return the table
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Table getTable(final String tableName) throws UncheckedIOException {
        try {
            return conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Close quietly.
     *
     * @param table the table
     */
    private void closeQuietly(final Table table) {
        IOUtil.closeQuietly(table);
    }

    // There is no too much benefit to add method for "Object rowKey"
    /**
     * Exists.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @return true, if successful
     * @throws UncheckedIOException the unchecked IO exception
     */
    // And it may cause error because the "Object" is ambiguous to any type. 
    boolean exists(final String tableName, final Object rowKey) throws UncheckedIOException {
        return exists(tableName, AnyGet.of(rowKey));
    }

    /**
     * Exists.
     *
     * @param tableName the table name
     * @param get the get
     * @return true, if successful
     * @throws UncheckedIOException the unchecked IO exception
     */
    public boolean exists(final String tableName, final Get get) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.exists(get);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Exists.
     *
     * @param tableName the table name
     * @param gets the gets
     * @return the list
     * @throws UncheckedIOException the unchecked IO exception
     */
    public List<Boolean> exists(final String tableName, final List<Get> gets) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return BooleanList.of(table.exists(gets)).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
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
     * @throws UncheckedIOException the unchecked IO exception
     * @deprecated since 2.0 version and will be removed in 3.0 version.
     *             use {@link #exists(List)}
     */
    @Deprecated
    public List<Boolean> existsAll(final String tableName, final List<Get> gets) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return BooleanList.of(table.existsAll(gets)).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Exists.
     *
     * @param tableName the table name
     * @param anyGet the any get
     * @return true, if successful
     * @throws UncheckedIOException the unchecked IO exception
     */
    public boolean exists(final String tableName, final AnyGet anyGet) throws UncheckedIOException {
        return exists(tableName, anyGet.val());
    }

    /**
     * Exists.
     *
     * @param tableName the table name
     * @param anyGets the any gets
     * @return the list
     * @throws UncheckedIOException the unchecked IO exception
     */
    public List<Boolean> exists(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return existsAll(tableName, toGet(anyGets));
    }

    /**
     * Exists all.
     *
     * @param tableName the table name
     * @param anyGets the any gets
     * @return the list
     * @throws UncheckedIOException the unchecked IO exception
     * @deprecated  use {@link #exists(String, Collection)}
     */
    @Deprecated
    public List<Boolean> existsAll(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return existsAll(tableName, toGet(anyGets));
    }

    // There is no too much benefit to add method for "Object rowKey"
    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @return the result
     * @throws UncheckedIOException the unchecked IO exception
     */
    // And it may cause error because the "Object" is ambiguous to any type. 
    Result get(final String tableName, final Object rowKey) throws UncheckedIOException {
        return get(tableName, AnyGet.of(rowKey));
    }

    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param get the get
     * @return the result
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result get(final String tableName, final Get get) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.get(get);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param gets the gets
     * @return the list
     * @throws UncheckedIOException the unchecked IO exception
     */
    public List<Result> get(final String tableName, final List<Get> gets) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return N.asList(table.get(gets));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param anyGet the any get
     * @return the result
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result get(final String tableName, final AnyGet anyGet) throws UncheckedIOException {
        return get(tableName, anyGet.val());
    }

    /**
     * Gets the.
     *
     * @param tableName the table name
     * @param anyGets the any gets
     * @return the list
     * @throws UncheckedIOException the unchecked IO exception
     */
    public List<Result> get(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return get(tableName, toGet(anyGets));
    }

    // There is no too much benefit to add method for "Object rowKey"
    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param rowKey the row key
     * @return the t
     * @throws UncheckedIOException the unchecked IO exception
     */
    // And it may cause error because the "Object" is ambiguous to any type. 
    <T> T get(final Class<T> targetClass, final String tableName, final Object rowKey) throws UncheckedIOException {
        return get(targetClass, tableName, AnyGet.of(rowKey));
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param get the get
     * @return the t
     * @throws UncheckedIOException the unchecked IO exception
     */
    public <T> T get(final Class<T> targetClass, final String tableName, final Get get) throws UncheckedIOException {
        return toEntity(targetClass, get(tableName, get));
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param gets the gets
     * @return the list
     * @throws UncheckedIOException the unchecked IO exception
     */
    public <T> List<T> get(final Class<T> targetClass, final String tableName, final List<Get> gets) throws UncheckedIOException {
        return toList(targetClass, get(tableName, gets));
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param anyGet the any get
     * @return the t
     * @throws UncheckedIOException the unchecked IO exception
     */
    public <T> T get(final Class<T> targetClass, final String tableName, final AnyGet anyGet) throws UncheckedIOException {
        return toEntity(targetClass, get(tableName, anyGet));
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param anyGets the any gets
     * @return the list
     * @throws UncheckedIOException the unchecked IO exception
     */
    public <T> List<T> get(final Class<T> targetClass, final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return toList(targetClass, get(tableName, anyGets));
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param scan the scan
     * @return the stream
     */
    public Stream<Result> scan(final String tableName, final Scan scan) {
        N.checkArgNotNull(tableName, "tableName");
        N.checkArgNotNull(scan, "scan");

        final ObjIteratorEx<Result> lazyIter = ObjIteratorEx.of(new Supplier<ObjIteratorEx<Result>>() {
            private ObjIteratorEx<Result> internalIter = null;

            @Override
            public ObjIteratorEx<Result> get() {
                if (internalIter == null) {
                    final Table table = getTable(tableName);

                    try {
                        final ResultScanner resultScanner = table.getScanner(scan);
                        final Iterator<Result> iter = resultScanner.iterator();

                        internalIter = new ObjIteratorEx<Result>() {
                            @Override
                            public boolean hasNext() {
                                return iter.hasNext();
                            }

                            @Override
                            public Result next() {
                                return iter.next();
                            }

                            @Override
                            public void close() {
                                try {
                                    IOUtil.closeQuietly(resultScanner);
                                } finally {
                                    IOUtil.closeQuietly(table);
                                }
                            }
                        };
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        if (internalIter == null) {
                            IOUtil.closeQuietly(table);
                        }
                    }
                }

                return internalIter;
            }
        });

        return Stream.of(lazyIter).onClose(new Runnable() {
            @Override
            public void run() {
                lazyIter.close();
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param anyScan the any scan
     * @return the stream
     */
    public Stream<Result> scan(final String tableName, final AnyScan anyScan) {
        return scan(tableName, anyScan.val());
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param family the family
     * @return the stream
     */
    public Stream<Result> scan(final String tableName, final String family) {
        return scan(tableName, AnyScan.create().addFamily(family));
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param family the family
     * @param qualifier the qualifier
     * @return the stream
     */
    public Stream<Result> scan(final String tableName, final String family, final String qualifier) {
        return scan(tableName, AnyScan.create().addColumn(family, qualifier));
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param family the family
     * @return the stream
     */
    public Stream<Result> scan(final String tableName, final byte[] family) {
        return scan(tableName, AnyScan.create().addFamily(family));
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param family the family
     * @param qualifier the qualifier
     * @return the stream
     */
    public Stream<Result> scan(final String tableName, final byte[] family, final byte[] qualifier) {
        return scan(tableName, AnyScan.create().addColumn(family, qualifier));
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param scan the scan
     * @return the stream
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final Scan scan) {
        return scan(tableName, scan).map(toEntity(targetClass));
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param anyScan the any scan
     * @return the stream
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final AnyScan anyScan) {
        return scan(tableName, anyScan).map(toEntity(targetClass));
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param family the family
     * @return the stream
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final String family) {
        return scan(tableName, family).map(toEntity(targetClass));
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param family the family
     * @param qualifier the qualifier
     * @return the stream
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final String family, final String qualifier) {
        return scan(tableName, family, qualifier).map(toEntity(targetClass));
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param family the family
     * @return the stream
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final byte[] family) {
        return scan(tableName, family).map(toEntity(targetClass));
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param family the family
     * @param qualifier the qualifier
     * @return the stream
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final byte[] family, final byte[] qualifier) {
        return scan(tableName, family, qualifier).map(toEntity(targetClass));
    }

    /**
     * To entity.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @return the function
     */
    private <T> Function<Result, T> toEntity(final Class<T> targetClass) {
        return new Function<Result, T>() {
            @Override
            public T apply(Result t) {
                return toEntity(targetClass, t);
            }
        };
    }

    /**
     * Put.
     *
     * @param tableName the table name
     * @param put the put
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void put(final String tableName, final Put put) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.put(put);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Put.
     *
     * @param tableName the table name
     * @param puts the puts
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void put(final String tableName, final List<Put> puts) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.put(puts);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Put.
     *
     * @param tableName the table name
     * @param anyPut the any put
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void put(final String tableName, final AnyPut anyPut) throws UncheckedIOException {
        put(tableName, anyPut.val());
    }

    /**
     * Put.
     *
     * @param tableName the table name
     * @param anyPuts the any puts
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void put(final String tableName, final Collection<AnyPut> anyPuts) throws UncheckedIOException {
        put(tableName, toPut(anyPuts));
    }

    // There is no too much benefit to add method for "Object rowKey"
    /**
     * Delete.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @throws UncheckedIOException the unchecked IO exception
     */
    // And it may cause error because the "Object" is ambiguous to any type. 
    void delete(final String tableName, final Object rowKey) throws UncheckedIOException {
        delete(tableName, AnyDelete.of(rowKey));
    }

    /**
     * Delete.
     *
     * @param tableName the table name
     * @param delete the delete
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void delete(final String tableName, final Delete delete) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Delete.
     *
     * @param tableName the table name
     * @param deletes the deletes
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void delete(final String tableName, final List<Delete> deletes) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.delete(deletes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Delete.
     *
     * @param tableName the table name
     * @param anyDelete the any delete
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void delete(final String tableName, final AnyDelete anyDelete) throws UncheckedIOException {
        delete(tableName, anyDelete.val());
    }

    /**
     * Delete.
     *
     * @param tableName the table name
     * @param anyDeletes the any deletes
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void delete(final String tableName, final Collection<AnyDelete> anyDeletes) throws UncheckedIOException {
        delete(tableName, toDelete(anyDeletes));
    }

    /**
     * Mutate row.
     *
     * @param tableName the table name
     * @param rm the rm
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void mutateRow(final String tableName, final RowMutations rm) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.mutateRow(rm);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Append.
     *
     * @param tableName the table name
     * @param append the append
     * @return the result
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result append(final String tableName, final Append append) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.append(append);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Increment.
     *
     * @param tableName the table name
     * @param increment the increment
     * @return the result
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result increment(final String tableName, final Increment increment) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.increment(increment);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Increment column value.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @param family the family
     * @param qualifier the qualifier
     * @param amount the amount
     * @return the long
     * @throws UncheckedIOException the unchecked IO exception
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier, final long amount)
            throws UncheckedIOException {
        return incrementColumnValue(tableName, rowKey, toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount);
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
     * @return the long
     * @throws UncheckedIOException the unchecked IO exception
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier, final long amount,
            final Durability durability) throws UncheckedIOException {
        return incrementColumnValue(tableName, rowKey, toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount, durability);
    }

    /**
     * Increment column value.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @param family the family
     * @param qualifier the qualifier
     * @param amount the amount
     * @return the long
     * @throws UncheckedIOException the unchecked IO exception
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier, final long amount)
            throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.incrementColumnValue(toRowKeyBytes(rowKey), family, qualifier, amount);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
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
     * @return the long
     * @throws UncheckedIOException the unchecked IO exception
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier, final long amount,
            final Durability durability) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.incrementColumnValue(toRowKeyBytes(rowKey), family, qualifier, amount, durability);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Coprocessor service.
     *
     * @param tableName the table name
     * @param rowKey the row key
     * @return the coprocessor rpc channel
     */
    public CoprocessorRpcChannel coprocessorService(final String tableName, final Object rowKey) {
        final Table table = getTable(tableName);

        try {
            return table.coprocessorService(toRowKeyBytes(rowKey));
        } finally {
            closeQuietly(table);
        }
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
     * @return the map
     * @throws UncheckedIOException the unchecked IO exception
     * @throws Exception the exception
     */
    public <T extends Service, R> Map<byte[], R> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable) throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            return table.coprocessorService(service, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), callable);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
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
     * @throws UncheckedIOException the unchecked IO exception
     * @throws Exception the exception
     */
    public <T extends Service, R> void coprocessorService(final String tableName, final Class<T> service, final Object startRowKey, final Object endRowKey,
            final Batch.Call<T, R> callable, final Batch.Callback<R> callback) throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            table.coprocessorService(service, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), callable, callback);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
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
     * @return the map
     * @throws UncheckedIOException the unchecked IO exception
     * @throws Exception the exception
     */
    public <R extends Message> Map<byte[], R> batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor,
            final Message request, final Object startRowKey, final Object endRowKey, final R responsePrototype) throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            return table.batchCoprocessorService(methodDescriptor, request, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), responsePrototype);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
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
     * @throws UncheckedIOException the unchecked IO exception
     * @throws Exception the exception
     */
    public <R extends Message> void batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor, final Message request,
            final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback)
            throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            table.batchCoprocessorService(methodDescriptor, request, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), responsePrototype, callback);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * To family qualifier bytes.
     *
     * @param str the str
     * @return the byte[]
     */
    static byte[] toFamilyQualifierBytes(final String str) {
        if (str == null) {
            return null;
        }

        byte[] bytes = familyQualifierBytesPool.get(str);

        if (bytes == null) {
            bytes = Bytes.toBytes(str);

            familyQualifierBytesPool.put(str, bytes);
        }

        return bytes;
    }

    /**
     * To row key bytes.
     *
     * @param rowKey the row key
     * @return the byte[]
     */
    static byte[] toRowKeyBytes(final Object rowKey) {
        return toValueBytes(rowKey);
    }

    /**
     * To row bytes.
     *
     * @param row the row
     * @return the byte[]
     */
    static byte[] toRowBytes(final Object row) {
        return toValueBytes(row);
    }

    /**
     * To value bytes.
     *
     * @param value the value
     * @return the byte[]
     */
    static byte[] toValueBytes(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof byte[]) {
            return (byte[]) value;
        } else if (value instanceof ByteBuffer) {
            return ((ByteBuffer) value).array();
        } else if (value instanceof String) {
            return Bytes.toBytes((String) value);
        } else {
            return Bytes.toBytes(N.stringOf(value));
        }
    }

    //
    //    static byte[] toBytes(final String str) {
    //        return str == null ? null : Bytes.toBytes(str);
    //    }
    //
    //    static byte[] toBytes(final Object obj) {
    //        return obj == null ? null : (obj instanceof byte[] ? (byte[]) obj : toBytes(N.stringOf(obj)));
    //    }

    /**
     * To row key string.
     *
     * @param bytes the bytes
     * @param offset the offset
     * @param len the len
     * @return the string
     */
    static String toRowKeyString(byte[] bytes, int offset, int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * To family qualifier string.
     *
     * @param bytes the bytes
     * @param offset the offset
     * @param len the len
     * @return the string
     */
    static String toFamilyQualifierString(byte[] bytes, int offset, int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * To value string.
     *
     * @param bytes the bytes
     * @param offset the offset
     * @param len the len
     * @return the string
     */
    static String toValueString(byte[] bytes, int offset, int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * Gets the row key string.
     *
     * @param cell the cell
     * @return the row key string
     */
    static String getRowKeyString(final Cell cell) {
        return toRowKeyString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    }

    /**
     * Gets the family string.
     *
     * @param cell the cell
     * @return the family string
     */
    static String getFamilyString(final Cell cell) {
        return toFamilyQualifierString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
    }

    /**
     * Gets the qualifier string.
     *
     * @param cell the cell
     * @return the qualifier string
     */
    static String getQualifierString(final Cell cell) {
        return toFamilyQualifierString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    }

    /**
     * Gets the value string.
     *
     * @param cell the cell
     * @return the value string
     */
    static String getValueString(final Cell cell) {
        return toValueString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    /**
     * Gets the adds the method name.
     *
     * @param getSetMethod the get set method
     * @return the adds the method name
     */
    static String getAddMethodName(Method getSetMethod) {
        return "add" + getSetMethod.getName().substring(3);
    }

    /**
     * Close.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void close() throws IOException {
        if (conn.isClosed() == false) {
            conn.close();
        }
    }
}
