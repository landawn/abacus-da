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

package com.landawn.abacus.da.type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.landawn.abacus.da.util.Sheet;
import com.landawn.abacus.parser.JSONDeserializationConfig;
import com.landawn.abacus.parser.JSONDeserializationConfig.JDC;
import com.landawn.abacus.type.AbstractType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.type.TypeFactory;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.WD;

// TODO: Auto-generated Javadoc
/**
 * The Class SheetType.
 *
 * @author Haiyang Li
 * @param <R>
 * @param <C>
 * @param <E> the element type
 * @since 0.8
 */
public class SheetType<R, C, E> extends AbstractType<Sheet<R, C, E>> {

    /** The Constant ROW_KEY_SET. */
    private static final String ROW_KEY_SET = "rowKeySet";

    /** The Constant COLUMN_KEY_SET. */
    private static final String COLUMN_KEY_SET = "columnKeySet";

    /** The Constant ROW_LIST. */
    private static final String ROW_LIST = "rowList";

    /** The declaring name. */
    private final String declaringName;

    /** The type class. */
    private final Class<Sheet<R, C, E>> typeClass;

    /** The parameter types. */
    private final Type<?>[] parameterTypes;

    /** The jdc. */
    private final JSONDeserializationConfig jdc;

    /**
     * Instantiates a new sheet type.
     *
     * @param typeClass
     * @param rowKeyTypeName
     * @param columnKeyTypeName
     * @param elementTypeName
     */
    @SuppressWarnings("unchecked")
    public SheetType(Class<Sheet<R, C, E>> typeClass, String rowKeyTypeName, String columnKeyTypeName, String elementTypeName) {
        super(getTypeName(typeClass, rowKeyTypeName, columnKeyTypeName, elementTypeName, false));

        this.declaringName = getTypeName(typeClass.isInterface() ? typeClass : Sheet.class, rowKeyTypeName, columnKeyTypeName, elementTypeName, true);

        this.typeClass = typeClass;
        this.parameterTypes = new Type[] { TypeFactory.getType(rowKeyTypeName), TypeFactory.getType(columnKeyTypeName), TypeFactory.getType(elementTypeName) };

        final Type<?> rowKeyListType = TypeFactory.getType("List<" + rowKeyTypeName + ">");
        final Type<?> columnKeyListType = TypeFactory.getType("List<" + columnKeyTypeName + ">");
        final Type<?> rowListType = TypeFactory.getType("List<List<" + elementTypeName + ">>");
        this.jdc = JDC.create().setPropType(ROW_KEY_SET, rowKeyListType).setPropType(COLUMN_KEY_SET, columnKeyListType).setPropType(ROW_LIST, rowListType);
    }

    /**
     * Declaring name.
     *
     * @return
     */
    @Override
    public String declaringName() {
        return declaringName;
    }

    /**
     * Clazz.
     *
     * @return
     */
    @Override
    public Class<Sheet<R, C, E>> clazz() {
        return typeClass;
    }

    /**
     * Gets the parameter types.
     *
     * @return
     */
    @Override
    public Type<?>[] getParameterTypes() {
        return parameterTypes;
    }

    /**
     * Checks if is generic type.
     *
     * @return true, if is generic type
     */
    @Override
    public boolean isGenericType() {
        return true;
    }

    /**
     * Checks if is serializable.
     *
     * @return true, if is serializable
     */
    @Override
    public boolean isSerializable() {
        return true;
    }

    /**
     * String of.
     *
     * @param x
     * @return
     */
    @Override
    public String stringOf(Sheet<R, C, E> x) {
        if (x == null) {
            return null;
        }

        final List<List<?>> rowList = new ArrayList<>(x.rowLength());

        for (R rowKey : x.rowKeySet()) {
            rowList.add(x.getRow(rowKey));
        }

        final Map<Object, Object> m = new LinkedHashMap<>();
        m.put(ROW_KEY_SET, x.rowKeySet());
        m.put(COLUMN_KEY_SET, x.columnKeySet());
        m.put(ROW_LIST, rowList);

        return Utils.jsonParser.serialize(m, Utils.jsc);
    }

    /**
     * Value of.
     *
     * @param str
     * @return
     */
    @Override
    public Sheet<R, C, E> valueOf(String str) {
        if (N.isNullOrEmpty(str)) {
            return null;
        }

        final Map<String, Object> m = Utils.jsonParser.deserialize(Map.class, str, jdc);
        final List<R> rowKeySet = (List<R>) m.get(ROW_KEY_SET);
        final List<C> columnKeySet = (List<C>) m.get(COLUMN_KEY_SET);
        final List<List<E>> rowList = (List<List<E>>) m.get(ROW_LIST);

        Sheet<R, C, E> sheet = null;

        if (typeClass.equals(Sheet.class)) {
            sheet = new Sheet<>(rowKeySet, columnKeySet);
        } else {
            sheet = ClassUtil.invokeConstructor(ClassUtil.getDeclaredConstructor(typeClass, Collection.class, Collection.class), rowKeySet, columnKeySet);
        }

        int i = 0;
        for (R rowKey : rowKeySet) {
            sheet.setRow(rowKey, rowList.get(i++));
        }

        return sheet;
    }

    /**
     * Gets the type name.
     *
     * @param typeClass
     * @param rowKeyTypeName
     * @param columnKeyTypeName
     * @param elementTypeName
     * @param isDeclaringName
     * @return
     */
    protected static String getTypeName(Class<?> typeClass, String rowKeyTypeName, String columnKeyTypeName, String elementTypeName, boolean isDeclaringName) {
        if (isDeclaringName) {
            return ClassUtil.getSimpleClassName(typeClass) + WD.LESS_THAN + TypeFactory.getType(rowKeyTypeName).declaringName() + WD.COMMA_SPACE
                    + TypeFactory.getType(columnKeyTypeName).declaringName() + WD.COMMA_SPACE + TypeFactory.getType(elementTypeName).declaringName()
                    + WD.GREATER_THAN;
        } else {
            return ClassUtil.getCanonicalClassName(typeClass) + WD.LESS_THAN + TypeFactory.getType(rowKeyTypeName).name() + WD.COMMA_SPACE
                    + TypeFactory.getType(columnKeyTypeName).name() + WD.COMMA_SPACE + TypeFactory.getType(elementTypeName).name() + WD.GREATER_THAN;

        }
    }
}
