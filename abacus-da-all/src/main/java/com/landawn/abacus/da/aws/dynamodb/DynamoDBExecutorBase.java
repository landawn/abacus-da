
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

package com.landawn.abacus.da.aws.dynamodb;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.landawn.abacus.annotation.JsonXmlConfig;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.exception.ParsingException;
import com.landawn.abacus.parser.JsonDeserConfig;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.BiMap;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.Immutable;
import com.landawn.abacus.util.ImmutableBiMap;
import com.landawn.abacus.util.ImmutableCollection;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.function.Function;

/**
 * Abstract parent class of the AWS SDK v1 {@link DynamoDBExecutor} and AWS SDK v2
 * {@code com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor}.
 *
 * <p>This class owns the SDK-neutral half of the two executors. The concrete executors add their SDK-specific
 * clients, requests, and {@code AttributeValue} model and share the conversion rules declared here:</p>
 *
 * <ul>
 * <li>item to bean mapping ({@link #toEntity(Map, Type, Function, MappingContext)})</li>
 * <li>JSON property reads with field/class date and number formats, including formatted numeric wrappers</li>
 * <li>native container conversion that preserves binary leaves</li>
 * <li>binary value handling and scalar conversion used by the {@code toValue} methods of the subclasses</li>
 * </ul>
 *
 * <p><b>Boolean targets:</b> a {@code BOOL} attribute maps directly, and a {@code NULL} attribute is {@code null} for a
 * {@code Boolean} and {@code false} for a {@code boolean}. An {@code N} or {@code S} attribute read into a
 * {@code Boolean}/{@code boolean} property, element or map value follows abacus-common's strict text rule: the text, without
 * its leading and trailing whitespace padding, must be {@code 1}, {@code y}, {@code Y} or {@code true} (any ASCII case) for
 * {@code true}, or {@code 0}, {@code -1}, {@code n}, {@code N} or {@code false} (any ASCII case) for {@code false}; so
 * {@code S} {@code " Y"} or {@code " true "} converts, while any other text, such as {@code N} {@code 2} or {@code 1.0}, or
 * {@code S} {@code yes} or {@code T}, throws {@link IllegalArgumentException}. Blank or empty {@code S} text is not an
 * error: item mapping, direct ({@code M}) bean mapping and single-value reads ({@code toValue}/{@code toList} with
 * {@code Boolean.class}) read it as {@code false} for both {@code Boolean} and {@code boolean}, while a bean in JSON
 * context (an element of {@code L}, or an {@code S} attribute holding JSON text) reads it as {@code null} for a
 * {@code Boolean} and {@code false} for a {@code boolean}. A native Java number (a decoded {@code Map} value converted to a
 * bean) converts only as an {@code Integer}, {@code Short} or {@code Byte} {@code 1} (true), {@code 0} or {@code -1}
 * (false); any other value or {@code Number} class is rejected with {@link IllegalArgumentException}. A JSON number token in
 * the JSON text of an {@code S} attribute follows the same rule: {@code {"flag":1}} reads as {@code true}, {@code {"flag":0}}
 * and {@code {"flag":-1}} as {@code false}, and {@code {"flag":1.0}} or {@code {"flag":2}} throw
 * {@link IllegalArgumentException}.</p>
 *
 * <p>The format caches are static because the inspected {@code BeanInfo}/{@code PropInfo} metadata is
 * SDK-independent; the two executors share them.</p>
 */
public abstract class DynamoDBExecutorBase {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected static final Class<Map<String, Object>> PROP_MAP_TYPE = (Class) Clazz.PROPS_MAP;

    // Annotation patterns are shared, but exact and floating-point parsing require different formatter settings.
    protected static final Map<String, ThreadLocal<DecimalFormat>> exactNumberFormatPool = new ConcurrentHashMap<>();
    protected static final Map<String, ThreadLocal<DecimalFormat>> numberFormatPool = new ConcurrentHashMap<>();
    protected static final Map<Type<?>, Boolean> formattedJsonTypePool = new ConcurrentHashMap<>();
    // PropInfo equality ignores resolved generic types; BeanInfo equality also ignores type arguments.
    // Scope property flags by the complete owning bean type, which also distinguishes inherited class formats.
    protected static final Map<java.lang.reflect.Type, Map<PropInfo, Boolean>> jsonScalarFormatPool = new ConcurrentHashMap<>();
    protected static final Map<java.lang.reflect.Type, Map<PropInfo, Boolean>> jsonDateFormatPool = new ConcurrentHashMap<>();

    /**
     * Constructor for SDK-specific subclasses.
     */
    protected DynamoDBExecutorBase() {
    }

    /** Distinguishes outer item mapping, native Map-to-bean conversion, and JSON property-read rules. */
    protected enum MappingContext {
        ITEM, MAPPED_BEAN, JSON
    }

    /**
     * Maps attributes or already-decoded native values to a bean using its complete generic type.
     * The decoder supplies raw property values; a {@code null} item returns {@code null}.
     *
     * <p>JSON reads apply property formats, skip every SERIALIZE_ONLY property, and ignore unmatched dotted
     * names. Item and direct bean mapping assign SERIALIZE_ONLY properties that have a write path (a writable field or
     * a setter), skip read-only properties ({@code PropInfo.isReadOnlyProperty}, including getter-only properties of an
     * {@code @AccessFieldByMethod} bean), and resolve dotted paths, converting their values as for the last property of the
     * path named directly. A read-only property is skipped in every context,
     * whatever its declared direction: a read-only property can carry an explicit {@code DESERIALIZE_ONLY} only on an
     * {@code @AccessFieldByMethod} field without a setter, and such a property is skipped too, by item and direct mapping and
     * by a JSON-context element of {@code L}, since {@code isReadOnlyProperty} is tested before the direction. Only a bean parsed from the JSON
     * text of an {@code S} attribute is read by abacus-common's JSON parser, which rejects such a property with
     * {@link UnsupportedOperationException}. Direct beans additionally retain the full-Type
     * conversions of the dependency's map-to-bean mapper; those conversions differ from both item scalar conversion and
     * JSON reads.</p>
     *
     * @param <T> the bean type
     * @param <V> the type of the raw attribute values in {@code item}
     * @param item the attributes or decoded native values keyed by attribute or property name; may be {@code null}
     * @param targetType the complete (possibly parameterized) bean type to create
     * @param valueDecoder decodes each raw value of {@code item} into its native Java value
     * @param context the conversion rules used by the enclosing value
     * @return the finished bean, or {@code null} for a {@code null} item
     * @throws RuntimeException if decoding, conversion, bean construction, or property assignment fails
     */
    protected static <T, V> T toEntity(final Map<String, V> item, final Type<T> targetType, final Function<? super V, ?> valueDecoder,
            final MappingContext context) {
        if (item == null) {
            return null;
        }

        final Map<String, String> column2FieldNameMap = QueryUtil.columnToPropNameMap(targetType.javaType());
        final BeanInfo entityInfo = ParserUtil.getBeanInfo(targetType.reflectType());
        final Object entity = entityInfo.createBeanResult();
        final boolean jsonRead = context == MappingContext.JSON;
        PropInfo propInfo = null;
        String propName = null;
        V propValue = null;
        String fieldName = null;

        // A JSON bean must see duplicate names in source order, including aliases for the same property.
        final Iterable<Map.Entry<String, V>> entries = item instanceof final JsonObject json ? (List<Map.Entry<String, V>>) (List<?>) json.entries
                : item.entrySet();
        for (final Map.Entry<String, V> entry : entries) {
            propName = entry.getKey();
            propValue = entry.getValue();

            propInfo = entityInfo.getPropInfo(propName);

            if (propInfo == null && (fieldName = column2FieldNameMap.get(propName)) != null) {
                propName = fieldName;
                propInfo = entityInfo.getPropInfo(propName);
            }

            if (propInfo == null) {
                if (!jsonRead && propName.indexOf(SK._PERIOD) > 0) { //NOSONAR
                    final Object rawValue = valueDecoder.apply(propValue);
                    // Convert a dotted path's value as the same property is converted at top level. The dependency's path
                    // setter only applies its raw conversion: an SDK v1 buffer became "" or null, an SDK v2 byte[] for a
                    // ByteBuffer an empty write-mode buffer, and typed containers, wrappers and enum codes skipped the item
                    // rules. Paths it ignores (unresolvable, or ending at a read-only property) still pass the value unchanged.
                    final PropInfo leafPropInfo = writableLeafOfPath(entityInfo, propName);
                    entityInfo.setPropValue(entity, propName, leafPropInfo == null ? rawValue : readPropertyValue(leafPropInfo, rawValue, context), true);
                }

                continue;
            }

            // JSON reads skip every SERIALIZE_ONLY property; database mapping only skips properties without a write path.
            // isReadOnlyProperty (abacus-common 8.2+) also covers a field-backed getter-only property of an
            // @AccessFieldByMethod bean, whose setPropValue throws UnsupportedOperationException; testing only
            // field == null missed it and failed the whole item.
            if (propInfo.isReadOnlyProperty || (jsonRead && propInfo.jsonXmlExpose == JsonXmlField.Direction.SERIALIZE_ONLY)) {
                continue;
            }

            final Object rawValue = valueDecoder.apply(propValue);
            final Type<?> propType = propInfo.jsonXmlType;

            // Direct Map-to-bean conversion used PropInfo's full Type, unlike the outer item mapper.
            // Erasing it here silently puts Strings in Optional<Long>/Pair<Long, Long> and loses enum codes.
            // Traverse native structures without a per-property JSON round trip, preserving binary leaves.
            if (jsonRead && rawValue instanceof final JsonValue json) {
                final Object value = json.value();
                final boolean scalar = !(value instanceof Map || value instanceof Collection);
                propInfo.setPropValue(entity,
                        scalar && (value instanceof String || hasJsonScalarFormat(entityInfo, propInfo))
                                ? readNestedPropValue(entityInfo, propInfo, json.text())
                                : json.read(propType));
            } else if (jsonRead && rawValue instanceof String) {
                // readPropValue honors field/class date and number formats, enum codes, and explicit
                // property types. Raw-class conversion would lose these when bypassing the JSON codec.
                propInfo.setPropValue(entity, readNestedPropValue(entityInfo, propInfo, (String) rawValue));
            } else if (jsonRead && rawValue instanceof Boolean && isScalarSlot(propType)) {
                // A BOOL value is JSON's boolean token. Storing it because a Boolean is assignable skipped the
                // property's number or date format, which rejects a boolean, as the JSON reader does.
                propInfo.setPropValue(entity, readNestedBoolean(entityInfo, propInfo, (Boolean) rawValue));
            } else if (jsonRead && rawValue != null) {
                // A JSON bean reads its other native values with the rules of its nested values, so a value of another shape is
                // converted or rejected as the JSON reader did, instead of being stored as the wrong element type.
                propInfo.setPropValue(entity, convertContainerElement(rawValue, propType, context));
            } else {
                propInfo.setPropValue(entity, readPropertyValue(propInfo, rawValue, context));
            }
        }

        return entityInfo.finishBeanResult(entity);
    }

    /**
     * Converts a decoded value to the value assigned to a bean property by item and direct bean mapping (and to a {@code null}
     * value in JSON context), for a property named directly or by a dotted path.
     */
    private static Object readPropertyValue(final PropInfo propInfo, final Object rawValue, final MappingContext context) {
        final Type<?> propType = propInfo.jsonXmlType;

        if (context == MappingContext.MAPPED_BEAN
                || isGenericWrapper(propType) && (rawValue != null || context == MappingContext.ITEM && propType.javaType() == Holder.class)) {
            final Object converted = convertContainerElement(rawValue, propType, context);

            if (context == MappingContext.ITEM && rawValue instanceof String && wrappedBinary(converted) instanceof final ByteBuffer buffer) {
                // toItem writes an item's wrapped buffer back as binary from its readable bytes. Base64 text decodes to a buffer
                // positioned after its bytes, so flip it as for a bare ByteBuffer; otherwise a read-modify-write stores an empty value.
                buffer.flip();
            }

            return converted;
        } else if ((rawValue instanceof Collection && (propType.isCollection() || propType.isObjectArray())) || (rawValue instanceof Map && propType.isMap())) {
            return convertNativeContainer(rawValue, propType, context);
        } else if (rawValue instanceof String && !propInfo.clazz.isInstance(rawValue) && isParameterizedValueType(propType)) {
            // Text for a parameterized type, or for an array of one, is read with the complete type. Raw-class conversion
            // parsed "[1, 2]" into a Pair<Long, Long> or an Optional<Long>[] with Integer contents.
            return readJsonText((String) rawValue, propType);
        } else if (rawValue != null && !(rawValue instanceof String) && isItemShapeMismatch(rawValue, propType)) {
            // A native value of another shape for a typed container, an object array, or another parameterized type (an M
            // attribute for a List<Long>, a BOOL or B attribute for a Long[]): raw-class conversion would store it unconverted.
            return convertMismatchedValue(rawValue, propType, context);
        } else if (rawValue instanceof Map && propType.isBean()) {
            return convertContainerElement(rawValue, propType, context);
        } else if (rawValue == null || propInfo.clazz.isAssignableFrom(rawValue.getClass())) {
            return rawValue;
        } else if (rawValue instanceof String && propType.javaType().isEnum()) {
            // Use the property's enum representation: a CODE enum accepts both the constant names that toItem
            // writes and numeric codes, instead of the raw enum class reading a numeric code as an ordinal.
            return propType.valueOf((String) rawValue);
        } else {
            return convertValue(rawValue, propInfo.clazz);
        }
    }

    /**
     * Returns the last property of a dotted path, or {@code null} when the dependency's path setter ignores the path: no
     * property chain matches it, or the chain ends at a read-only property.
     */
    private static PropInfo writableLeafOfPath(final BeanInfo beanInfo, final String path) {
        final List<PropInfo> chain = beanInfo.getPropInfoChain(path);

        if (chain.isEmpty()) {
            return null;
        }

        final PropInfo leaf = chain.get(chain.size() - 1);
        return leaf.isReadOnlyProperty ? null : leaf;
    }

    /**
     * Reads scalar text for a bean in JSON context, applying property formats and the effective property type.
     * Unformatted {@code Number} values and supported wrapper chains ending in {@code Number} use JSON numeric
     * inference instead of the abstract number type's String factory.
     */
    protected static Object readNestedPropValue(final BeanInfo beanInfo, final PropInfo propInfo, final String value) {
        if (value == null) {
            return propInfo.readPropValue(null);
        }

        if (needsFormattedJsonRead(propInfo.jsonXmlType) && !hasJsonScalarFormat(beanInfo, propInfo)) {
            return readJsonText(value, propInfo.jsonXmlType);
        }

        if (Number.class.equals(propInfo.jsonXmlType.javaType())) {
            final JsonXmlField fieldConfig = propInfo.getAnnotation(JsonXmlField.class);
            final JsonXmlConfig beanConfig = beanInfo.getAnnotation(JsonXmlConfig.class);
            final String numberFormat = fieldConfig != null && Strings.isNotEmpty(fieldConfig.numberFormat()) ? fieldConfig.numberFormat()
                    : beanConfig == null ? null : beanConfig.numberFormat();

            if (Strings.isBlank(numberFormat) && (fieldConfig == null || Strings.isBlank(fieldConfig.dateFormat()))) {
                // Number is abstract and its Type has no String factory. Use the JSON parser's numeric
                // inference, as before, unless PropInfo has an explicit format to apply or reject.
                return N.fromJson(value, Number.class);
            }
        } else if (isNumericWrapper(propInfo.jsonXmlType)) {
            final JsonXmlField fieldConfig = propInfo.getAnnotation(JsonXmlField.class);

            if (hasJsonDateFormat(beanInfo, propInfo)) {
                // Honor every date-format annotation recognized by PropInfo before specializing numeric wrappers.
                // This also preserves its rejection of unsupported wrapper types and overrides that trim to empty.
                return propInfo.readPropValue(value);
            }

            // PropInfo applies class-level formats only to number types, not generic wrappers. Mirror that rule
            // and its pattern trimming so native containers read the same values as serialized JSON containers.
            final String numberFormat = fieldConfig == null ? null : Strings.trim(fieldConfig.numberFormat());

            if (!Strings.isBlank(numberFormat)) {
                return parseFormattedNumber(value, numberFormat, propInfo.jsonXmlType);
            } else if (needsNumberInference(propInfo.jsonXmlType)) {
                return readJsonScalar(value, propInfo.jsonXmlType);
            }
        }

        return propInfo.readPropValue(value);
    }

    /** Caches schema inspection; changing generic cycles are resolved while reading the finite JSON value. */
    protected static boolean needsFormattedJsonRead(final Type<?> type) {
        return formattedJsonTypePool.computeIfAbsent(type, key -> needsFormattedJsonRead(key, new HashMap<>()));
    }

    protected static boolean needsFormattedJsonRead(final Type<?> type, final Map<Class<?>, Type<?>> visiting) {
        if (type.isCollection() || type.isMap() || type.isObjectArray() || isGenericWrapper(type)) {
            final Type<?> elementType = type.isObjectArray() ? type.elementType()
                    : type.isParameterizedType() ? type.parameterTypes().get(type.isMap() ? 1 : 0) : N.typeOf(Object.class);
            return needsFormattedJsonRead(elementType, visiting);
        } else if (!type.isBean()) {
            return false;
        }

        final BeanInfo beanInfo = ParserUtil.getBeanInfo(type.reflectType());
        // Check the current instantiation first; its resolved properties may already contain a numeric wrapper.
        for (final PropInfo property : beanInfo.propInfoList) {
            final JsonXmlField config = property.getAnnotation(JsonXmlField.class);
            if (property.jsonXmlExpose != JsonXmlField.Direction.SERIALIZE_ONLY && isNumericWrapper(property.jsonXmlType) && config != null
                    && !Strings.isBlank(config.numberFormat())) {
                return true;
            }
        }

        final Type<?> previous = visiting.putIfAbsent(type.javaType(), type);
        if (previous != null) {
            // A repeated class with different type arguments may gain a formatted wrapper several recurrences later.
            // Keep that fallback unless a finite scan of declarations and type arguments proves no field number
            // format is reachable. Eagerly expanding resolved types can be infinite (e.g. Node<List<T>>).
            return !previous.equals(type) && hasReachableJsonNumberFormat(type, new java.util.HashSet<>());
        }

        try {
            for (final PropInfo property : beanInfo.propInfoList) {
                if (property.jsonXmlExpose != JsonXmlField.Direction.SERIALIZE_ONLY && needsFormattedJsonRead(property.jsonXmlType, visiting)) {
                    return true;
                }
            }

            return false;
        } finally {
            visiting.remove(type.javaType());
        }
    }

    /**
     * Conservatively finds field number formats that a changing generic cycle could make relevant. Inspect raw bean
     * declarations only once to keep the graph finite, but always inspect type arguments: a formatted bean may be
     * introduced by a recursive declaration or become a property only after several rotations. Any field format
     * keeps the fallback, even when its current raw property type is not a numeric wrapper.
     */
    protected static boolean hasReachableJsonNumberFormat(final Type<?> type, final Set<Class<?>> visited) {
        if (type.isObjectArray()) {
            return hasReachableJsonNumberFormat(type.elementType(), visited);
        }

        for (final Type<?> argument : type.parameterTypes()) {
            if (hasReachableJsonNumberFormat(argument, visited)) {
                return true;
            }
        }

        // Non-static member beans can also read type variables declared by their parameterized owner.
        java.lang.reflect.Type owner = type.reflectType() instanceof final java.lang.reflect.ParameterizedType parameterized ? parameterized.getOwnerType()
                : null;
        while (owner instanceof final java.lang.reflect.ParameterizedType parameterizedOwner) {
            for (final java.lang.reflect.Type argument : parameterizedOwner.getActualTypeArguments()) {
                if (hasReachableJsonNumberFormat(Type.of(argument), visited)) {
                    return true;
                }
            }
            owner = parameterizedOwner.getOwnerType();
        }

        if (!type.isBean() || !visited.add(type.javaType())) {
            return false;
        }

        for (final PropInfo property : ParserUtil.getBeanInfo(type.javaType()).propInfoList) {
            if (property.jsonXmlExpose == JsonXmlField.Direction.SERIALIZE_ONLY) {
                continue;
            }
            final JsonXmlField config = property.getAnnotation(JsonXmlField.class);
            if (config != null && !Strings.isBlank(config.numberFormat()) || hasReachableJsonNumberFormat(property.jsonXmlType, visited)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Reads JSON containers through the same property conversion as native L/M values when their schema
     * contains formatted numeric wrappers. Keep each child's JSON text until its declared type is known:
     * an untyped intermediate Double would lose precision/scale even in unrelated BigDecimal properties.
     */
    @SuppressWarnings({ "unchecked" })
    protected static Object readJsonText(final String value, final Type<?> targetType) {
        final String text = value.strip();
        if (text.isEmpty() || text.charAt(0) != '[' && text.charAt(0) != '{' || !needsFormattedJsonRead(targetType)) {
            return targetType.valueOf(value);
        }

        final Object decoded;
        if (text.charAt(0) == '{') {
            final JsonObject values = N.fromJson(text, JsonObject.class);
            final JsonObject fragments = N.fromJson(text, JsonDeserConfig.create().setMapValueType(String.class), JsonObject.class);
            final JsonObject result = new JsonObject();
            for (int i = 0; i < values.entries.size(); i++) {
                final Map.Entry<String, Object> entry = values.entries.get(i);
                result.put(entry.getKey(), new JsonValue((String) fragments.entries.get(i).getValue(), entry.getValue()));
            }
            decoded = result;
        } else {
            final List<?> values = N.fromJson(text, List.class);
            final List<String> fragments = N.fromJson(text, JsonDeserConfig.create().setElementType(String.class), List.class);
            final List<Object> result = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                result.add(new JsonValue(fragments.get(i), values.get(i)));
            }
            decoded = result;
        }

        return convertContainerElement(decoded, targetType, MappingContext.JSON);
    }

    /** Maps keep their usual last-value-wins behavior; beans additionally need every name in source order. */
    protected static final class JsonObject extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
        private final List<Map.Entry<String, Object>> entries = new ArrayList<>();

        @Override
        public Object put(final String key, final Object value) {
            entries.add(new java.util.AbstractMap.SimpleImmutableEntry<>(key, value));
            return super.put(key, value);
        }
    }

    /**
     * Converts an array of alternating property names and values into an ordered
     * {@link Map} suitable for use as a property bag.
     *
     * <p>The input array is interpreted as alternating key-value pairs in the form
     * {@code [name1, value1, name2, value2, ...]}. Each name element must be a
     * {@link String}; a non-String (or {@code null}) name causes an
     * {@link IllegalArgumentException} to be thrown. The corresponding value is
     * stored as-is. The returned map is a {@link LinkedHashMap}, so iteration
     * order matches the order of the pairs in the input array. If a name occurs
     * more than once, the later value overwrites the earlier one (but the original
     * insertion position is retained).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: alternating name/value pairs, iteration order preserved.
     * Map<String, Object> p = DynamoDBExecutorBase.asProps(new Object[] { "name", "Alice", "age", 30 });
     * // returns {name=Alice, age=30} (a LinkedHashMap)
     *
     * // Negative: a non-String name is rejected.
     * DynamoDBExecutorBase.asProps(new Object[] { 100, "hundred" });
     * // throws IllegalArgumentException
     *
     * // Duplicate name: later value overwrites, original position retained.
     * Map<String, Object> r = DynamoDBExecutorBase.asProps(new Object[] { "z", 1, "a", 2, "z", 3 });
     * // returns {z=3, a=2}
     *
     * // Edge: null array yields an empty map (not null).
     * Map<String, Object> s = DynamoDBExecutorBase.asProps(null);
     * // returns {} (an empty LinkedHashMap)
     *
     * // Edge: empty array yields an empty map.
     * Map<String, Object> t = DynamoDBExecutorBase.asProps(new Object[0]);
     * // returns {}
     *
     * // Edge: a null value is allowed and preserved.
     * Map<String, Object> u = DynamoDBExecutorBase.asProps(new Object[] { "key", null });
     * // returns {key=null}
     *
     * // Negative: odd-length array is rejected.
     * DynamoDBExecutorBase.asProps(new Object[] { "name", "Alice", "age" });
     * // throws IllegalArgumentException
     * }</pre>
     *
     * @param propNameAndValues an even-length array of alternating property names
     *                          and values; may be {@code null}
     * @return a new {@link LinkedHashMap} containing the supplied pairs in order;
     *         an empty map if {@code propNameAndValues} is {@code null}
     * @throws IllegalArgumentException if {@code propNameAndValues} has an odd length,
     *         or if any property name is not a {@link String}
     */
    public static Map<String, Object> asProps(final Object[] propNameAndValues) throws IllegalArgumentException {
        if (propNameAndValues == null) {
            return new LinkedHashMap<>(); // NOSONAR
        }

        if ((propNameAndValues.length % 2) != 0) {
            throw new IllegalArgumentException("The length of property name/value array must be even: " + propNameAndValues.length);
        }

        for (int i = 0, len = propNameAndValues.length; i < len; i += 2) {
            if (!(propNameAndValues[i] instanceof String)) {
                throw new IllegalArgumentException("Parameters must be property name-value pairs whose names are Strings, but found "
                        + (propNameAndValues[i] == null ? "null" : propNameAndValues[i].getClass().getName()) + " at index " + i);
            }
        }

        final Map<String, Object> props = new LinkedHashMap<>(propNameAndValues.length); // capacity = 2x pair count, so no rehash below the default load factor

        for (int i = 0, len = propNameAndValues.length; i < len; i += 2) {
            props.put((String) propNameAndValues[i], propNameAndValues[i + 1]);
        }

        return props;
    }

    /** A child token retains both its JSON kind and its exact text until the target type is available. */
    protected record JsonValue(String text, Object value) {
        String source() {
            return value instanceof String ? N.toJson(value) : value == null ? "null" : text;
        }

        Object read(final Type<?> type) {
            if (value != null && needsFormattedJsonRead(type)) {
                return readJsonText(source(), type);
            }

            // A scalar root uses Type.valueOf; an array slot retains JSON token semantics (e.g.
            // numeric Date/Character values mean epoch milliseconds/code points, not text).
            final List<?> result = N.fromJson("[" + source() + "]", JsonDeserConfig.create().setElementType(type), List.class);
            return result.get(0);
        }
    }

    /** Scalar formats consume token text; unformatted numeric tokens retain JSON's typed conversion. */
    protected static boolean hasJsonScalarFormat(final BeanInfo beanInfo, final PropInfo property) {
        return jsonScalarFormatPool.computeIfAbsent(beanInfo.javaType, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(property, key -> findJsonScalarFormat(beanInfo, key));
    }

    protected static boolean findJsonScalarFormat(final BeanInfo beanInfo, final PropInfo property) {
        final JsonXmlField field = property.getAnnotation(JsonXmlField.class);
        final JsonXmlConfig bean = beanInfo.getAnnotation(JsonXmlConfig.class);
        final String numberFormat = field != null && Strings.isNotEmpty(field.numberFormat()) ? field.numberFormat()
                : bean != null && property.type.isNumber() ? bean.numberFormat() : null;
        return !Strings.isBlank(numberFormat) || hasJsonDateFormat(beanInfo, property);
    }

    protected static boolean hasJsonDateFormat(final BeanInfo beanInfo, final PropInfo property) {
        return jsonDateFormatPool.computeIfAbsent(beanInfo.javaType, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(property, key -> findJsonDateFormat(beanInfo, key));
    }

    /**
     * Mirrors PropInfo: select the first nonempty annotation before trimming, so overrides that trim to empty
     * suppress lower-priority formats. Use trim rather than isBlank: control characters and Unicode spaces differ.
     */
    protected static boolean findJsonDateFormat(final BeanInfo beanInfo, final PropInfo property) {
        final JsonXmlField field = property.getAnnotation(JsonXmlField.class);
        if (field != null && Strings.isNotEmpty(field.dateFormat())) {
            return Strings.isNotEmpty(Strings.trim(field.dateFormat()));
        }

        // PropInfo also recognizes these optional annotation APIs. Inspect their pattern without
        // making either JSON library a required runtime dependency of the DynamoDB executors.
        if (property.field != null) {
            for (final String name : List.of("com.alibaba.fastjson2.annotation.JSONField", "com.fasterxml.jackson.annotation.JsonFormat")) {
                for (final java.lang.annotation.Annotation annotation : property.field.getAnnotations()) {
                    if (!annotation.annotationType().getName().equals(name)) {
                        continue;
                    }
                    final String method = name.endsWith(".JSONField") ? "format" : "pattern";
                    try {
                        final String pattern = (String) annotation.annotationType().getMethod(method).invoke(annotation);
                        if (!pattern.isEmpty()) {
                            return Strings.isNotEmpty(Strings.trim(pattern));
                        }
                    } catch (final ReflectiveOperationException e) {
                        throw new IllegalStateException("Cannot read property format: " + property.field, e);
                    }
                }
            }
        }

        final JsonXmlConfig bean = beanInfo.getAnnotation(JsonXmlConfig.class);
        final Class<?> cls = property.clazz;
        return bean != null && Strings.isNotEmpty(Strings.trim(bean.dateFormat()))
                && (cls == java.util.Date.class || cls == java.util.Calendar.class || cls == java.sql.Date.class || cls == java.sql.Time.class
                        || cls == java.sql.Timestamp.class || cls == java.time.LocalDate.class || cls == java.time.LocalTime.class
                        || cls == java.time.LocalDateTime.class || cls == java.time.ZonedDateTime.class || cls.getName().equals("org.joda.time.DateTime")
                        || cls.getName().equals("org.joda.time.MutableDateTime"));
    }

    /** Returns whether a supported generic wrapper chain ends in a {@link Number} subtype. */
    protected static boolean isNumericWrapper(Type<?> type) {
        if (!isGenericWrapper(type)) {
            return false;
        }

        do {
            type = type.parameterTypes().get(0);
        } while (isGenericWrapper(type));

        return Number.class.isAssignableFrom(type.javaType());
    }

    /**
     * Parses formatted text without invoking the wrapper chain's string codec. Exact parsing is required before
     * conversion to integral targets; floating-point and abstract Number targets retain DecimalFormat's default types.
     */
    protected static Object parseFormattedNumber(final String value, final String numberFormat, final Type<?> targetType) {
        Type<?> elementType = targetType;
        while (isGenericWrapper(elementType)) {
            elementType = elementType.parameterTypes().get(0);
        }

        final Class<?> numberClass = elementType.javaType();
        final boolean parseBigDecimal = numberClass == java.math.BigDecimal.class || numberClass == java.math.BigInteger.class || numberClass == int.class
                || numberClass == Integer.class || numberClass == long.class || numberClass == Long.class || numberClass == short.class
                || numberClass == Short.class || numberClass == byte.class || numberClass == Byte.class;
        final Map<String, ThreadLocal<DecimalFormat>> formatPool = parseBigDecimal ? exactNumberFormatPool : numberFormatPool;
        final DecimalFormat decimalFormat = formatPool.computeIfAbsent(numberFormat, pattern -> {
            final DecimalFormat prototype = new DecimalFormat(pattern, DecimalFormatSymbols.getInstance(java.util.Locale.ROOT));
            prototype.setParseBigDecimal(parseBigDecimal);
            return ThreadLocal.withInitial(() -> (DecimalFormat) prototype.clone());
        }).get();

        final ParsePosition position = new ParsePosition(0);
        final Number number = decimalFormat.parse(value, position);

        if (number == null || position.getIndex() != value.length()) {
            throw new ParsingException(
                    "Failed to parse complete number value: " + value + " with format: " + numberFormat + " at index: " + position.getIndex());
        }

        return readFormattedNumber(number, targetType);
    }

    /** Reconstructs a numeric wrapper after converting only its innermost numeric value. */
    protected static Object readFormattedNumber(final Number number, final Type<?> targetType) {
        if (!isGenericWrapper(targetType)) {
            return targetType.javaType().isInstance(number) ? number : convertValue(number, targetType.javaType());
        }

        final Object converted = readFormattedNumber(number, targetType.parameterTypes().get(0));
        final Class<?> wrapperClass = targetType.javaType();

        if (wrapperClass == java.util.Optional.class) {
            return java.util.Optional.ofNullable(converted);
        } else if (wrapperClass == com.landawn.abacus.util.u.Optional.class) {
            return com.landawn.abacus.util.u.Optional.ofNullable(converted);
        } else if (wrapperClass == com.landawn.abacus.util.u.Nullable.class) {
            return com.landawn.abacus.util.u.Nullable.of(converted);
        } else {
            return Holder.of(converted);
        }
    }

    /**
     * Reads a BOOL value for a bean in JSON context as the JSON reader reads a boolean token: a property format
     * parses (and rejects) its text, and property types other than {@code Boolean}'s supertypes read that text.
     * Slots declared as a supertype of {@code Boolean}, such as {@code Object}, keep the Boolean value.
     */
    protected static Object readNestedBoolean(final BeanInfo beanInfo, final PropInfo propInfo, final Boolean value) {
        final String text = value.toString();

        if (Boolean.class.equals(propInfo.clazz) || !propInfo.clazz.isAssignableFrom(Boolean.class)) {
            return readNestedPropValue(beanInfo, propInfo, text);
        }

        // Without a format, a supertype slot reads the text back unchanged; any other result or failure comes from a format.
        final Object formatted = propInfo.readPropValue(text);
        return text.equals(formatted) ? value : formatted;
    }

    /**
     * Returns whether the type holds a single value: not a collection, map, array, bean, or supported generic wrapper.
     */
    protected static boolean isScalarSlot(final Type<?> type) {
        return !type.isCollection() && !type.isMap() && !type.isArray() && !type.isBean() && !isGenericWrapper(type);
    }

    /**
     * Returns whether text for the type must be read with its complete type: a parameterized type other than an array,
     * or an object array whose element type is parameterized (such as {@code Optional<Long>[]}).
     */
    protected static boolean isParameterizedValueType(final Type<?> type) {
        // Abacus reports every array type, including byte[], as parameterized by its element type.
        return type.isObjectArray() ? type.elementType().isParameterizedType() && !type.elementType().isArray() : type.isParameterizedType() && !type.isArray();
    }

    /**
     * Returns whether a non-text native item value has another shape than its declared type: any value for an object array,
     * a typed collection, or a typed map (matching collections and maps were converted before), or a value that is not an
     * instance of another parameterized type. Wrappers, beans, and Object-element containers are converted elsewhere.
     */
    protected static boolean isItemShapeMismatch(final Object value, final Type<?> type) {
        if (type.isObjectArray()) {
            return true;
        } else if (type.isArray() || !type.isParameterizedType() || isGenericWrapper(type) || type.isBean()) {
            return false;
        } else if (type.isCollection() || type.isMap()) {
            return isElementConversionNeeded(type);
        }

        return !type.javaType().isInstance(value) && !acceptsBinaryValue(value, type);
    }

    /**
     * Returns whether a non-text native value read with JSON rules does not fit its declared type: a binary value for a
     * container, an object array, a bean, or another parameterized type that a ByteBuffer cannot be, or any other value that
     * is not an instance of the type.
     */
    protected static boolean isJsonShapeMismatch(final Object value, final Type<?> type) {
        if (value instanceof ByteBuffer || value instanceof byte[]) {
            return type.isCollection() || type.isMap() || type.isObjectArray() || type.isBean()
                    || type.isParameterizedType() && !type.isArray() && !acceptsBinaryValue(value, type);
        }

        return !type.javaType().isInstance(value);
    }

    /**
     * Returns whether a binary value can be held as-is or as a ByteBuffer by a declared type such as {@code Comparable<?>}:
     * {@code convertValue} keeps or wraps it, as for an untyped slot.
     */
    protected static boolean acceptsBinaryValue(final Object value, final Type<?> type) {
        return (value instanceof ByteBuffer || value instanceof byte[])
                && (type.javaType().isInstance(value) || type.javaType().isAssignableFrom(ByteBuffer.class));
    }

    /**
     * Returns whether the type is a parameterized JDK or Abacus {@code Optional}, Abacus {@code Nullable}, or {@link Holder}.
     */
    protected static boolean isGenericWrapper(final Type<?> type) {
        final Class<?> cls = type.javaType();
        return type.isParameterizedType() && (cls == java.util.Optional.class || cls == com.landawn.abacus.util.u.Optional.class
                || cls == com.landawn.abacus.util.u.Nullable.class || cls == Holder.class);
    }

    /**
     * Returns whether the type is exactly {@link Number}, possibly enclosed in supported generic wrappers.
     * Concrete number subclasses retain their own type codecs.
     */
    protected static boolean needsNumberInference(Type<?> type) {
        while (isGenericWrapper(type)) {
            type = type.parameterTypes().get(0);
        }

        return type.javaType() == Number.class;
    }

    /**
     * Reads non-null scalar text as {@link Number}, or a supported wrapper chain ending in {@code Number},
     * using the JSON reader's numeric inference and empty/null-like text handling.
     */
    protected static Object readJsonScalar(final String value, final Type<?> targetType) {
        if (targetType.javaType() == Number.class) {
            return N.fromJson(value, Number.class);
        }

        // Wrapper Type.valueOf delegates to its element's String factory, which Number lacks. A JSON
        // array slot uses the parser's scalar inference recursively, including Optional<Optional<Number>>.
        final List<?> result = N.fromJson(N.toJson(List.of(value)), JsonDeserConfig.create().setElementType(targetType), List.class);
        return result.get(0);
    }

    /**
     * Returns the ByteBuffer or byte array held, at any nesting depth, by present JDK or Abacus {@code Optional}, Abacus
     * {@code Nullable}, or {@link Holder} wrappers, or {@code null} if {@code value} is not such a wrapper or holds no binary value.
     */
    protected static Object wrappedBinary(final Object value) {
        Object content = value;
        boolean wrapped = false;

        while (content != null) {
            if (content instanceof final java.util.Optional<?> optional) {
                content = optional.orElse(null);
            } else if (content instanceof final com.landawn.abacus.util.u.Optional<?> optional) {
                content = optional.orElseNull();
            } else if (content instanceof final com.landawn.abacus.util.u.Nullable<?> nullable) {
                content = nullable.orElseNull();
            } else if (content instanceof final Holder<?> holder) {
                content = holder.value();
            } else {
                break;
            }

            wrapped = true;
        }

        return wrapped && (content instanceof ByteBuffer || content instanceof byte[]) ? content : null;
    }

    /**
     * Converts a decoded value to the requested Java type.
     *
     * <p>The two AWS SDKs expose binary attributes differently: SDK v1 decodes a {@code B} attribute as a
     * {@link ByteBuffer}, while SDK v2 decodes it as a {@code byte[]}. Both representations are handled here so
     * that the executors share one set of conversion rules: a buffer for a type that cannot hold it converts from its
     * readable bytes, as a {@code byte[]} does (see {@link #readableBytesOfBuffer(Object, Class)}).</p>
     *
     * @throws RuntimeException if the value cannot be converted to the requested Java type, or the target bean or container cannot be constructed or
     *         populated
     */
    protected static Object convertValue(final Object value, final Class<?> targetClass) throws RuntimeException {
        if (value instanceof String && needsFormattedJsonRead(N.typeOf(targetClass))) {
            return readJsonText((String) value, N.typeOf(targetClass));
        }

        // SDK v2 hands out a byte[] for a B attribute. abacus-common's ByteBufferType uses a write-mode convention
        // (content in [0, position)), so N.convert(byte[], ByteBuffer.class) returns a buffer whose position == limit:
        // it reads as EMPTY under the NIO read convention used by the SDK and by toAttributeValue. Wrap the bytes
        // instead, yielding a read-mode buffer (position 0) like the v1 executor, which hands out getB() as-is.
        if (value instanceof byte[] && targetClass.isAssignableFrom(ByteBuffer.class)) {
            return ByteBuffer.wrap((byte[]) value);
        }

        // N.convert(ByteBuffer, byte[].class) returns null (abacus-common has no ByteBuffer -> byte[] conversion),
        // which would silently drop a native B attribute read into a byte[] property or byte[] target. Copy the
        // remaining bytes from a duplicate so the attribute's buffer position is left untouched.
        if (value instanceof ByteBuffer && targetClass == byte[].class) {
            final ByteBuffer buf = ((ByteBuffer) value).duplicate();
            final byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return bytes;
        }

        if (value instanceof String && targetClass == ByteBuffer.class) {
            // ByteBufferType decodes Base64 text into a buffer positioned after the decoded bytes. Flip it so
            // the bytes are readable from position zero, like the buffers decoded from binary attributes.
            final ByteBuffer buffer = N.convert(value, ByteBuffer.class);
            return buffer == null ? null : buffer.flip();
        }

        if (targetClass == Number.class && !(value instanceof Number)) {
            return toNumber(value);
        }

        // toAttributeValue writes an object array as its JSON text (an S attribute), but N.convert(String, X[].class) wraps the
        // whole text as ONE element whenever String is assignable to the component type (String[], CharSequence[], Object[]):
        // ["a", "b"] came back as {"[\"a\", \"b\"]"}. Parse JSON-array text with the array Type's codec (the inverse of stringOf);
        // any other string keeps the N.convert behavior. isJsonArrayText only checks the brackets, so plain text such as
        // "[a] and [b]" fails to parse: it falls back to N.convert instead of failing the whole item.
        if (value instanceof String && targetClass.isArray() && targetClass.getComponentType().isAssignableFrom(String.class)
                && isJsonArrayText((String) value)) {
            try {
                return N.typeOf(targetClass).valueOf((String) value);
            } catch (final ParsingException e) {
                // not JSON-array text after all: fall through to the lenient N.convert below
            }
        }

        return N.convert(readableBytesOfBuffer(value, targetClass), targetClass);
    }

    /**
     * Returns the readable bytes of an SDK v1 {@link ByteBuffer} that is converted to a type which cannot hold the buffer, so it
     * converts as the {@code byte[]} that SDK v2 hands out for the same {@code B} attribute; returns {@code value} unchanged for
     * any other value, and for a target that can hold the buffer: its own type or a supertype, a collection, or an object array
     * whose elements can be the buffer (these receive the buffer as is, or as their single element).
     *
     * <p>abacus-common converts a ByteBuffer to another type only through its write-mode text form, which is empty for the
     * read-mode buffer of a {@code B} attribute: a String became {@code ""}, and a number, date, UUID, enum, bean, map or typed
     * array {@code null} (a primitive its default), silently losing the attribute that SDK v2 converts from its bytes (failing,
     * for example, for a number, and keeping the bytes' text {@code "[1, 2, 3]"} for a String).</p>
     *
     * @param value the decoded value
     * @param targetClass the class the value is converted to
     * @return a copy of the buffer's remaining bytes (the buffer's position is not changed), or {@code value}
     */
    protected static Object readableBytesOfBuffer(final Object value, final Class<?> targetClass) {
        if (!(value instanceof final ByteBuffer buffer) || targetClass.isInstance(value) || Collection.class.isAssignableFrom(targetClass)
                || (targetClass.isArray() && targetClass.getComponentType().isAssignableFrom(ByteBuffer.class))) {
            return value;
        }

        final ByteBuffer readable = buffer.duplicate();
        final byte[] bytes = new byte[readable.remaining()];
        readable.get(bytes);
        return bytes;
    }

    /**
     * Reads a decoded value as {@link Number}. Number is abstract and has no String factory, so {@code N.convert} rejected every
     * value with {@link UnsupportedOperationException}: text uses JSON numeric inference, as nested Number properties do, and
     * other values fail as the JSON reader rejects them. A decimal that the inferred {@code Double} does not represent exactly
     * becomes a {@link java.math.BigDecimal}, so a DynamoDB number of up to 38 digits reads and writes back unchanged.
     *
     * @throws NumberFormatException if the value is not a number
     * @throws IllegalArgumentException if the value is binary
     */
    protected static Number toNumber(final Object value) {
        if (value instanceof ByteBuffer || value instanceof byte[]) {
            throw new IllegalArgumentException("Cannot convert a binary value to " + Number.class.getName());
        }

        if (!(value instanceof final String text)) {
            return N.fromJson(N.toJson(value), Number.class);
        }

        final Number number = N.fromJson(text, Number.class);

        if (number instanceof final Double inferred) {
            try {
                final java.math.BigDecimal exact = new java.math.BigDecimal(text.strip());

                if (Double.isInfinite(inferred) || java.math.BigDecimal.valueOf(inferred).compareTo(exact) != 0) {
                    return exact;
                }
            } catch (final NumberFormatException e) {
                // JSON number syntax the BigDecimal parser does not accept: keep the inferred value.
            }
        }

        return number;
    }

    /**
     * Returns whether {@code text}, ignoring surrounding whitespace, is bracketed like a JSON array.
     */
    protected static boolean isJsonArrayText(final String text) {
        final String trimmed = text.strip();

        return trimmed.length() >= 2 && trimmed.charAt(0) == '[' && trimmed.charAt(trimmed.length() - 1) == ']';
    }

    /**
     * Returns whether {@code text} is a decimal number literal, the syntax of {@code BigDecimal(String)} with ASCII digits: an
     * optional sign, digits with at most one decimal point (at least one digit in all), and an optional exponent ({@code e} or
     * {@code E}, an optional sign, at least one digit), with nothing before or after it. The {@code toAttributeValue} methods
     * write the text of a number type as a DynamoDB {@code N} value only when it is such a literal; any other text, such as
     * abacus {@code Fraction}'s {@code "3/4"}, is not a DynamoDB number.
     *
     * @param text the text of a number; may be {@code null}
     * @return {@code true} if {@code text} is a decimal number literal, {@code false} otherwise or for {@code null}
     */
    protected static boolean isDecimalNumberText(final String text) {
        final int len = text == null ? 0 : text.length();
        int i = 0;

        if (i < len && (text.charAt(i) == '+' || text.charAt(i) == '-')) {
            i++;
        }

        int digits = 0;

        for (; i < len && isAsciiDigit(text.charAt(i)); i++) {
            digits++;
        }

        if (i < len && text.charAt(i) == '.') {
            for (i++; i < len && isAsciiDigit(text.charAt(i)); i++) {
                digits++;
            }
        }

        if (digits == 0) {
            return false;
        }

        if (i < len && (text.charAt(i) == 'e' || text.charAt(i) == 'E')) {
            i++;

            if (i < len && (text.charAt(i) == '+' || text.charAt(i) == '-')) {
                i++;
            }

            final int exponentStart = i;

            while (i < len && isAsciiDigit(text.charAt(i))) {
                i++;
            }

            if (i == exponentStart) {
                return false;
            }
        }

        return i == len;
    }

    private static boolean isAsciiDigit(final char ch) {
        return ch >= '0' && ch <= '9';
    }

    /**
     * Checks the row type of {@code query(QueryRequest, Class)} before any request is sent. The executors build that Dataset
     * from {@code Map} rows or bean rows only; an array, collection or single-value row type cannot become a Dataset (it has
     * no column names), so it is rejected here instead of after every page has been read and paid for.
     *
     * @param targetClass the requested row type; {@code null} passes (whether {@code null} is allowed is the caller's rule)
     * @throws IllegalArgumentException if {@code targetClass} is not {@code null} and is neither a {@link Map} type nor a bean class
     */
    protected static void checkDatasetRowType(final Class<?> targetClass) throws IllegalArgumentException {
        // Map first: an @Entity-annotated Map subclass takes the Map path of query(QueryRequest, Class). Otherwise the rows
        // reach N.newDataset(Collection), which accepts only Map or bean rows; Type.isBean() is the test the row mappers use to
        // produce bean rows, so every other type would fail there once an item is returned (or, for a single-value type, at
        // the first item that does not have exactly one attribute).
        if (targetClass != null && !Map.class.isAssignableFrom(targetClass) && !N.typeOf(targetClass).isBean()) {
            throw new IllegalArgumentException("Unsupported row type: " + ClassUtil.getCanonicalClassName(targetClass)
                    + ". query(QueryRequest, Class) builds a Dataset only from a Map type or a bean class; use list(QueryRequest, Class) for other row types");
        }
    }

    /**
     * Converts a native map or collection to its declared map, collection, or object-array type while preserving
     * binary leaves. Unconstrained item containers retain their existing raw conversion; exact CopyOnWriteArrayList
     * targets always use bulk construction. Arrays pass their context to each element.
     *
     * <p>Direct bean containers use full-Type {@code N.convert} after their nested values are converted; this
     * preserves its size-aware factories, immutable stand-ins, map-key collision checks, and rejected-entry errors.
     * Their {@code Number} map keys use JSON numeric inference first, with the same collision check.
     * Item containers use JSON rules except for immediate binary elements/values, which retain native factories.
     * JSON containers use no-argument constructors, JSON key parsing, and their existing last-value-wins behavior.
     * In item and JSON containers, a null key or value that the target map rejects is reported as an
     * {@link IllegalArgumentException}, as in direct bean containers. Exact EnumMap construction requires a resolved
     * concrete enum key type.</p>
     *
     * <p>Immutable targets are populated through mutable stand-ins and finalized with {@code N.convert},
     * including nested containers. CopyOnWriteArrayList targets accumulate in an ordinary list and are constructed
     * once; ArrayBlockingQueue targets do too, sized to their elements in every context, including untyped item queues. Other
     * empty queues use their no-argument constructors, which accept no elements where a capacity of zero is rejected. Scalar
     * containers are traversed directly too, avoiding repeated JSON serialization inside each bean.</p>
     *
     * @throws ParsingException if an {@code EnumMap} must be constructed and its resolved key type is not a concrete enum
     * @throws IllegalArgumentException if converted map keys collide in a direct bean container, or a target map rejects a converted
     *         entry
     * @throws RuntimeException if a key or element cannot be converted, or the requested container
     *         cannot be constructed or populated
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected static Object convertNativeContainer(final Object value, final Type<?> targetType, final MappingContext context) throws RuntimeException {
        final Class<?> targetClass = targetType.javaType();
        final boolean copyOnWrite = targetClass == CopyOnWriteArrayList.class;
        final boolean boundedQueue = targetClass == ArrayBlockingQueue.class;

        if (value instanceof final Collection<?> values && values.isEmpty() && !boundedQueue && Queue.class.isAssignableFrom(targetClass)) {
            // Size-aware factories request capacity zero for an empty input, which PriorityQueue and the linked blocking
            // queues and deques reject; their no-argument constructors accept it.
            return N.newCollection((Class<Collection>) targetClass);
        } else if (context == MappingContext.ITEM && !copyOnWrite && !boundedQueue && !targetType.isObjectArray()
                && (!targetType.isParameterizedType() || !isElementConversionNeeded(targetType))) {
            return convertValue(value, targetType.javaType());
        }

        final Type<?> elementType = targetType.isObjectArray() ? targetType.elementType()
                : targetType.isParameterizedType() ? targetType.parameterTypes().get(targetType.isMap() ? 1 : 0) : N.typeOf(Object.class);
        final MappingContext elementContext = context == MappingContext.ITEM && !targetType.isObjectArray() && elementType.javaType() != byte[].class
                && elementType.javaType() != ByteBuffer.class ? MappingContext.JSON : context;
        final boolean jsonContainer = elementContext == MappingContext.JSON;
        final boolean immutable = Immutable.class.isAssignableFrom(targetClass);

        if (targetType.isMap()) {
            final Type<?> keyType = targetType.isParameterizedType() ? targetType.parameterTypes().get(0) : N.typeOf(Object.class);

            if (targetClass == EnumMap.class && !keyType.javaType().isEnum()) {
                throw new ParsingException("EnumMap requires a declared enum key type; resolved key type: " + keyType.name());
            }

            // Keep the original keys until the dependency constructs a direct-bean map. Converting them
            // into an intermediate Map first would hide collisions such as "1" and "01" becoming 1L.
            if (context == MappingContext.MAPPED_BEAN) {
                // Number has no String factory, so the dependency rejects every Number key text. Infer those keys as
                // JSON does, and keep its collision check for keys inferred to the same value.
                final boolean inferNumberKeys = needsNumberInference(keyType);
                final Map<Object, Object> convertedValues = new LinkedHashMap<>();

                for (final Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    final Object key = inferNumberKeys && entry.getKey() instanceof String ? readJsonScalar((String) entry.getKey(), keyType) : entry.getKey();

                    if (inferNumberKeys && convertedValues.containsKey(key)) {
                        throw new IllegalArgumentException("Converted map keys collide for " + targetType.name());
                    }

                    convertedValues.put(key, convertContainerElement(entry.getValue(), elementType, context));
                }

                return N.convert(convertedValues, targetType);
            }

            final Map<Object, Object> result = targetClass == EnumMap.class ? new EnumMap((Class<Enum>) keyType.javaType())
                    : targetClass == ImmutableBiMap.class ? new BiMap<>()
                            : jsonContainer && !immutable ? N.newInstance((Class<Map>) targetClass) : N.newMap((Class<Map>) targetClass);

            final boolean inferNumberKeys = jsonContainer && needsNumberInference(keyType);

            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                final Object key = jsonContainer && entry.getKey() instanceof String
                        ? inferNumberKeys ? readJsonScalar((String) entry.getKey(), keyType) : keyType.valueOf((String) entry.getKey())
                        : convertContainerElement(entry.getKey(), keyType, elementContext);
                final Object convertedValue = convertContainerElement(entry.getValue(), elementType, elementContext);

                try {
                    result.put(key, convertedValue);
                } catch (final NullPointerException | ClassCastException e) {
                    // e.g. a NULL attribute for a ConcurrentHashMap/Hashtable value: report the rejected entry as the
                    // dependency's direct-bean conversion does, instead of a bare JDK exception.
                    throw new IllegalArgumentException("Converted map entry (" + key + ", " + convertedValue + ") is not accepted by " + targetType.name(), e);
                }
            }

            // Finalize both factory-provided and explicit immutable stand-ins at every nesting level;
            // a declared ImmutableMap/List must never expose its mutable construction buffer.
            return N.convert(result, targetClass);
        }

        final Collection<?> values = (Collection<?>) value;

        if (targetType.isObjectArray()) {
            final Object[] result = N.newArray(targetClass.getComponentType(), values.size());
            int index = 0;

            for (final Object element : values) {
                result[index++] = convertContainerElement(element, elementType, context);
            }

            return result;
        }

        // Build an exact CopyOnWriteArrayList once, as the JSON reader does; individual adds copy the
        // growing backing array and turn decoding a long list into quadratic work and allocation.
        final Collection<Object> result = copyOnWrite || boundedQueue || context == MappingContext.MAPPED_BEAN || targetClass == ImmutableCollection.class
                ? new ArrayList<>(values.size())
                : jsonContainer && !immutable ? N.newInstance((Class<Collection>) targetClass) : N.newCollection((Class<Collection>) targetClass);

        for (final Object element : values) {
            result.add(convertContainerElement(element, elementType, elementContext));
        }

        if (boundedQueue) {
            // This queue has no no-argument constructor, and the size-aware native factory requests capacity zero for an
            // empty input, which it rejects. Size it to the elements in every context.
            return new ArrayBlockingQueue<>(Math.max(1, values.size()), false, result);
        }

        return copyOnWrite ? new CopyOnWriteArrayList<>(result)
                : context == MappingContext.MAPPED_BEAN ? N.convert(result, targetType) : N.convert(result, targetClass);
    }

    /**
     * Converts one decoded native value using its full declared type, preserving {@code Object} values as-is.
     * Native wrappers decode their contents in JSON context. Direct native beans use map-to-bean rules;
     * JSON beans retain JSON rules. Direct bean scalar conversion uses the complete Type, including enum
     * representations and generic wrappers. Parameterized text uses its declared codec except where the direct
     * bean mapper preserves scalar-to-container conversion (for example a literal String in List&lt;String&gt;).
     * In JSON context, a non-text value that does not fit its declared type is handled by {@code convertMismatchedValue},
     * as is a binary value for a typed container in a direct bean. Base64 text for a ByteBuffer outside JSON context becomes
     * a buffer readable from position zero; JSON reads, including wrapper contents, keep the JSON codec's buffer convention.
     *
     * <p>A null value produces empty Optional/Nullable wrappers. Holder slots remain null in JSON context and
     * otherwise receive a Holder of the null value for their content type, so a Holder of another wrapper holds that
     * wrapper empty; all other null values remain null. Bean property defaults are applied separately by the property setter.</p>
     */
    protected static Object convertContainerElement(final Object value, final Type<?> targetType, final MappingContext context) {
        if (value instanceof final JsonValue json) {
            return json.read(targetType);
        } else if (value instanceof String && needsFormattedJsonRead(targetType)) {
            return readJsonText((String) value, targetType);
        }

        if (value == null) {
            // JSON slots keep null Holders; direct native arrays previously used N.convert, which supplies
            // Holder(null). Optional/Nullable slots use empty wrappers in both contexts.
            if (!isGenericWrapper(targetType) || (context == MappingContext.JSON && targetType.javaType() == Holder.class)) {
                return null;
            }

            // A NULL attribute cannot record which nested wrapper was empty. Keep the declared wrapper inside a Holder,
            // instead of a null where, for example, an Optional is declared.
            return targetType.javaType() == Holder.class ? Holder.of(convertContainerElement(null, targetType.parameterTypes().get(0), context))
                    : targetType.defaultValue();
        } else if (targetType.javaType() == Object.class) {
            return value;
        }

        if (!(value instanceof String) && isGenericWrapper(targetType)) {
            // Decode the wrapped value with its complete type before wrapping. Serializing a native B
            // value first loses buffer slices or turns bytes into JSON arrays that a buffer cannot read.
            final Object converted = convertContainerElement(value, targetType.parameterTypes().get(0), MappingContext.JSON);
            final Class<?> wrapperClass = targetType.javaType();

            if (wrapperClass == java.util.Optional.class) {
                return java.util.Optional.ofNullable(converted);
            } else if (wrapperClass == com.landawn.abacus.util.u.Optional.class) {
                return com.landawn.abacus.util.u.Optional.ofNullable(converted);
            } else if (wrapperClass == com.landawn.abacus.util.u.Nullable.class) {
                return com.landawn.abacus.util.u.Nullable.of(converted);
            } else {
                return Holder.of(converted);
            }
        }

        if (value instanceof Map && targetType.isBean()) {
            return toEntity((Map<String, Object>) value, targetType, Fn.identity(),
                    context == MappingContext.JSON ? MappingContext.JSON : MappingContext.MAPPED_BEAN);
        }

        if ((value instanceof Collection && (targetType.isCollection() || targetType.isObjectArray())) || (value instanceof Map && targetType.isMap())) {
            return convertNativeContainer(value, targetType, context);
        } else if (value instanceof String && needsNumberInference(targetType)) {
            return readJsonScalar((String) value, targetType);
        } else if (context == MappingContext.MAPPED_BEAN && (value instanceof ByteBuffer || value instanceof byte[])
                && isJsonShapeMismatch(value, targetType)) {
            // The dependency's conversion turned a ByteBuffer into null and a byte array into one-byte arrays or Base64 errors:
            // apply the binary rule of item and JSON values to a direct bean's typed containers too.
            return convertMismatchedValue(value, targetType, context);
        } else if (context == MappingContext.MAPPED_BEAN && !isBinaryConversion(value, targetType.javaType())) {
            // An SDK v1 buffer converts from its readable bytes, as SDK v2's byte[] does, instead of becoming null or "".
            return N.convert(readableBytesOfBuffer(value, targetType.javaType()), targetType);
        } else if (value instanceof String && (targetType.isParameterizedType() || context == MappingContext.JSON)) {
            return targetType.valueOf((String) value);
        } else if (context == MappingContext.JSON && isJsonShapeMismatch(value, targetType)) {
            // A JSON slot holding a native value of another shape (a boolean, a binary value, or a container that does not
            // match the declared type) is converted or rejected as the JSON reader did. Raw-class conversion would store it
            // unconverted, e.g. a Boolean inside a List<Long> or a map inside a List<List<Long>>.
            return convertMismatchedValue(value, targetType, context);
        }

        return convertValue(value, targetType.javaType());
    }

    /**
     * Converts a non-text native value whose shape does not match the declared type: a boolean, a binary value, or a
     * container of another kind.
     *
     * <p>A binary value read as a collection or object array becomes its single element when the elements are binary or
     * untyped, and supplies its byte values when they are other scalars, as the JSON reader read a byte array; text
     * conversion would lose a buffer's bytes. A binary value has no map, pair, bean, or nested-container form and is
     * rejected.</p>
     *
     * <p>Any other value for a collection or object array becomes its single element, read as the element type, as the JSON
     * reader reads a value that is not an array; reading the value's JSON text with the container's codec instead left a map
     * unconverted inside a {@code List<List<..>>}. A {@code Number} reads the value's JSON form with numeric inference. A number
     * (such as a byte of a binary value) or a boolean is read as the JSON reader reads that token for the declared type: a
     * number is a {@code Date}'s epoch milliseconds and a {@code Character}'s code point, not their text. A map or collection
     * for any other type is read through the declared type's JSON codec, as the JSON reader did.</p>
     *
     * @throws IllegalArgumentException if a binary value has no form of the declared type
     * @throws RuntimeException if the codec, the JSON reader, or an element conversion rejects the value, for example with a
     *         {@code NumberFormatException}, {@code ParsingException}, or {@code DateTimeParseException}
     */
    protected static Object convertMismatchedValue(final Object value, final Type<?> targetType, final MappingContext context) {
        final Type<?> elementType = targetType.isObjectArray() ? targetType.elementType()
                : targetType.isCollection() && targetType.isParameterizedType() ? targetType.parameterTypes().get(0) : null;

        if (value instanceof ByteBuffer || value instanceof byte[]) {
            if (elementType != null && acceptsWholeBinary(elementType)) {
                return convertNativeContainer(singletonList(value), targetType, context);
            } else if (elementType != null && !elementType.isCollection() && !elementType.isMap() && !elementType.isBean() && !elementType.isArray()) {
                return convertNativeContainer(byteValues(value), targetType, context);
            }

            // Raw conversion silently produced null for a ByteBuffer, and a parsing error for a byte array.
            throw new IllegalArgumentException("Cannot convert a binary value to " + targetType.name());
        }

        if (elementType != null) {
            return convertNativeContainer(singletonList(value), targetType, context);
        } else if (needsNumberInference(targetType)) {
            return readJsonScalar(N.toJson(value), targetType);
        } else if (value instanceof Number || value instanceof Boolean) {
            // Read the token as JSON does: a type's text codec read 1 as the text "1", an ambiguous date or the character '1'.
            final List<?> result = N.fromJson(N.toJson(singletonList(value)), JsonDeserConfig.create().setElementType(targetType), List.class);
            return result.get(0);
        }

        return targetType.valueOf(N.toJson(value));
    }

    /**
     * Returns a mutable list holding only {@code value}, the input for converting a single value as a container's element.
     */
    protected static List<Object> singletonList(final Object value) {
        final List<Object> result = new ArrayList<>(1);
        result.add(value);
        return result;
    }

    /**
     * Returns whether a collection element of this type holds a whole binary value: {@code byte[]}, {@link ByteBuffer}, or
     * {@code Object}, possibly enclosed in supported generic wrappers.
     */
    protected static boolean acceptsWholeBinary(Type<?> type) {
        while (isGenericWrapper(type)) {
            type = type.parameterTypes().get(0);
        }

        final Class<?> cls = type.javaType();
        return cls == byte[].class || cls == ByteBuffer.class || cls == Object.class;
    }

    /**
     * Returns the bytes of a byte array, or the remaining bytes of a ByteBuffer without consuming it, as a list.
     */
    protected static List<Byte> byteValues(final Object binary) {
        final byte[] bytes;

        if (binary instanceof final byte[] array) {
            bytes = array;
        } else {
            final ByteBuffer buffer = ((ByteBuffer) binary).duplicate();
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }

        final List<Byte> result = new ArrayList<>(bytes.length);

        for (final byte b : bytes) {
            result.add(b);
        }

        return result;
    }

    /**
     * Returns whether {@code convertValue} must handle a binary conversion that {@code N.convert} gets wrong:
     * SDK v1's ByteBuffer to byte[] (unsupported), SDK v2's byte[] to ByteBuffer (write-mode buffer), or Base64
     * text to ByteBuffer (positioned after the bytes).
     */
    protected static boolean isBinaryConversion(final Object value, final Class<?> targetClass) {
        return targetClass == byte[].class ? value instanceof ByteBuffer
                : value instanceof byte[] ? !targetClass.isInstance(value) && targetClass.isAssignableFrom(ByteBuffer.class)
                        : targetClass == ByteBuffer.class && value instanceof String;
    }

    /**
     * Resolves the DynamoDB attribute name for a bean property.
     *
     * <p>Returns the {@code @Column} name when present; otherwise applies the naming policy to the
     * property name ({@link NamingPolicy#CAMEL_CASE} keeps the name unchanged).</p>
     *
     * @param propInfo the bean property metadata
     * @param namingPolicy the naming policy to apply when no {@code @Column} name is present
     * @return the attribute name to use for the property, never {@code null}
     */
    protected static String getAttrName(final PropInfo propInfo, final NamingPolicy namingPolicy) {
        if (propInfo.columnName.isPresent()) {
            return propInfo.columnName.get();
        } else if (namingPolicy == NamingPolicy.CAMEL_CASE) {
            return propInfo.name;
        } else {
            // Called for every property of every written entity; NamingPolicy.convert is pure but uncached (~100 ns and
            // an allocation per call). Keys are bean property names, so each cache is bounded by the bean metadata.
            return attrNamePool.get(namingPolicy).computeIfAbsent(propInfo.name, namingPolicy::convert);
        }
    }

    private static final Map<NamingPolicy, Map<String, String>> attrNamePool = new EnumMap<>(NamingPolicy.class);

    static {
        for (final NamingPolicy namingPolicy : NamingPolicy.values()) {
            attrNamePool.put(namingPolicy, new ConcurrentHashMap<>());
        }
    }

    /**
     * Returns whether a parameterized item container constrains its values or its native String map keys.
     * Unconstrained Object slots retain their decoded values, including binary values, as-is.
     */
    protected static boolean isElementConversionNeeded(final Type<?> propType) {
        if (propType.isMap()) {
            final Class<?> keyClass = propType.parameterTypes().get(0).javaType();
            final Class<?> valueClass = propType.parameterTypes().get(1).javaType();

            return valueClass != Object.class || (keyClass != String.class && keyClass != Object.class);
        }

        return propType.parameterTypes().get(0).javaType() != Object.class;
    }
}
