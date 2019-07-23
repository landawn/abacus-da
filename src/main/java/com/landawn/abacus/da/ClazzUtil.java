package com.landawn.abacus.da;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.ReadOnlyId;
import com.landawn.abacus.util.Array;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.N;

// TODO: Auto-generated Javadoc
/**
 * The Class ClazzUtil.
 */
final class ClazzUtil {

    /**
     * Instantiates a new clazz util.
     */
    private ClazzUtil() {
        // utility class
    }

    /** The Constant CLASS_MASK. */
    static final Class<?> CLASS_MASK = ClassMask.class;

    /** The Constant METHOD_MASK. */
    static final Method METHOD_MASK = internalGetDeclaredMethod(ClassMask.class, "methodMask");

    /** The Constant FIELD_MASK. */
    static final Field FIELD_MASK;

    static {
        try {
            FIELD_MASK = ClassMask.class.getDeclaredField(ClassMask.FIELD_MASK);
        } catch (Exception e) {
            throw N.toRuntimeException(e);
        }
    }

    /** The Constant idPropNamesMap. */
    private static final Map<Class<?>, List<String>> idPropNamesMap = new ConcurrentHashMap<>();

    /** The Constant fakeIds. */
    private static final List<String> fakeIds = ImmutableList.of("not_defined_fake_id_in_abacus_" + N.uuid());

    /**
     * Gets the id field names.
     *
     * @param targetClass the target class
     * @return the id field names
     */
    static List<String> getIdFieldNames(final Class<?> targetClass) {
        return getIdFieldNames(targetClass, false);
    }

    /**
     * Gets the id field names.
     *
     * @param targetClass the target class
     * @param fakeIdForEmpty the fake id for empty
     * @return the id field names
     */
    static List<String> getIdFieldNames(final Class<?> targetClass, boolean fakeIdForEmpty) {
        List<String> idPropNames = idPropNamesMap.get(targetClass);

        if (idPropNames == null) {
            final Set<String> idPropNameSet = new LinkedHashSet<>();
            final Set<Field> allFields = new LinkedHashSet<>();

            for (Class<?> superClass : ClassUtil.getAllSuperclasses(targetClass)) {
                allFields.addAll(Array.asList(superClass.getDeclaredFields()));
            }

            allFields.addAll(Array.asList(targetClass.getDeclaredFields()));

            for (Field field : allFields) {
                if (ClassUtil.getPropGetMethod(targetClass, field.getName()) == null
                        && ClassUtil.getPropGetMethod(targetClass, ClassUtil.formalizePropName(field.getName())) == null) {
                    continue;
                }

                if (field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(ReadOnlyId.class)) {
                    idPropNameSet.add(field.getName());
                } else {
                    try {
                        if (field.isAnnotationPresent(javax.persistence.Id.class)) {
                            idPropNameSet.add(field.getName());
                        }
                    } catch (Throwable e) {
                        // ignore
                    }
                }
            }

            if (targetClass.isAnnotationPresent(Id.class)) {
                String[] values = targetClass.getAnnotation(Id.class).value();
                N.checkArgNotNullOrEmpty(values, "values for annotation @Id on Type/Class can't be null or empty");
                idPropNameSet.addAll(Arrays.asList(values));
            }

            if (N.isNullOrEmpty(idPropNameSet)) {
                final Field idField = ClassUtil.getPropField(targetClass, "id");
                final Set<Class<?>> idType = N.<Class<?>> asSet(int.class, Integer.class, long.class, Long.class, String.class, Timestamp.class, UUID.class);

                if (idField != null && idType.contains(idField.getType())) {
                    idPropNameSet.add(idField.getName());
                }
            }

            idPropNames = ImmutableList.copyOf(idPropNameSet);
            idPropNamesMap.put(targetClass, idPropNames);
        }

        return N.isNullOrEmpty(idPropNames) && fakeIdForEmpty ? fakeIds : idPropNames;
    }

    /**
     * Checks if is fake id.
     *
     * @param idPropNames the id prop names
     * @return true, if is fake id
     */
    static boolean isFakeId(List<String> idPropNames) {
        if (idPropNames != null && idPropNames.size() == 1 && fakeIds.get(0).equals(idPropNames.get(0))) {
            return true;
        }

        return false;
    }

    /**
     * Internal get declared method.
     *
     * @param cls the cls
     * @param methodName the method name
     * @param parameterTypes the parameter types
     * @return the method
     */
    static Method internalGetDeclaredMethod(final Class<?> cls, final String methodName, final Class<?>... parameterTypes) {
        Method method = null;

        try {
            method = cls.getDeclaredMethod(methodName, parameterTypes);
        } catch (NoSuchMethodException e) {
            // ignore.
        }

        if (method == null) {
            Method[] methods = cls.getDeclaredMethods();

            for (Method m : methods) {
                if (m.getName().equalsIgnoreCase(methodName) && N.equals(parameterTypes, m.getParameterTypes())) {
                    method = m;

                    break;
                }
            }
        }

        return method;
    }

    /**
     * The Class ClassMask.
     */
    static final class ClassMask {

        /** The Constant FIELD_MASK. */
        static final String FIELD_MASK = "FIELD_MASK";

        /**
         * Method mask.
         */
        static final void methodMask() {
        }
    }
}
