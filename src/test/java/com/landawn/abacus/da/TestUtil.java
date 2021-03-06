/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.DateUtil;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;

/**
 *
 * @since 0.8
 *
 * @author Haiyang Li
 */
public final class TestUtil {
    protected static final Logger logger = LoggerFactory.getLogger(TestUtil.class);
    private static final String IDEN = "    ";
    private static final Map<Class<?>, Object> typeValues = new HashMap<>();
    static {
        typeValues.put(boolean.class, false);
        typeValues.put(Boolean.class, Boolean.TRUE);
        typeValues.put(char.class, '0');
        typeValues.put(Character.class, '1');
        typeValues.put(byte.class, (byte) 2);
        typeValues.put(Byte.class, (byte) 3);
        typeValues.put(short.class, (short) 4);
        typeValues.put(Short.class, (short) 5);
        typeValues.put(int.class, 6);
        typeValues.put(Integer.class, 7);
        typeValues.put(long.class, 8l);
        typeValues.put(Long.class, 9l);
        typeValues.put(float.class, 10.1f);
        typeValues.put(Float.class, 11.2f);
        typeValues.put(double.class, 12.3d);
        typeValues.put(Double.class, 14.3d);

        typeValues.put(String.class, "1970");

        typeValues.put(Calendar.class, DateUtil.parseCalendar("1970-01-01'T'10:10:10'Z'"));
        typeValues.put(java.util.Date.class, DateUtil.parseCalendar("1970-01-01'T'10:10:11'Z'"));
        typeValues.put(Date.class, DateUtil.parseCalendar("1970-01-01'T'10:10:12'Z'"));
        typeValues.put(Time.class, DateUtil.parseCalendar("1970-01-01'T'10:10:13'Z'"));
        typeValues.put(Timestamp.class, DateUtil.parseCalendar("1970-01-01'T'10:10:14'Z'"));

    }

    private TestUtil() {
        // singleton.
    }

    public static <T> T createEntity(Class<T> entityClass) {
        return createEntity(entityClass, false);
    }

    public static <T> T createEntity(Class<T> entityClass, boolean withFixedValues) {
        if (!ClassUtil.isEntity(entityClass)) {
            throw new RuntimeException(entityClass.getCanonicalName() + " is not a valid entity class with property getter/setter method");
        }

        T entity = N.newInstance(entityClass);

        if (withFixedValues) {
            for (Method method : ClassUtil.getPropSetMethods(entityClass).values()) {
                ClassUtil.setPropValue(entity, method, typeValues.get(method.getParameterTypes()[0]));
            }
        } else {
            N.fill(entity);
        }

        return entity;
    }

    public static <T> List<T> createEntityList(Class<T> entityClass, int size) {
        return createEntityList(entityClass, size, false);
    }

    public static <T> List<T> createEntityList(Class<T> entityClass, int size, boolean withFixedValues) {
        final List<T> list = N.newArrayList(size);

        for (int i = 0; i < size; i++) {
            list.add(createEntity(entityClass, withFixedValues));
        }

        return list;
    }

    public static <T> T[] createEntityArray(Class<T> entityClass, int size) {
        return createEntityArray(entityClass, size, false);
    }

    public static <T> T[] createEntityArray(Class<T> entityClass, int size, boolean withFixedValues) {
        final T[] a = N.newArray(entityClass, size);

        for (int i = 0; i < size; i++) {
            a[i] = createEntity(entityClass, withFixedValues);
        }

        return a;
    }

    public static List<Object> executeMethod(Object instance, List<Object[]> parameters) {
        Class<?> cls = instance.getClass();
        List<Method> methodList = new ArrayList<>();

        for (Method m : cls.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers()) && !Modifier.isAbstract(m.getModifiers())) {
                methodList.add(m);
            }
        }

        return executeMethod(instance, methodList, parameters);
    }

    public static List<Object> executeMethod(Object instance, List<Method> methodList, List<Object[]> parametersList) {
        if (N.notNullOrEmpty(parametersList) && (parametersList.size() != methodList.size())) {
            throw new IllegalArgumentException("the size of parameters list must be same as the size of method list");
        }

        List<Object> resultList = new ArrayList<>();
        Object result = null;
        Object[] parameters = null;
        Method method = null;

        for (int i = 0; i < methodList.size(); i++) {
            method = methodList.get(i);

            Class<?>[] parameterTypes = method.getParameterTypes();

            if (N.notNullOrEmpty(parametersList)) {
                parameters = parametersList.get(i);
            } else if (N.notNullOrEmpty(parameterTypes)) {
                parameters = new Object[parameterTypes.length];

                for (int k = 0; k < parameterTypes.length; k++) {
                    parameters[k] = N.defaultValueOf(parameterTypes[k]);
                }
            } else {
                parameters = null;
            }

            boolean isAccessible = method.isAccessible();

            try {
                method.setAccessible(true);

                if (N.isNullOrEmpty(parameterTypes)) {
                    method.invoke(instance);
                } else {
                    method.invoke(instance, parameters);
                }
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Fail to run method: " + method.getName(), e);
                }

                result = e;
            } finally {
                method.setAccessible(isAccessible);
            }

            resultList.add(result);
        }

        return resultList;
    }

    public static void generateUnitTest(Class<?> cls, boolean inFail) {
        String simpleClassName = cls.getSimpleName();
        String canonicalClassName = cls.getCanonicalName();
        Set<String> importClasses = N.asSortedSet();
        Map<String, Integer> methodNameMap = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        sb.append("import junit.framework.TestCase;" + IOUtil.LINE_SEPARATOR);
        sb.append("import org.junit.Test;" + IOUtil.LINE_SEPARATOR);
        sb.append("import " + canonicalClassName + ";" + IOUtil.LINE_SEPARATOR);

        sb.append(IOUtil.LINE_SEPARATOR);
        sb.append("public class " + cls.getSimpleName() + "Test extends TestCase {");
        sb.append(IOUtil.LINE_SEPARATOR);
        sb.append(IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + simpleClassName + " getInstance() {" + IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + IDEN + "return null; // TODO" + IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + "}" + IOUtil.LINE_SEPARATOR);
        sb.append(IOUtil.LINE_SEPARATOR);

        sb.append(IDEN + "@Override" + IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + "protected void setUp() throws Exception {" + IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + IDEN + "super.setUp();" + IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + "}" + IOUtil.LINE_SEPARATOR);
        sb.append(IOUtil.LINE_SEPARATOR);

        sb.append(IDEN + "@Override" + IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + "protected void tearDown() throws Exception {" + IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + IDEN + "super.tearDown();" + IOUtil.LINE_SEPARATOR);
        sb.append(IDEN + "}" + IOUtil.LINE_SEPARATOR);
        sb.append(IOUtil.LINE_SEPARATOR);

        Method[] methods = cls.getDeclaredMethods();

        for (Method m : methods) {
            if (Modifier.isPublic(m.getModifiers())) {
                importClasses.add(m.getReturnType().getCanonicalName());

                for (Class<?> pt : m.getParameterTypes()) {
                    importClasses.add(pt.getCanonicalName());
                }

                Integer num = methodNameMap.get(m.getName());

                if (num == null) {
                    num = 0;
                }

                methodNameMap.put(m.getName(), num + 1);

                sb.append(IOUtil.LINE_SEPARATOR);
                sb.append(IDEN + "@Test" + IOUtil.LINE_SEPARATOR);
                sb.append(IDEN + "public void test_" + m.getName() + "_" + num + "0() throws Exception {" + IOUtil.LINE_SEPARATOR);

                String parameterStr = "";

                int i = 0;
                String defaultValue = null;

                for (Class<?> pt : m.getParameterTypes()) {
                    defaultValue = N.stringOf(N.defaultValueOf(pt));

                    if (float.class.equals(pt)) {
                        defaultValue += "f";
                    }

                    if (i > 0) {
                        parameterStr += ", ";
                    }

                    parameterStr += ("param" + i);
                    sb.append(IDEN + IDEN + pt.getSimpleName() + " param" + i + " = " + defaultValue + ";" + IOUtil.LINE_SEPARATOR);
                    i++;
                }

                String instanceVarName = null;

                if (Modifier.isStatic(m.getModifiers())) {
                    instanceVarName = simpleClassName;
                } else {
                    sb.append(IDEN + IDEN + simpleClassName + " instance = getInstance();" + IOUtil.LINE_SEPARATOR);
                    instanceVarName = "instance";
                }

                String iden = IDEN + IDEN;

                if (inFail) {
                    iden += IDEN;
                    sb.append(IOUtil.LINE_SEPARATOR);
                    sb.append(IDEN + IDEN + "try {" + IOUtil.LINE_SEPARATOR);
                }

                if (void.class.equals(m.getReturnType())) {
                    sb.append(iden + instanceVarName + "." + m.getName() + "(" + parameterStr + ");" + IOUtil.LINE_SEPARATOR);
                } else {
                    sb.append(iden + m.getReturnType().getSimpleName() + " result = " + instanceVarName + "." + m.getName() + "(" + parameterStr + ");"
                            + IOUtil.LINE_SEPARATOR);
                    sb.append(iden + "System.out.println(result);" + IOUtil.LINE_SEPARATOR);
                }

                if (inFail) {
                    sb.append(IDEN + IDEN + "} catch(Exception e) {" + IOUtil.LINE_SEPARATOR);
                    sb.append(IDEN + IDEN + IDEN + "// ignore" + IOUtil.LINE_SEPARATOR);
                    sb.append(IDEN + IDEN + "}" + IOUtil.LINE_SEPARATOR);
                }

                sb.append(IDEN + "}" + IOUtil.LINE_SEPARATOR);
            }
        }

        sb.append('}');

        N.println("========================================================================================================================");
        N.println(IOUtil.LINE_SEPARATOR);

        for (String className : importClasses) {
            if (className.startsWith("java.lang") || (className.indexOf('.') < 0)) {
                // skipped
            } else {
                N.println("import " + className + ";");
            }
        }

        N.println(sb.toString());

        N.println(IOUtil.LINE_SEPARATOR);
        N.println("========================================================================================================================");
    }
}
