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
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.Dates;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;

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
        typeValues.put(long.class, 8L);
        typeValues.put(Long.class, 9L);
        typeValues.put(float.class, 10.1f);
        typeValues.put(Float.class, 11.2f);
        typeValues.put(double.class, 12.3d);
        typeValues.put(Double.class, 14.3d);

        typeValues.put(String.class, "1970");

        typeValues.put(Calendar.class, Dates.parseCalendar("1970-01-01'T'10:10:10'Z'"));
        typeValues.put(java.util.Date.class, Dates.parseCalendar("1970-01-01'T'10:10:11'Z'"));
        typeValues.put(Date.class, Dates.parseCalendar("1970-01-01'T'10:10:12'Z'"));
        typeValues.put(Time.class, Dates.parseCalendar("1970-01-01'T'10:10:13'Z'"));
        typeValues.put(Timestamp.class, Dates.parseCalendar("1970-01-01'T'10:10:14'Z'"));

    }

    private TestUtil() {
        // singleton.
    }

    /**
     * 
     *
     * @param <T> 
     * @param entityClass 
     * @return 
     */
    public static <T> T createEntity(Class<T> entityClass) {
        return createEntity(entityClass, false);
    }

    /**
     * 
     *
     * @param <T> 
     * @param entityClass 
     * @param withFixedValues 
     * @return 
     */
    public static <T> T createEntity(Class<T> entityClass, boolean withFixedValues) {
        if (!Beans.isBeanClass(entityClass)) {
            throw new RuntimeException(entityClass.getCanonicalName() + " is not a valid entity class with property getter/setter method");
        }

        T entity = N.newInstance(entityClass);

        if (withFixedValues) {
            for (Method method : Beans.getPropSetters(entityClass).values()) {
                Beans.setPropValue(entity, method, typeValues.get(method.getParameterTypes()[0]));
            }
        } else {
            Beans.randomize(entity);
        }

        return entity;
    }

    /**
     * 
     *
     * @param <T> 
     * @param entityClass 
     * @param size 
     * @return 
     */
    public static <T> List<T> createEntityList(Class<T> entityClass, int size) {
        return createEntityList(entityClass, size, false);
    }

    /**
     * 
     *
     * @param <T> 
     * @param entityClass 
     * @param size 
     * @param withFixedValues 
     * @return 
     */
    public static <T> List<T> createEntityList(Class<T> entityClass, int size, boolean withFixedValues) {
        final List<T> list = N.newArrayList(size);

        for (int i = 0; i < size; i++) {
            list.add(createEntity(entityClass, withFixedValues));
        }

        return list;
    }

    /**
     * 
     *
     * @param <T> 
     * @param entityClass 
     * @param size 
     * @return 
     */
    public static <T> T[] createEntityArray(Class<T> entityClass, int size) {
        return createEntityArray(entityClass, size, false);
    }

    /**
     * 
     *
     * @param <T> 
     * @param entityClass 
     * @param size 
     * @param withFixedValues 
     * @return 
     */
    public static <T> T[] createEntityArray(Class<T> entityClass, int size, boolean withFixedValues) {
        final T[] a = N.newArray(entityClass, size);

        for (int i = 0; i < size; i++) {
            a[i] = createEntity(entityClass, withFixedValues);
        }

        return a;
    }

    /**
     * 
     *
     * @param instance 
     * @param parameters 
     * @return 
     */
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

    /**
     * 
     *
     * @param instance 
     * @param methodList 
     * @param parametersList 
     * @return 
     */
    public static List<Object> executeMethod(Object instance, List<Method> methodList, List<Object[]> parametersList) {
        if (N.notEmpty(parametersList) && (parametersList.size() != methodList.size())) {
            throw new IllegalArgumentException("the size of parameters list must be same as the size of method list");
        }

        List<Object> resultList = new ArrayList<>();
        Object result = null;
        Object[] parameters = null;
        Method method = null;

        for (int i = 0; i < methodList.size(); i++) {
            method = methodList.get(i);

            Class<?>[] parameterTypes = method.getParameterTypes();

            if (N.notEmpty(parametersList)) {
                parameters = parametersList.get(i);
            } else if (N.notEmpty(parameterTypes)) {
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

                if (N.isEmpty(parameterTypes)) {
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

    /**
     * 
     *
     * @param cls 
     * @param inFail 
     */
    public static void generateUnitTest(Class<?> cls, boolean inFail) {
        String simpleClassName = cls.getSimpleName();
        String canonicalClassName = cls.getCanonicalName();
        Set<String> importClasses = N.toSortedSet();
        Map<String, Integer> methodNameMap = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        sb.append("import junit.framework.TestCase;" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append("import org.junit.jupiter.api.Test;" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append("import " + canonicalClassName + ";" + IOUtil.LINE_SEPARATOR_UNIX);

        sb.append(IOUtil.LINE_SEPARATOR_UNIX);
        sb.append("public class " + cls.getSimpleName() + "Test extends TestCase {");
        sb.append(IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + simpleClassName + " getInstance() {" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + IDEN + "return null; // TODO" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + "}" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IOUtil.LINE_SEPARATOR_UNIX);

        sb.append(IDEN + "@Override" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + "protected void setUp() throws Exception {" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + IDEN + "super.setUp();" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + "}" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IOUtil.LINE_SEPARATOR_UNIX);

        sb.append(IDEN + "@Override" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + "protected void tearDown() throws Exception {" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + IDEN + "super.tearDown();" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IDEN + "}" + IOUtil.LINE_SEPARATOR_UNIX);
        sb.append(IOUtil.LINE_SEPARATOR_UNIX);

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

                sb.append(IOUtil.LINE_SEPARATOR_UNIX);
                sb.append(IDEN + "@Test" + IOUtil.LINE_SEPARATOR_UNIX);
                sb.append(IDEN + "public void test_" + m.getName() + "_" + num + "0() throws Exception {" + IOUtil.LINE_SEPARATOR_UNIX);

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
                    sb.append(IDEN + IDEN + pt.getSimpleName() + " param" + i + " = " + defaultValue + ";" + IOUtil.LINE_SEPARATOR_UNIX);
                    i++;
                }

                String instanceVarName = null;

                if (Modifier.isStatic(m.getModifiers())) {
                    instanceVarName = simpleClassName;
                } else {
                    sb.append(IDEN + IDEN + simpleClassName + " instance = getInstance();" + IOUtil.LINE_SEPARATOR_UNIX);
                    instanceVarName = "instance";
                }

                String iden = IDEN + IDEN;

                if (inFail) {
                    iden += IDEN;
                    sb.append(IOUtil.LINE_SEPARATOR_UNIX);
                    sb.append(IDEN + IDEN + "try {" + IOUtil.LINE_SEPARATOR_UNIX);
                }

                if (void.class.equals(m.getReturnType())) {
                    sb.append(iden + instanceVarName + "." + m.getName() + "(" + parameterStr + ");" + IOUtil.LINE_SEPARATOR_UNIX);
                } else {
                    sb.append(iden + m.getReturnType().getSimpleName() + " result = " + instanceVarName + "." + m.getName() + "(" + parameterStr + ");"
                            + IOUtil.LINE_SEPARATOR_UNIX);
                    sb.append(iden + "System.out.println(result);" + IOUtil.LINE_SEPARATOR_UNIX);
                }

                if (inFail) {
                    sb.append(IDEN + IDEN + "} catch(Exception e) {" + IOUtil.LINE_SEPARATOR_UNIX);
                    sb.append(IDEN + IDEN + IDEN + "// ignore" + IOUtil.LINE_SEPARATOR_UNIX);
                    sb.append(IDEN + IDEN + "}" + IOUtil.LINE_SEPARATOR_UNIX);
                }

                sb.append(IDEN + "}" + IOUtil.LINE_SEPARATOR_UNIX);
            }
        }

        sb.append('}');

        N.println("========================================================================================================================");
        N.println(IOUtil.LINE_SEPARATOR_UNIX);

        for (String className : importClasses) {
            if (className.startsWith("java.lang") || (className.indexOf('.') < 0)) {
                // skipped
            } else {
                N.println("import " + className + ";");
            }
        }

        N.println(sb.toString());

        N.println(IOUtil.LINE_SEPARATOR_UNIX);
        N.println("========================================================================================================================");
    }
}
