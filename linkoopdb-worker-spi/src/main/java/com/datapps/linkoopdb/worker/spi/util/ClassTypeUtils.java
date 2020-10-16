package com.datapps.linkoopdb.worker.spi.util;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datapps.linkoopdb.jdbc.error.Error;
import com.datapps.linkoopdb.jdbc.error.ErrorCode;

/**
 * @author yukkit.zhang
 * @date 2020/8/20
 */
public class ClassTypeUtils {

    private static final Set<Class> BOX_CLASS = new HashSet<>();

    static {
        BOX_CLASS.add(Boolean.class);
        BOX_CLASS.add(Byte.class);
        // BOX_CLASS.add(String.class);
        BOX_CLASS.add(Short.class);
        BOX_CLASS.add(Integer.class);
        BOX_CLASS.add(Long.class);
        BOX_CLASS.add(Float.class);
        BOX_CLASS.add(Double.class);
    }

    /**
     * 基本类型或者包装类型
     */
    public static boolean simpleType(Class cls) {
        return cls.isPrimitive() || BOX_CLASS.contains(cls);
    }

    public static boolean stringType(Class cls) {
        return String.class.isAssignableFrom(cls);
    }

    public static boolean booleanType(Class cls) {
        return Boolean.class.isAssignableFrom(cls);
    }

    public static boolean castBoolean(String value) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.valueOf(value);
        } else {
            throw new IllegalArgumentException(value);
        }
    }

    /**
     * 基本类型或者包装类型
     */
    public static Object cast(Class cls, String value) {
        if (stringType(cls)) {
            return value;
        } else if (booleanType(cls)) {
            return castBoolean(value);
        } else if (simpleType(cls)) {
            try {
                Class<?> clazz = Class.forName(cls.getName());
                Method method = clazz.getMethod("valueOf", String.class);
                return method.invoke(null, value);
            } catch (NoSuchMethodException nse) {
                // code error
                throw Error.error(ErrorCode.GENERAL_ERROR, nse);
            } catch (Exception e) {
                // NumberFormatException
                throw Error.error(ErrorCode.X_42556, e.getMessage());
            }
        } else {
            throw Error.error(ErrorCode.X_42556, "not support value type.");
        }
    }

    /**
     * 集合类 包含list、set
     */
    public static boolean collectionType(Class cls) {
        return Collection.class.isAssignableFrom(cls);
    }

    /**
     * listType
     */
    public static boolean listType(Class cls) {
        return List.class.isAssignableFrom(cls);
    }

    /**
     * setType
     */
    public static boolean setType(Class cls) {
        return Set.class.isAssignableFrom(cls);
    }

    /**
     * mapType
     */
    public static boolean mapType(Class cls) {
        return Map.class.isAssignableFrom(cls);
    }


    /**
     * arrayType
     */
    public static boolean arrayType(Class cls) {
        return cls.isArray();
    }

    /**
     * primitiveArrayType
     */
    public static boolean primitiveArray(Class cls) {

        if (byte[].class.isAssignableFrom(cls)) {
            return true;
        } else if (short[].class.isAssignableFrom(cls)) {
            return true;
        } else if (int[].class.isAssignableFrom(cls)) {
            return true;
        } else if (long[].class.isAssignableFrom(cls)) {
            return true;
        } else if (char[].class.isAssignableFrom(cls)) {
            return true;
        } else if (float[].class.isAssignableFrom(cls)) {
            return true;
        } else if (double[].class.isAssignableFrom(cls)) {
            return true;
        } else {
            return boolean[].class.isAssignableFrom(cls);
        }
    }
}
