package com.berry.clickhouse.tcp.client.misc;



public class StrUtil {

    public static String getOrDefault( String origin, String defaultValue) {
        if (origin == null || origin.isEmpty())
            return defaultValue;
        return origin;
    }

    public static String toString( Object object) {
        return object == null ? "" : object.toString();
    }

    public static boolean isEmpty( String str) {
        return str == null || str.isEmpty();
    }

    public static boolean isBlank( String str) {
        return str == null || str.trim().isEmpty();
    }

    public static boolean isNotEmpty( String str) {
        return !isEmpty(str);
    }

    public static boolean isNotBlank( String str) {
        return !isBlank(str);
    }
}
