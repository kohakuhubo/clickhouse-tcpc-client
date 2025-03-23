package com.berry.clickhouse.tcp.client.misc;

/**
 * StrUtil类提供了一些字符串操作的工具方法
 */
public class StrUtil {

    /**
     * 获取字符串或默认值
     * 
     * @param origin 原始字符串
     * @param defaultValue 默认值
     * @return 返回原始字符串或默认值
     */
    public static String getOrDefault(String origin, String defaultValue) {
        if (origin == null || origin.isEmpty())
            return defaultValue; // 返回默认值
        return origin; // 返回原始字符串
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
