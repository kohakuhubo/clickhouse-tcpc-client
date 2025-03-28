package com.berry.clickhouse.tcp.client.misc;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CollectionUtil类提供了一些集合操作的工具方法
 */
public class CollectionUtil {

    /**
     * 连接两个列表
     * 
     * @param first 第一个列表
     * @param second 第二个列表
     * @return 连接后的列表
     */
    public static <T> List<T> concat(List<T> first, List<T> second) {
        return Stream.concat(first.stream(), second.stream()).collect(Collectors.toList()); // 返回连接后的列表
    }

    @SafeVarargs
    public static <T> List<T> concat(List<T> originList, T... elements) {
        return Stream.concat(originList.stream(), Stream.of(elements)).collect(Collectors.toList());
    }

    public static <T> List<T> repeat(int time, List<T> origin) {
        assert time > 0;
        List<T> result = origin;
        for (int i = 0; i < time - 1; i++) {
            result = concat(result, origin);
        }
        return result;
    }

    public static List<String> filterIgnoreCase(List<String> set, String keyword) {
        return set.stream()
                .filter(key -> key.equalsIgnoreCase(keyword))
                .collect(Collectors.toList());
    }

    public static Map<String, String> filterKeyIgnoreCase(Properties properties, String keyword) {
        Map<String, String> props = new HashMap<>();
        properties.forEach((k, v) -> props.put(k.toString(), v.toString()));
        return filterKeyIgnoreCase(props, keyword);
    }

    public static <V> Map<String, V> filterKeyIgnoreCase(Map<String, V> map, String keyword) {
        return map.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(keyword))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static <K, V> Map<K, V> mergeMapKeepFirst(Map<K, V> one, Map<K, V> other) {
        return Stream.concat(one.entrySet().stream(), other.entrySet().stream()).collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (former, latter) -> former));
    }

    public static <K, V> Map<K, V> mergeMapKeepLast(Map<K, V> one, Map<K, V> other) {
        return Stream.concat(one.entrySet().stream(), other.entrySet().stream()).collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (former, latter) -> latter));
    }

    public static <K, V> void mergeMapInPlaceKeepFirst(Map<K, V> one,  Map<K, V> other) {
        if (other != null)
            other.forEach(one::putIfAbsent);
    }

    public static <K, V> void mergeMapInPlaceKeepLast(Map<K, V> one,  Map<K, V> other) {
        if (other != null)
            other.forEach(one::put);
    }

    public static boolean isNotEmpty(Collection<?> collection) {
        return collection != null && collection.size() > 0;
    }
}
