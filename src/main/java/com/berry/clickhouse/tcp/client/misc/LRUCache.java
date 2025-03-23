package com.berry.clickhouse.tcp.client.misc;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRUCache类实现了一个基于最近最少使用（LRU）策略的缓存
 * 
 * @param <K> 键类型
 * @param <V> 值类型
 */
public class LRUCache<K, V> {
    private static final float HASH_TABLE_LOAD_FACTOR = 0.75f; // 哈希表负载因子

    private final int cacheSize; // 缓存大小
    private final LinkedHashMap<K, V> map; // 存储缓存的映射

    /**
     * 构造函数，初始化LRUCache
     * 
     * @param cacheSize 缓存大小
     */
    public LRUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.map = new LinkedHashMap<K, V>(cacheSize, HASH_TABLE_LOAD_FACTOR, true) {
            public boolean removeEldestEntry(Map.Entry eldest) {
                return size() > LRUCache.this.cacheSize; // 超过缓存大小时移除最老的条目
            }
        };
    }

    /**
     * 获取指定键的值
     * 
     * @param key 键
     * @return 值
     */
    public synchronized V get(K key) {
        return map.get(key); // 返回指定键的值
    }

    /**
     * 添加键值对到缓存
     * 
     * @param key 键
     * @param value 值
     */
    public synchronized void put(K key, V value) {
        map.remove(key); // 移除旧值
        map.put(key, value); // 添加新值
    }

    /**
     * 如果键不存在则添加键值对到缓存
     * 
     * @param key 键
     * @param value 值
     */
    public synchronized void putIfAbsent(K key, V value) {
        map.putIfAbsent(key, value); // 如果键不存在则添加
    }

    /**
     * 清空缓存
     */
    public synchronized void clear() {
        map.clear(); // 清空缓存
    }

    /**
     * 获取当前缓存大小
     * 
     * @return 当前缓存大小
     */
    public synchronized int cacheSize() {
        return map.size(); // 返回当前缓存大小
    }
}
