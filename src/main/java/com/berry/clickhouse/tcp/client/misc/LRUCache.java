package com.berry.clickhouse.tcp.client.misc;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> {
    private static final float HASH_TABLE_LOAD_FACTOR = 0.75f;

    private final int cacheSize;
    private final LinkedHashMap<K, V> map;

    public LRUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.map = new LinkedHashMap<K, V>(cacheSize, HASH_TABLE_LOAD_FACTOR, true) {
            public boolean removeEldestEntry(Map.Entry eldest) {
                return size() > LRUCache.this.cacheSize;
            }
        };
    }

    public synchronized V get(K key) {
        return map.get(key);
    }

    public synchronized void put(K key, V value) {
        map.remove(key);
        map.put(key, value);
    }

    public synchronized void putIfAbsent(K key, V value) {
        map.putIfAbsent(key, value);
    }

    public synchronized void clear() {
        map.clear();
    }

    public synchronized int cacheSize() {
        return map.size();
    }
}
