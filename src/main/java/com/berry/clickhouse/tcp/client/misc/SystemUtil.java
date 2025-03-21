package com.berry.clickhouse.tcp.client.misc;

public class SystemUtil {

    public static String loadProp(String key, String def) {
        String property = System.getProperty(key);
        if (property != null)
            return property;
        String env = System.getenv(key);
        if (env != null)
            return env;
        return def;
    }
}
