package com.berry.clickhouse.tcp.client.misc;

/**
 * SystemUtil类提供了一些系统相关的工具方法
 */
public class SystemUtil {

    /**
     * 加载系统属性或环境变量
     * 
     * @param key 属性或环境变量的键
     * @param def 默认值
     * @return 返回属性值或默认值
     */
    public static String loadProp(String key, String def) {
        String property = System.getProperty(key);
        if (property != null)
            return property; // 返回系统属性
        String env = System.getenv(key);
        if (env != null)
            return env; // 返回环境变量
        return def; // 返回默认值
    }
}
