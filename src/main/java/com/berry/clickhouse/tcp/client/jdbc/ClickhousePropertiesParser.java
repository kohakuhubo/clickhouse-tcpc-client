package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;
import com.berry.clickhouse.tcp.client.settings.SettingKey;

import java.io.Serializable;
import java.util.*;

/**
 * ClickhousePropertiesParser类用于解析ClickHouse连接属性
 * 将Properties对象转换为SettingKey和Serializable的映射
 */
public class ClickhousePropertiesParser {

    public static final String HOST_DELIMITER = ","; // 主机分隔符
    public static final String PORT_DELIMITER = ":"; // 端口分隔符

    private static final Logger LOG = LoggerFactory.getLogger(ClickhousePropertiesParser.class);

    /**
     * 解析Properties对象，返回SettingKey和Serializable的映射
     * 
     * @param properties Properties对象
     * @return 解析后的设置映射
     */
    public static Map<SettingKey, Serializable> parseProperties(Properties properties) {
        Map<SettingKey, Serializable> settings = new HashMap<>();

        for (String name : properties.stringPropertyNames()) {
            String value = properties.getProperty(name);
            parseSetting(settings, name, value); // 解析每个设置
        }

        return settings;
    }

    /**
     * 解析单个设置
     * 
     * @param settings 设置映射
     * @param name 设置名称
     * @param value 设置值
     */
    private static void parseSetting(Map<SettingKey, Serializable> settings, String name, String value) {
        SettingKey settingKey = SettingKey.definedSettingKeys().get(name.toLowerCase(Locale.ROOT));
        if (settingKey != null) {
            settings.put(settingKey, settingKey.type().deserializeURL(value)); // 将设置添加到映射中
        } else {
            LOG.warn("ignore undefined setting: {}={}", name, value); // 记录未定义的设置
        }
    }
}
