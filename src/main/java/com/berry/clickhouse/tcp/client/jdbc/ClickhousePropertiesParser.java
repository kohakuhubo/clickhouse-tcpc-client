package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;
import com.berry.clickhouse.tcp.client.settings.SettingKey;

import java.io.Serializable;
import java.util.*;

public class ClickhousePropertiesParser {

    public static final String HOST_DELIMITER = ",";
    public static final String PORT_DELIMITER = ":";

    private static final Logger LOG = LoggerFactory.getLogger(ClickhousePropertiesParser.class);

    public static Map<SettingKey, Serializable> parseProperties(Properties properties) {
        Map<SettingKey, Serializable> settings = new HashMap<>();

        for (String name : properties.stringPropertyNames()) {
            String value = properties.getProperty(name);

            parseSetting(settings, name, value);
        }

        return settings;
    }

    private static void parseSetting(Map<SettingKey, Serializable> settings, String name, String value) {
        SettingKey settingKey = SettingKey.definedSettingKeys().get(name.toLowerCase(Locale.ROOT));
        if (settingKey != null) {
            settings.put(settingKey, settingKey.type().deserializeURL(value));
        } else {
            LOG.warn("ignore undefined setting: {}={}", name, value);
        }
    }
}
