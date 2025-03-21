package com.berry.clickhouse.tcp.client.settings;

import com.berry.clickhouse.tcp.client.jdbc.ClickhousePropertiesParser;
import com.berry.clickhouse.tcp.client.misc.CollectionUtil;
import com.berry.clickhouse.tcp.client.misc.StrUtil;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;

import static com.berry.clickhouse.tcp.client.jdbc.ClickhousePropertiesParser.HOST_DELIMITER;

public class ClickHouseConfig implements Serializable {

    private final String host;
    private final List<String> hosts;
    private final int port;
    private final String database;
    private final String user;
    private final String password;
    private final Duration queryTimeout;
    private final Duration connectTimeout;
    private final String charset;
    private final Map<SettingKey, Serializable> settings;
    private final boolean tcpKeepAlive;
    private final boolean ssl;
    private final String sslMode;
    private final String clientName;
    private final int selfColumStackLength;
    private final int systemColumStackLength;
    private final int selfByteBufferSize;
    private final int selfByteBufferLength;
    private final int systemByteBufferSize;
    private final int systemByteBufferLength;
    private final int systemByteBufferStackLength;
    private final int connectionPoolMaxIdle;
    private final int connectionPoolMinIdle;
    private final int connectionPoolTotal;
    private final String serializedIPv4;
    private final String serializedIPv6;

    private ClickHouseConfig(String host, int port, String database, String user, String password,
                             Duration queryTimeout, Duration connectTimeout, boolean tcpKeepAlive,
                             boolean ssl, String sslMode, String charset, String clientName,
                             Map<SettingKey, Serializable> settings,
                             int selfColumStackLength,
                             int systemColumStackLength,
                             int selfByteBufferSize,
                             int selfByteBufferLength,
                             int systemByteBufferSize,
                             int systemByteBufferLength,
                             int systemByteBufferStackLength,
                             int connectionPoolMaxIdle,
                             int connectionPoolMinIdle,
                             int connectionPoolTotal,
                             String serializedIPv4,
                             String serializedIPv6) {
        this.host = host;
        this.hosts = Arrays.asList(host.split(HOST_DELIMITER));
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
        this.queryTimeout = queryTimeout;
        this.connectTimeout = connectTimeout;
        this.tcpKeepAlive = tcpKeepAlive;
        this.ssl = ssl;
        this.sslMode = sslMode;
        this.charset = charset;
        this.clientName = clientName;
        this.selfColumStackLength = selfColumStackLength;
        this.systemColumStackLength = systemColumStackLength;
        this.selfByteBufferSize = selfByteBufferSize;
        this.selfByteBufferLength = selfByteBufferLength;
        this.systemByteBufferSize = systemByteBufferSize;
        this.systemByteBufferLength = systemByteBufferLength;
        this.systemByteBufferStackLength = systemByteBufferStackLength;
        this.connectionPoolMaxIdle = connectionPoolMaxIdle;
        this.connectionPoolMinIdle = connectionPoolMinIdle;
        this.connectionPoolTotal = connectionPoolTotal;
        this.serializedIPv4 = serializedIPv4;
        this.serializedIPv6 = serializedIPv6;
        this.settings = settings;
    }

    public String host() {
        return this.host;
    }

    public List<String> hosts() {
        return this.hosts;
    }

    public int port() {
        return this.port;
    }

    public String database() {
        return this.database;
    }

    public String user() {
        return this.user;
    }

    public String password() {
        return this.password;
    }

    public Duration queryTimeout() {
        return this.queryTimeout;
    }

    public Duration connectTimeout() {
        return this.connectTimeout;
    }

    public boolean ssl() {
        return this.ssl;
    }

    public String sslMode() {
        return this.sslMode;
    }

    public Charset charset() {
        return Charset.forName(charset);
    }

    public String clientName() {
        return this.clientName;
    }

    public Map<SettingKey, Serializable> settings() {
        return settings;
    }

    public ClickHouseConfig withHostPort(String host, int port) {
        return Builder.builder(this)
                .host(host)
                .port(port)
                .build();
    }

    public ClickHouseConfig withDatabase(String database) {
        return Builder.builder(this)
                .database(database)
                .build();
    }

    public ClickHouseConfig withCredentials(String user, String password) {
        return Builder.builder(this)
                .user(user)
                .password(password)
                .build();
    }

    public ClickHouseConfig withQueryTimeout(Duration timeout) {
        return Builder.builder(this)
                .queryTimeout(timeout)
                .build();
    }

    public ClickHouseConfig withTcpKeepAlive(boolean enable) {
        return Builder.builder(this)
                .tcpKeepAlive(enable)
                .build();
    }

    public ClickHouseConfig withSSL(boolean enable) {
        return Builder.builder(this)
                .ssl(enable)
                .build();
    }

    public ClickHouseConfig withSSLMode(String mode) {
        return Builder.builder(this)
                .sslMode(mode)
                .build();
    }

    public ClickHouseConfig withCharset(Charset charset) {
        return Builder.builder(this)
                .charset(charset)
                .build();
    }

    public ClickHouseConfig withClientName(String clientName) {
        return Builder.builder(this)
                .clientName(clientName)
                .build();
    }

    public ClickHouseConfig withSettings(Map<SettingKey, Serializable> settings) {
        return Builder.builder(this)
                .withSettings(settings)
                .build();
    }

    public ClickHouseConfig withProperties(Properties properties) {
        return Builder.builder(this)
                .withProperties(properties)
                .build();
    }

    public boolean tcpKeepAlive() {
        return tcpKeepAlive;
    }

    public static final class Builder {
        private String host;
        private int port;
        private String database;
        private String user;
        private String password;
        private Duration connectTimeout;
        private Duration queryTimeout;
        private boolean tcpKeepAlive;
        private boolean ssl;
        private String sslMode;
        private Charset charset;
        private String clientName;
        private int selfColumStackLength;
        private int systemColumStackLength;
        private int selfByteBufferSize;
        private int selfByteBufferLength;
        private int systemByteBufferSize;
        private int systemByteBufferLength;
        private int systemByteBufferStackLength;
        private int connectionPoolMaxIdle;
        private int connectionPooMinIdle;
        private int connectionPoolTotal;
        private String serializedIPv4;
        private String serializedIPv6;
        private Map<SettingKey, Serializable> settings = new HashMap<>();

        private Builder() {
        }

        public Builder serializedIPv6(String serializedIPv6) {
            this.serializedIPv6 = serializedIPv6;
            return this;
        }

        public Builder serializedIPv4(String serializedIPv4) {
            this.serializedIPv4 = serializedIPv4;
            return this;
        }

        public Builder connectionPoolTotal(int connectionPoolTotal) {
            this.connectionPoolTotal = connectionPoolTotal;
            return this;
        }

        public Builder connectionPooMinIdle(int connectionPooMinIdle) {
            this.connectionPooMinIdle = connectionPooMinIdle;
            return this;
        }

        public Builder connectionPoolMaxIdle(int connectionPoolMaxIdle) {
            this.connectionPoolMaxIdle = connectionPoolMaxIdle;
            return this;
        }

        public Builder systemByteBufferStackLength(int systemByteBufferStackLength) {
            this.systemByteBufferStackLength = systemByteBufferStackLength;
            return this;
        }

        public Builder systemByteBufferLength(int systemByteBufferLength) {
            this.systemByteBufferLength = systemByteBufferLength;
            return this;
        }

        public Builder systemByteBufferSize(int systemByteBufferSize) {
            this.systemByteBufferSize = systemByteBufferSize;
            return this;
        }

        public Builder selfByteBufferLength(int selfByteBufferLength) {
            this.selfByteBufferLength = selfByteBufferLength;
            return this;
        }

        public Builder selfByteBufferSize(int selfByteBufferSize) {
            this.selfByteBufferSize = selfByteBufferSize;
            return this;
        }

        public Builder systemColumStackLength(int systemColumStackLength) {
            this.systemColumStackLength = systemColumStackLength;
            return this;
        }

        public Builder selfColumStackLength(int selfColumStackLength) {
            this.selfColumStackLength = selfColumStackLength;
            return this;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static Builder builder(ClickHouseConfig cfg) {
            return new Builder()
                    .host(cfg.host())
                    .port(cfg.port())
                    .database(cfg.database())
                    .user(cfg.user())
                    .password(cfg.password())
                    .connectTimeout(cfg.connectTimeout())
                    .queryTimeout(cfg.queryTimeout())
                    .charset(cfg.charset())
                    .tcpKeepAlive(cfg.tcpKeepAlive())
                    .ssl(cfg.ssl())
                    .sslMode(cfg.sslMode())
                    .clientName(cfg.clientName())
                    .withSettings(cfg.settings());
        }

        public Builder withSetting(SettingKey key, Serializable value) {
            this.settings.put(key, value);
            return this;
        }

        public Builder withSettings(Map<SettingKey, Serializable> settings) {
            CollectionUtil.mergeMapInPlaceKeepLast(this.settings, settings);
            return this;
        }

        public Builder host(String host) {
            this.withSetting(SettingKey.host, host);
            return this;
        }

        public Builder port(int port) {
            this.withSetting(SettingKey.port, port);
            return this;
        }

        public Builder database(String database) {
            this.withSetting(SettingKey.database, database);
            return this;
        }

        public Builder user(String user) {
            this.withSetting(SettingKey.user, user);
            return this;
        }

        public Builder password(String password) {
            this.withSetting(SettingKey.password, password);
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.withSetting(SettingKey.connect_timeout, connectTimeout);
            return this;
        }

        public Builder queryTimeout(Duration queryTimeout) {
            this.withSetting(SettingKey.query_timeout, queryTimeout);
            return this;
        }

        public Builder tcpKeepAlive(boolean tcpKeepAlive) {
            this.withSetting(SettingKey.tcp_keep_alive, tcpKeepAlive);
            return this;
        }

        public Builder ssl(boolean ssl) {
            this.withSetting(SettingKey.ssl, ssl);
            return this;
        }

        public Builder sslMode(String sslMode) {
            this.withSetting(SettingKey.ssl, ssl);
            return this;
        }

        public Builder charset(String charset) {
            this.withSetting(SettingKey.charset, charset);
            return this;
        }

        public Builder charset(Charset charset) {
            this.withSetting(SettingKey.charset, charset.name());
            return this;
        }

        public Builder clientName(String clientName) {
            this.withSetting(SettingKey.client_name, clientName);
            return this;
        }

        public Builder settings(Map<SettingKey, Serializable> settings) {
            this.settings = settings;
            return this;
        }

        public Builder clearSettings() {
            this.settings = new HashMap<>();
            return this;
        }

        public Builder withProperties(Properties properties) {
            return this.withSettings(ClickhousePropertiesParser.parseProperties(properties));
        }

        public ClickHouseConfig build() {
            this.host = (String) this.settings.getOrDefault(SettingKey.host, "127.0.0.1");
            this.port = ((Number) this.settings.getOrDefault(SettingKey.port, 9000)).intValue();
            this.user = (String) this.settings.getOrDefault(SettingKey.user, "default");
            this.password = (String) this.settings.getOrDefault(SettingKey.password, "");
            this.database = (String) this.settings.getOrDefault(SettingKey.database, "default");
            this.connectTimeout = (Duration) this.settings.getOrDefault(SettingKey.connect_timeout, Duration.ZERO);
            this.queryTimeout = (Duration) this.settings.getOrDefault(SettingKey.query_timeout, Duration.ZERO);
            this.tcpKeepAlive = (boolean) this.settings.getOrDefault(SettingKey.tcp_keep_alive, false);
            this.ssl = (boolean) this.settings.getOrDefault(SettingKey.ssl, false);
            this.sslMode = (String) this.settings.getOrDefault(SettingKey.sslMode, "disabled");
            this.charset = Charset.forName((String) this.settings.getOrDefault(SettingKey.charset, "UTF-8"));
            this.clientName = (String) this.settings.getOrDefault(SettingKey.client_name,
                    String.format(Locale.ROOT, "%s %s", ClickHouseDefines.NAME, "client"));

            revisit();
            purgeSettings();

            return new ClickHouseConfig(host, port, database, user, password, queryTimeout, connectTimeout,
                    tcpKeepAlive, ssl, sslMode, charset.name(), clientName, settings,
                    selfColumStackLength, systemColumStackLength,
                    selfByteBufferSize, selfByteBufferLength,
                    systemByteBufferSize, systemByteBufferLength,
                    systemByteBufferStackLength,
                    connectionPoolMaxIdle, connectionPooMinIdle, connectionPoolTotal,
                    serializedIPv4, serializedIPv6);
        }

        private void revisit() {
            if (StrUtil.isBlank(this.host)) this.host = "127.0.0.1";
            if (this.port == -1) this.port = 9000;
            if (StrUtil.isBlank(this.user)) this.user = "default";
            if (StrUtil.isBlank(this.password)) this.password = "";
            if (StrUtil.isBlank(this.database)) this.database = "default";
            if (this.queryTimeout.isNegative()) this.queryTimeout = Duration.ZERO;
            if (this.connectTimeout.isNegative()) this.connectTimeout = Duration.ZERO;
        }

        private void purgeSettings() {
            this.settings.remove(SettingKey.port);
            this.settings.remove(SettingKey.host);
            this.settings.remove(SettingKey.password);
            this.settings.remove(SettingKey.user);
            this.settings.remove(SettingKey.database);
            this.settings.remove(SettingKey.query_timeout);
            this.settings.remove(SettingKey.connect_timeout);
            this.settings.remove(SettingKey.tcp_keep_alive);
            this.settings.remove(SettingKey.ssl);
            this.settings.remove(SettingKey.sslMode);
            this.settings.remove(SettingKey.charset);
            this.settings.remove(SettingKey.client_name);
        }
    }

    public int getSystemByteBufferLength() {
        return systemByteBufferLength;
    }

    public int getSystemByteBufferSize() {
        return systemByteBufferSize;
    }

    public int getSelfByteBufferLength() {
        return selfByteBufferLength;
    }

    public int getSelfByteBufferSize() {
        return selfByteBufferSize;
    }

    public int getSystemColumStackLength() {
        return systemColumStackLength;
    }

    public int getSelfColumStackLength() {
        return selfColumStackLength;
    }

    public int getSystemByteBufferStackLength() {
        return systemByteBufferStackLength;
    }

    public int getConnectionPoolMaxIdle() {
        return connectionPoolMaxIdle;
    }

    public int getConnectionPoolMinIdle() {
        return connectionPoolMinIdle;
    }

    public int getConnectionPoolTotal() {
        return connectionPoolTotal;
    }

    public String getSerializedIPv4() {
        return serializedIPv4;
    }

    public String getSerializedIPv6() {
        return serializedIPv6;
    }
}
