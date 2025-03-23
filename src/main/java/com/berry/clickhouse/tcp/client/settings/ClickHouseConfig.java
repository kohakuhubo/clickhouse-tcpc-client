package com.berry.clickhouse.tcp.client.settings;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;
import com.berry.clickhouse.tcp.client.buffer.DefaultBufferPoolManager;
import com.berry.clickhouse.tcp.client.data.ColumnWriterBufferPoolManager;
import com.berry.clickhouse.tcp.client.data.DefaultColumnWriterBufferPoolManager;
import com.berry.clickhouse.tcp.client.jdbc.ClickhousePropertiesParser;
import com.berry.clickhouse.tcp.client.misc.CollectionUtil;
import com.berry.clickhouse.tcp.client.misc.StrUtil;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;

import static com.berry.clickhouse.tcp.client.jdbc.ClickhousePropertiesParser.HOST_DELIMITER;

/**
 * ClickHouseConfig类用于配置ClickHouse连接的各种参数
 * 包括主机、端口、数据库、用户凭证等设置
 */
public class ClickHouseConfig implements Serializable {

    private final String host; // 主机名
    private final List<String> hosts; // 主机列表
    private final int port; // 端口号
    private final String database; // 数据库名
    private final String user; // 用户名
    private final String password; // 密码
    private final Duration queryTimeout; // 查询超时时间
    private final Duration connectTimeout; // 连接超时时间
    private final String charset; // 字符集
    private final Map<SettingKey, Serializable> settings; // 设置键值对
    private final boolean tcpKeepAlive; // TCP保持活动
    private final boolean ssl; // 是否启用SSL
    private final String sslMode; // SSL模式
    private final String clientName; // 客户端名称
    private final int connectionPoolMaxIdle; // 连接池最大空闲连接数
    private final int connectionPoolMinIdle; // 连接池最小空闲连接数
    private final int connectionPoolTotal; // 连接池总连接数
    private final String serializedIPv4; // 序列化的IPv4地址
    private final String serializedIPv6; // 序列化的IPv6地址
    private final ColumnWriterBufferPoolManager columnWriterBufferPoolManager; // 列写入缓冲池管理器
    private final BufferPoolManager bufferPoolManager; // 缓冲池管理器

    /**
     * ClickHouseConfig构造函数
     * 
     * @param host 主机名
     * @param port 端口号
     * @param database 数据库名
     * @param user 用户名
     * @param password 密码
     * @param queryTimeout 查询超时时间
     * @param connectTimeout 连接超时时间
     * @param tcpKeepAlive TCP保持活动
     * @param ssl 是否启用SSL
     * @param sslMode SSL模式
     * @param charset 字符集
     * @param clientName 客户端名称
     * @param settings 设置键值对
     * @param connectionPoolMaxIdle 连接池最大空闲连接数
     * @param connectionPoolMinIdle 连接池最小空闲连接数
     * @param connectionPoolTotal 连接池总连接数
     * @param serializedIPv4 序列化的IPv4地址
     * @param serializedIPv6 序列化的IPv6地址
     * @param columnWriterBufferPoolManager 列写入缓冲池管理器
     * @param bufferPoolManager 缓冲池管理器
     */
    private ClickHouseConfig(String host, int port, String database, String user, String password,
                             Duration queryTimeout, Duration connectTimeout, boolean tcpKeepAlive,
                             boolean ssl, String sslMode, String charset, String clientName,
                             Map<SettingKey, Serializable> settings,
                             int connectionPoolMaxIdle,
                             int connectionPoolMinIdle,
                             int connectionPoolTotal,
                             String serializedIPv4,
                             String serializedIPv6,
                             ColumnWriterBufferPoolManager columnWriterBufferPoolManager,
                             BufferPoolManager bufferPoolManager) {
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
        this.connectionPoolMaxIdle = connectionPoolMaxIdle;
        this.connectionPoolMinIdle = connectionPoolMinIdle;
        this.connectionPoolTotal = connectionPoolTotal;
        this.serializedIPv4 = serializedIPv4;
        this.serializedIPv6 = serializedIPv6;
        this.settings = settings;
        this.bufferPoolManager = bufferPoolManager;
        this.columnWriterBufferPoolManager = columnWriterBufferPoolManager;
    }

    // 各种getter方法
    public String host() {
        return this.host; // 返回主机名
    }

    public List<String> hosts() {
        return this.hosts; // 返回主机列表
    }

    public int port() {
        return this.port; // 返回端口号
    }

    public String database() {
        return this.database; // 返回数据库名
    }

    public String user() {
        return this.user; // 返回用户名
    }

    public String password() {
        return this.password; // 返回密码
    }

    public Duration queryTimeout() {
        return this.queryTimeout; // 返回查询超时时间
    }

    public Duration connectTimeout() {
        return this.connectTimeout; // 返回连接超时时间
    }

    public boolean ssl() {
        return this.ssl; // 返回是否启用SSL
    }

    public String sslMode() {
        return this.sslMode; // 返回SSL模式
    }

    public Charset charset() {
        return Charset.forName(charset); // 返回字符集
    }

    public String clientName() {
        return this.clientName; // 返回客户端名称
    }

    public Map<SettingKey, Serializable> settings() {
        return settings; // 返回设置键值对
    }

    // 各种配置方法
    public ClickHouseConfig withHostPort(String host, int port) {
        return Builder.builder(this)
                .host(host)
                .port(port)
                .build(); // 更新主机和端口
    }

    public ClickHouseConfig withDatabase(String database) {
        return Builder.builder(this)
                .database(database)
                .build(); // 更新数据库名
    }

    public ClickHouseConfig withCredentials(String user, String password) {
        return Builder.builder(this)
                .user(user)
                .password(password)
                .build(); // 更新用户名和密码
    }

    public ClickHouseConfig withQueryTimeout(Duration timeout) {
        return Builder.builder(this)
                .queryTimeout(timeout)
                .build(); // 更新查询超时时间
    }

    public ClickHouseConfig withTcpKeepAlive(boolean enable) {
        return Builder.builder(this)
                .tcpKeepAlive(enable)
                .build(); // 更新TCP保持活动设置
    }

    public ClickHouseConfig withSSL(boolean enable) {
        return Builder.builder(this)
                .ssl(enable)
                .build(); // 更新SSL设置
    }

    public ClickHouseConfig withSSLMode(String mode) {
        return Builder.builder(this)
                .sslMode(mode)
                .build(); // 更新SSL模式
    }

    public ClickHouseConfig withCharset(Charset charset) {
        return Builder.builder(this)
                .charset(charset)
                .build(); // 更新字符集
    }

    public ClickHouseConfig withClientName(String clientName) {
        return Builder.builder(this)
                .clientName(clientName)
                .build(); // 更新客户端名称
    }

    public ClickHouseConfig withSettings(Map<SettingKey, Serializable> settings) {
        return Builder.builder(this)
                .withSettings(settings)
                .build(); // 更新设置
    }

    public ClickHouseConfig withProperties(Properties properties) {
        return Builder.builder(this)
                .withProperties(properties)
                .build(); // 更新属性
    }

    public boolean tcpKeepAlive() {
        return tcpKeepAlive; // 返回TCP保持活动设置
    }

    // Builder类用于构建ClickHouseConfig实例
    public static final class Builder {
        private String host; // 主机名
        private int port; // 端口号
        private String database; // 数据库名
        private String user; // 用户名
        private String password; // 密码
        private Duration connectTimeout; // 连接超时时间
        private Duration queryTimeout; // 查询超时时间
        private boolean tcpKeepAlive; // TCP保持活动
        private boolean ssl; // 是否启用SSL
        private String sslMode; // SSL模式
        private Charset charset; // 字符集
        private String clientName; // 客户端名称
        private int selfColumStackLength; // 列堆栈长度
        private int selfByteBufferSize; // 字节缓冲区大小
        private int selfByteBufferLength; // 字节缓冲区长度
        private int connectionPoolMaxIdle; // 连接池最大空闲连接数
        private int connectionPooMinIdle; // 连接池最小空闲连接数
        private int connectionPoolTotal; // 连接池总连接数
        private String serializedIPv4; // 序列化的IPv4地址
        private String serializedIPv6; // 序列化的IPv6地址
        private Map<SettingKey, Serializable> settings = new HashMap<>(); // 设置键值对
        private BufferPoolManager bufferPoolManager; // 缓冲池管理器
        private ColumnWriterBufferPoolManager columnWriterBufferPoolManager; // 列写入缓冲池管理器

        private Builder() {
        }

        // 各种Builder方法
        public Builder serializedIPv6(String serializedIPv6) {
            this.serializedIPv6 = serializedIPv6; // 设置序列化的IPv6地址
            return this;
        }

        public Builder serializedIPv4(String serializedIPv4) {
            this.serializedIPv4 = serializedIPv4; // 设置序列化的IPv4地址
            return this;
        }

        public Builder connectionPoolTotal(int connectionPoolTotal) {
            this.connectionPoolTotal = connectionPoolTotal; // 设置连接池总连接数
            return this;
        }

        public Builder connectionPooMinIdle(int connectionPooMinIdle) {
            this.connectionPooMinIdle = connectionPooMinIdle; // 设置连接池最小空闲连接数
            return this;
        }

        public Builder connectionPoolMaxIdle(int connectionPoolMaxIdle) {
            this.connectionPoolMaxIdle = connectionPoolMaxIdle; // 设置连接池最大空闲连接数
            return this;
        }

        public Builder selfByteBufferLength(int selfByteBufferLength) {
            this.selfByteBufferLength = selfByteBufferLength; // 设置字节缓冲区长度
            return this;
        }

        public Builder selfByteBufferSize(int selfByteBufferSize) {
            this.selfByteBufferSize = selfByteBufferSize; // 设置字节缓冲区大小
            return this;
        }

        public Builder selfColumStackLength(int selfColumStackLength) {
            this.selfColumStackLength = selfColumStackLength; // 设置列堆栈长度
            return this;
        }

        public Builder bufferPoolManager(BufferPoolManager bufferPoolManager) {
            this.bufferPoolManager = bufferPoolManager; // 设置缓冲池管理器
            return this;
        }

        public Builder columnWriterBufferPoolManager(ColumnWriterBufferPoolManager columnWriterBufferPoolManager) {
            this.columnWriterBufferPoolManager = columnWriterBufferPoolManager; // 设置列写入缓冲池管理器
            return this;
        }

        public static Builder builder() {
            return new Builder(); // 创建新的Builder实例
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
                    .withSettings(cfg.settings()); // 从现有配置构建新的Builder
        }

        public Builder withSetting(SettingKey key, Serializable value) {
            this.settings.put(key, value); // 添加设置
            return this;
        }

        public Builder withSettings(Map<SettingKey, Serializable> settings) {
            this.settings.putAll(settings); // 添加多个设置
            return this;
        }

        public Builder host(String host) {
            this.withSetting(SettingKey.host, host); // 设置主机名
            return this;
        }

        public Builder port(int port) {
            this.withSetting(SettingKey.port, port); // 设置端口号
            return this;
        }

        public Builder database(String database) {
            this.withSetting(SettingKey.database, database); // 设置数据库名
            return this;
        }

        public Builder user(String user) {
            this.withSetting(SettingKey.user, user); // 设置用户名
            return this;
        }

        public Builder password(String password) {
            this.withSetting(SettingKey.password, password); // 设置密码
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.withSetting(SettingKey.connect_timeout, connectTimeout); // 设置连接超时时间
            return this;
        }

        public Builder queryTimeout(Duration queryTimeout) {
            this.withSetting(SettingKey.query_timeout, queryTimeout); // 设置查询超时时间
            return this;
        }

        public Builder tcpKeepAlive(boolean tcpKeepAlive) {
            this.withSetting(SettingKey.tcp_keep_alive, tcpKeepAlive); // 设置TCP保持活动
            return this;
        }

        public Builder ssl(boolean ssl) {
            this.withSetting(SettingKey.ssl, ssl); // 设置SSL
            return this;
        }

        public Builder sslMode(String sslMode) {
            this.withSetting(SettingKey.sslMode, sslMode); // 设置SSL模式
            return this;
        }

        public Builder charset(String charset) {
            this.withSetting(SettingKey.charset, charset); // 设置字符集
            return this;
        }

        public Builder charset(Charset charset) {
            this.withSetting(SettingKey.charset, charset.name()); // 设置字符集
            return this;
        }

        public Builder clientName(String clientName) {
            this.withSetting(SettingKey.client_name, clientName); // 设置客户端名称
            return this;
        }

        public Builder settings(Map<SettingKey, Serializable> settings) {
            this.settings = settings; // 设置键值对
            return this;
        }

        public Builder clearSettings() {
            this.settings.clear(); // 清空设置
            return this;
        }

        public Builder withProperties(Properties properties) {
            return this.withSettings(ClickhousePropertiesParser.parseProperties(properties)); // 从属性中设置
        }

        public ClickHouseConfig build() {
            // 构建ClickHouseConfig实例
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
            this.selfByteBufferSize = (this.selfByteBufferSize <= 0) ? 1024 * 1024 : this.selfByteBufferSize;
            this.bufferPoolManager = (null == bufferPoolManager) ? new DefaultBufferPoolManager(this.selfByteBufferSize) : bufferPoolManager;
            this.selfColumStackLength = (this.selfColumStackLength <= 0) ? 1024 : this.selfColumStackLength;
            this.selfByteBufferLength = (this.selfByteBufferLength <= 0) ? 1: this.selfByteBufferLength;
            this.columnWriterBufferPoolManager = (null == columnWriterBufferPoolManager) ? new DefaultColumnWriterBufferPoolManager(this.selfColumStackLength, this.selfByteBufferLength) : columnWriterBufferPoolManager;

            revisit();
            purgeSettings();

            return new ClickHouseConfig(host, port, database, user, password, queryTimeout, connectTimeout,
                    tcpKeepAlive, ssl, sslMode, charset.name(), clientName, settings,
                    connectionPoolMaxIdle, connectionPooMinIdle, connectionPoolTotal,
                    serializedIPv4, serializedIPv6, columnWriterBufferPoolManager, bufferPoolManager);
        }

        private void revisit() {
            // 检查并设置默认值
            if (StrUtil.isBlank(this.host)) this.host = "127.0.0.1";
            if (this.port == -1) this.port = 9000;
            if (StrUtil.isBlank(this.user)) this.user = "default";
            if (StrUtil.isBlank(this.password)) this.password = "";
            if (StrUtil.isBlank(this.database)) this.database = "default";
            if (this.queryTimeout.isNegative()) this.queryTimeout = Duration.ZERO;
            if (this.connectTimeout.isNegative()) this.connectTimeout = Duration.ZERO;
        }

        private void purgeSettings() {
            // 清除不必要的设置
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

    // 其他getter方法
    public int getConnectionPoolMaxIdle() {
        return connectionPoolMaxIdle; // 返回连接池最大空闲连接数
    }

    public int getConnectionPoolMinIdle() {
        return connectionPoolMinIdle; // 返回连接池最小空闲连接数
    }

    public int getConnectionPoolTotal() {
        return connectionPoolTotal; // 返回连接池总连接数
    }

    public String getSerializedIPv4() {
        return serializedIPv4; // 返回序列化的IPv4地址
    }

    public String getSerializedIPv6() {
        return serializedIPv6; // 返回序列化的IPv6地址
    }

    public ColumnWriterBufferPoolManager getColumnWriterBufferPoolManager() {
        return columnWriterBufferPoolManager; // 返回列写入缓冲池管理器
    }

    public BufferPoolManager getBufferPoolManager() {
        return bufferPoolManager; // 返回缓冲池管理器
    }
}
