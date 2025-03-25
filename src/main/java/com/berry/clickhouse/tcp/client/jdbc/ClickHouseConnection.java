package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.NativeClient;
import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.SessionState;
import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.data.ColumnWriterBufferFactory;
import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.protocol.HelloResponse;
import com.berry.clickhouse.tcp.client.settings.ClickHouseConfig;
import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;
import com.berry.clickhouse.tcp.client.stream.QueryResult;

import java.net.InetSocketAddress;
import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static com.berry.clickhouse.tcp.client.jdbc.ClickhousePropertiesParser.PORT_DELIMITER;

/**
 * ClickHouseConnection类表示与ClickHouse数据库的连接
 * 提供执行查询、插入数据和管理连接状态的功能
 */
public class ClickHouseConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnection.class);
    private static final Pattern VALUES_REGEX = Pattern.compile("[Vv][Aa][Ll][Uu][Ee][Ss]\\s*\\(");

    private final AtomicBoolean isClosed; // 连接是否关闭的状态
    private final AtomicReference<ClickHouseConfig> cfg; // 连接配置
    private final AtomicReference<SessionState> state = new AtomicReference<>(SessionState.IDLE); // 当前会话状态
    private volatile NativeContext nativeCtx; // 原生上下文

    /**
     * 构造函数，初始化ClickHouseConnection实例
     * 
     * @param cfg 连接配置
     * @param nativeCtx 原生上下文
     */
    protected ClickHouseConnection(ClickHouseConfig cfg, NativeContext nativeCtx) {
        this.isClosed = new AtomicBoolean(false);
        this.cfg = new AtomicReference<>(cfg);
        this.nativeCtx = nativeCtx;
    }

    /**
     * 获取连接配置
     * 
     * @return ClickHouseConfig对象
     */
    public ClickHouseConfig cfg() {
        return cfg.get();
    }

    /**
     * 获取服务器上下文
     * 
     * @return 服务器上下文
     */
    public NativeContext.ServerContext serverContext() {
        return nativeCtx.serverCtx();
    }

    /**
     * 获取客户端上下文
     * 
     * @return 客户端上下文
     */
    public NativeContext.ClientContext clientContext() {
        return nativeCtx.clientCtx();
    }

    /**
     * 关闭连接
     * 
     * @throws SQLException 如果关闭连接时发生错误
     */
    public void close() throws SQLException {
        if (!isClosed() && isClosed.compareAndSet(false, true)) {
            NativeClient nativeClient = nativeCtx.nativeClient();
            nativeClient.disconnect(); // 断开与ClickHouse的连接
        }
    }

    /**
     * 检查连接是否已关闭
     * 
     * @return true如果连接已关闭，false否则
     * @throws SQLException 如果检查状态时发生错误
     */
    public boolean isClosed() throws SQLException {
        return isClosed.get();
    }

    /**
     * 设置客户端信息
     * 
     * @param properties 客户端信息属性
     * @throws SQLClientInfoException 如果设置客户端信息时发生错误
     */
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            cfg.set(ClickHouseConfig.Builder.builder(cfg.get()).withProperties(properties).build());
        } catch (Exception ex) {
            Map<String, ClientInfoStatus> failed = new HashMap<>();
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                failed.put((String) entry.getKey(), ClientInfoStatus.REASON_UNKNOWN);
            }
            throw new SQLClientInfoException(failed, ex);
        }
    }

    /**
     * 设置单个客户端信息
     * 
     * @param name 属性名称
     * @param value 属性值
     * @throws SQLClientInfoException 如果设置客户端信息时发生错误
     */
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        Properties properties = new Properties();
        properties.put(name, value);
        this.setClientInfo(properties);
    }

    /**
     * 检查连接是否有效
     * 
     * @param timeout 超时时间
     * @return true如果连接有效，false否则
     * @throws SQLException 如果检查连接时发生错误
     */
    public boolean isValid(int timeout) throws SQLException {
        return getNativeClient().ping(Duration.ofSeconds(timeout), nativeCtx.serverCtx());
    }

    /**
     * 发送ping请求以检查连接
     * 
     * @param timeout 超时时间
     * @return true如果ping成功，false否则
     * @throws SQLException 如果发送ping请求时发生错误
     */
    public boolean ping(Duration timeout) throws SQLException {
        return nativeCtx.nativeClient().ping(timeout, nativeCtx.serverCtx());
    }

    /**
     * 获取样本数据块
     * 
     * @param insertQuery 插入查询
     * @return 样本数据块
     * @throws SQLException 如果获取样本数据块时发生错误
     */
    public Block getSampleBlock(final String insertQuery) throws SQLException {
        NativeClient nativeClient = getHealthyNativeClient();
        nativeClient.sendQuery(insertQuery, nativeCtx.clientCtx(), cfg.get().settings());
        Validate.isTrue(this.state.compareAndSet(SessionState.IDLE, SessionState.WAITING_INSERT),
                "Connection is currently waiting for an insert operation, check your previous InsertStatement.");
        return nativeClient.receiveSampleBlock(cfg.get().queryTimeout(), nativeCtx.serverCtx());
    }

    /**
     * 发送查询请求
     * 
     * @param query SQL查询
     * @param cfg 连接配置
     * @param lazy 是否懒加载
     * @param serialize 是否序列化
     * @return 查询结果
     * @throws SQLException 如果发送查询请求时发生错误
     */
    public QueryResult sendQueryRequest(final String query, ClickHouseConfig cfg, boolean lazy, boolean serialize) throws SQLException {
        Validate.isTrue(this.state.get() == SessionState.IDLE,
                "Connection is currently waiting for an insert operation, check your previous InsertStatement.");
        NativeClient nativeClient = getHealthyNativeClient();
        nativeClient.sendQuery(query, nativeCtx.clientCtx(), cfg.settings());
        return nativeClient.receiveQuery(cfg.queryTimeout(), nativeCtx.serverCtx(), lazy, serialize);
    }

    /**
     * 发送插入请求
     * 
     * @param block 数据块
     * @return 插入的行数
     * @throws SQLException 如果发送插入请求时发生错误
     */
    public int sendInsertRequest(Block block) throws SQLException {
        Validate.isTrue(this.state.get() == SessionState.WAITING_INSERT, "Call getSampleBlock before insert.");
        try {
            NativeClient nativeClient = getNativeClient();
            nativeClient.sendData(block);
            nativeClient.sendData(new Block());
            nativeClient.receiveEndOfStream(cfg.get().queryTimeout(), nativeCtx.serverCtx());
        } finally {
            Validate.isTrue(this.state.compareAndSet(SessionState.WAITING_INSERT, SessionState.IDLE));
        }
        return block.rowCnt();
    }

    /**
     * 获取健康的NativeClient实例
     * 
     * @return NativeClient实例
     * @throws SQLException 如果获取NativeClient时发生错误
     */
    private synchronized NativeClient getHealthyNativeClient() throws SQLException {
        NativeContext oldCtx = nativeCtx;
        if (!oldCtx.nativeClient().ping(cfg.get().queryTimeout(), nativeCtx.serverCtx())) {
            LOG.warn("connection loss with state[{}], create new connection and reset state", state);
            nativeCtx = createNativeContext(cfg.get());
            state.set(SessionState.IDLE);
            oldCtx.nativeClient().silentDisconnect();
        }

        return nativeCtx.nativeClient();
    }

    /**
     * 获取NativeClient实例
     * 
     * @return NativeClient实例
     */
    private NativeClient getNativeClient() {
        return nativeCtx.nativeClient();
    }

    /**
     * 创建ClickHouseConnection实例
     * 
     * @param configure 连接配置
     * @return ClickHouseConnection实例
     * @throws SQLException 如果创建连接时发生错误
     */
    public static ClickHouseConnection createClickHouseConnection(ClickHouseConfig configure) throws SQLException {
        return new ClickHouseConnection(configure, createNativeContext(configure));
    }

    /**
     * 创建原生上下文
     * 
     * @param configure 连接配置
     * @return NativeContext实例
     * @throws SQLException 如果创建上下文时发生错误
     */
    private static NativeContext createNativeContext(ClickHouseConfig configure) throws SQLException {
        if (configure.hosts().size() == 1) {
            NativeClient nativeClient = NativeClient.connect(configure);
            return new NativeContext(clientContext(nativeClient, configure), serverContext(nativeClient, configure), nativeClient);
        }

        return createFailoverNativeContext(configure);
    }

    /**
     * 创建故障转移的原生上下文
     * 
     * @param configure 连接配置
     * @return NativeContext实例
     * @throws SQLException 如果创建上下文时发生错误
     */
    private static NativeContext createFailoverNativeContext(ClickHouseConfig configure) throws SQLException {
        NativeClient nativeClient = null;
        SQLException lastException = null;

        int tryIndex = 0;
        do {
            String hostAndPort = configure.hosts().get(tryIndex);
            String[] hostAndPortSplit = hostAndPort.split(PORT_DELIMITER, 2);
            String host = hostAndPortSplit[0];
            int port;

            if (hostAndPortSplit.length == 2) {
                port = Integer.parseInt(hostAndPortSplit[1]);
            } else {
                port = configure.port();
            }

            try {
                nativeClient = NativeClient.connect(host, port, configure);
            } catch (SQLException e) {
                lastException = e;
            }
            tryIndex++;
        } while (nativeClient == null && tryIndex < configure.hosts().size());

        if (nativeClient == null) {
            throw lastException;
        }

        return new NativeContext(clientContext(nativeClient, configure), serverContext(nativeClient, configure), nativeClient);
    }

    /**
     * 创建客户端上下文
     * 
     * @param nativeClient NativeClient实例
     * @param configure 连接配置
     * @return ClientContext实例
     * @throws SQLException 如果创建上下文时发生错误
     */
    private static NativeContext.ClientContext clientContext(NativeClient nativeClient, ClickHouseConfig configure) throws SQLException {
        Validate.isTrue(nativeClient.address() instanceof InetSocketAddress);
        InetSocketAddress address = (InetSocketAddress) nativeClient.address();
        String clientName = configure.clientName();
        String initialAddress = "[::ffff:127.0.0.1]:0";
        return new NativeContext.ClientContext(initialAddress, address.getHostName(), clientName);
    }

    /**
     * 创建服务器上下文
     * 
     * @param nativeClient NativeClient实例
     * @param configure 连接配置
     * @return ServerContext实例
     * @throws SQLException 如果创建上下文时发生错误
     */
    private static NativeContext.ServerContext serverContext(NativeClient nativeClient, ClickHouseConfig configure) throws SQLException {
        try {
            long revision = ClickHouseDefines.CLIENT_REVISION;
            nativeClient.sendHello("client", revision, configure.database(), configure.user(), configure.password());

            HelloResponse response = nativeClient.receiveHello(configure.queryTimeout(), null);
            ZoneId timeZone = ZoneId.of(response.serverTimeZone());
            return new NativeContext.ServerContext(
                    response.majorVersion(), response.minorVersion(), response.reversion(),
                    configure, timeZone, response.serverDisplayName(), ColumnWriterBufferFactory.getInstance(configure));
        } catch (SQLException rethrows) {
            nativeClient.silentDisconnect();
            throw rethrows;
        }
    }
}
