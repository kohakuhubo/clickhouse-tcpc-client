package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.NativeClient;
import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.SessionState;
import com.berry.clickhouse.tcp.client.data.Block;
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

public class ClickHouseConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnection.class);
    private static final Pattern VALUES_REGEX = Pattern.compile("[Vv][Aa][Ll][Uu][Ee][Ss]\\s*\\(");

    private final AtomicBoolean isClosed;
    private final AtomicReference<ClickHouseConfig> cfg;
    private final AtomicReference<SessionState> state = new AtomicReference<>(SessionState.IDLE);
    private volatile NativeContext nativeCtx;

    protected ClickHouseConnection(ClickHouseConfig cfg, NativeContext nativeCtx) {
        this.isClosed = new AtomicBoolean(false);
        this.cfg = new AtomicReference<>(cfg);
        this.nativeCtx = nativeCtx;
    }

    public ClickHouseConfig cfg() {
        return cfg.get();
    }

    public NativeContext.ServerContext serverContext() {
        return nativeCtx.serverCtx();
    }

    public NativeContext.ClientContext clientContext() {
        return nativeCtx.clientCtx();
    }

    public void close() throws SQLException {
        if (!isClosed() && isClosed.compareAndSet(false, true)) {
            NativeClient nativeClient = nativeCtx.nativeClient();
            nativeClient.disconnect();
        }
    }

    public boolean isClosed() throws SQLException {
        return isClosed.get();
    }

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

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        Properties properties = new Properties();
        properties.put(name, value);
        this.setClientInfo(properties);
    }

    public boolean isValid(int timeout) throws SQLException {
        return getNativeClient().ping(Duration.ofSeconds(timeout), nativeCtx.serverCtx());
    }

    public boolean ping(Duration timeout) throws SQLException {
        return nativeCtx.nativeClient().ping(timeout, nativeCtx.serverCtx());
    }

    public Block getSampleBlock(final String insertQuery) throws SQLException {
        NativeClient nativeClient = getHealthyNativeClient();
        nativeClient.sendQuery(insertQuery, nativeCtx.clientCtx(), cfg.get().settings());
        Validate.isTrue(this.state.compareAndSet(SessionState.IDLE, SessionState.WAITING_INSERT),
                "Connection is currently waiting for an insert operation, check your previous InsertStatement.");
        return nativeClient.receiveSampleBlock(cfg.get().queryTimeout(), nativeCtx.serverCtx());
    }

    public QueryResult sendQueryRequest(final String query, ClickHouseConfig cfg, boolean lazy, boolean serialize) throws SQLException {
        Validate.isTrue(this.state.get() == SessionState.IDLE,
                "Connection is currently waiting for an insert operation, check your previous InsertStatement.");
        NativeClient nativeClient = getHealthyNativeClient();
        nativeClient.sendQuery(query, nativeCtx.clientCtx(), cfg.settings());
        return nativeClient.receiveQuery(cfg.queryTimeout(), nativeCtx.serverCtx(), lazy, serialize);
    }
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

    private NativeClient getNativeClient() {
        return nativeCtx.nativeClient();
    }

    public static ClickHouseConnection createClickHouseConnection(ClickHouseConfig configure) throws SQLException {
        return new ClickHouseConnection(configure, createNativeContext(configure));
    }

    private static NativeContext createNativeContext(ClickHouseConfig configure) throws SQLException {
        if (configure.hosts().size() == 1) {
            NativeClient nativeClient = NativeClient.connect(configure);
            return new NativeContext(clientContext(nativeClient, configure), serverContext(nativeClient, configure), nativeClient);
        }

        return createFailoverNativeContext(configure);
    }

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

    private static NativeContext.ClientContext clientContext(NativeClient nativeClient, ClickHouseConfig configure) throws SQLException {
        Validate.isTrue(nativeClient.address() instanceof InetSocketAddress);
        InetSocketAddress address = (InetSocketAddress) nativeClient.address();
        String clientName = configure.clientName();
        String initialAddress = "[::ffff:127.0.0.1]:0";
        return new NativeContext.ClientContext(initialAddress, address.getHostName(), clientName);
    }

    private static NativeContext.ServerContext serverContext(NativeClient nativeClient, ClickHouseConfig configure) throws SQLException {
        try {
            long revision = ClickHouseDefines.CLIENT_REVISION;
            nativeClient.sendHello("client", revision, configure.database(), configure.user(), configure.password());

            HelloResponse response = nativeClient.receiveHello(configure.queryTimeout(), null);
            ZoneId timeZone = ZoneId.of(response.serverTimeZone());
            return new NativeContext.ServerContext(
                    response.majorVersion(), response.minorVersion(), response.reversion(),
                    configure, timeZone, response.serverDisplayName());
        } catch (SQLException rethrows) {
            nativeClient.silentDisconnect();
            throw rethrows;
        }
    }
}
