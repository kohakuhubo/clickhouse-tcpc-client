package com.berry.clickhouse.tcp.client;

import com.berry.clickhouse.tcp.client.buffer.SocketBuffedReader;
import com.berry.clickhouse.tcp.client.buffer.SocketBuffedWriter;
import com.berry.clickhouse.tcp.client.ssl.SSLContextBuilder;
import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.protocol.*;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;
import com.berry.clickhouse.tcp.client.settings.ClickHouseConfig;
import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;
import com.berry.clickhouse.tcp.client.settings.SettingKey;
import com.berry.clickhouse.tcp.client.stream.ClickHouseQueryResult;
import com.berry.clickhouse.tcp.client.stream.QueryResult;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NativeClient {

    private static final Logger LOG = LoggerFactory.getLogger(NativeClient.class);

    public static NativeClient connect(ClickHouseConfig config) throws SQLException {
        return connect(config.host(), config.port(), config);
    }

    public static NativeClient connect(String host, int port, ClickHouseConfig config) throws SQLException {
        try {
            SocketAddress endpoint = new InetSocketAddress(host, port);
            Socket socket;

            boolean useSSL = config.ssl();
            if (useSSL) {
                LOG.debug("Client works in SSL mode!");
                SSLContext context = new SSLContextBuilder(config).getSSLContext();
                SSLSocketFactory factory = context.getSocketFactory();
                socket = (SSLSocket) factory.createSocket();
            } else {
                socket = new Socket();
            }
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(ClickHouseDefines.SOCKET_SEND_BUFFER_BYTES);
            socket.setReceiveBufferSize(ClickHouseDefines.SOCKET_RECV_BUFFER_BYTES);
            socket.setKeepAlive(config.tcpKeepAlive());
            socket.connect(endpoint, (int) config.connectTimeout().toMillis());

            if (useSSL) ((SSLSocket) socket).startHandshake();

            return new NativeClient(socket);
        } catch (IOException |
                 NoSuchAlgorithmException |
                 KeyStoreException |
                 CertificateException |
                 UnrecoverableKeyException |
                 KeyManagementException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    private final Socket socket;
    private final SocketAddress address;
    private final boolean compression;
    private final BinarySerializer serializer;
    private final BinaryDeserializer deserializer;

    private NativeClient(Socket socket) throws IOException {
        this.socket = socket;
        this.address = socket.getLocalSocketAddress();
        this.compression = ClickHouseDefines.COMPRESSION;

        this.serializer = new BinarySerializer(new SocketBuffedWriter(socket), compression);
        this.deserializer = new BinaryDeserializer(new SocketBuffedReader(socket), compression);
    }

    public SocketAddress address() {
        return address;
    }

    public boolean ping(Duration soTimeout, NativeContext.ServerContext info) {
        try {
            sendRequest(PingRequest.INSTANCE);
            while (true) {
                Response response = receiveResponse(soTimeout, info, true);

                if (response instanceof PongResponse)
                    return true;
                LOG.debug("expect pong, skip response: {}", response.type());
            }
        } catch (SQLException e) {
            LOG.warn(e.getMessage());
            return false;
        }
    }

    public Block receiveSampleBlock(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        while (true) {
            Response response = receiveResponse(soTimeout, info, true);
            if (response instanceof DataResponse) {
                return ((DataResponse) response).block();
            }
            LOG.debug("expect sample block, skip response: {}", response.type());
        }
    }

    public void sendHello(String client, long reversion, String db, String user, String password) throws SQLException {
        sendRequest(new HelloRequest(client, reversion, db, user, password));
    }

    public void sendQuery(String query, NativeContext.ClientContext info, Map<SettingKey, Serializable> settings) throws SQLException {
        sendQuery((String) settings.getOrDefault(SettingKey.query_id, UUID.randomUUID().toString()),
                QueryRequest.STAGE_COMPLETE, info, query, settings);
    }

    public void sendData(Block data) throws SQLException {
        sendRequest(new DataRequest("", data));
    }

    public HelloResponse receiveHello(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        Response response = receiveResponse(soTimeout, info, true);
        Validate.isTrue(response instanceof HelloResponse, "Expect Hello Response.");
        return (HelloResponse) response;
    }

    public EOFStreamResponse receiveEndOfStream(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        Response response = receiveResponse(soTimeout, info, true);
        Validate.isTrue(response instanceof EOFStreamResponse, "Expect EOFStream Response.");
        return (EOFStreamResponse) response;
    }

    public QueryResult receiveQuery(Duration soTimeout, NativeContext.ServerContext info, boolean lazy, boolean serialize) throws SQLException {
        if (lazy) {
            return new ClickHouseQueryResult(() -> receiveResponse(soTimeout, info, serialize));
        } else {
            List<Block> blocks = new LinkedList<>();
            try {
                boolean atEnd = false;
                while (!atEnd) {
                    Block block = null;
                    if (blocks.size() > 1 && !serialize) {
                        block = blocks.get(1);
                    }

                    Response response = receiveResponse(soTimeout, info, serialize, block);
                    if (response instanceof DataResponse) {
                        Block newBlock = ((DataResponse) response).block();
                        if (block != newBlock) {
                            blocks.add(newBlock);
                        }
                    } else if (response instanceof EOFStreamResponse) {
                        atEnd = true;
                    }
                }

                if (!serialize) {
                    for (Block block : blocks) {
                        block.rewind();
                    }
                    blocks.add(new Block());
                }
                return new ClickHouseQueryResult(blocks);
            } catch (Exception e) {
                if (!blocks.isEmpty() && !serialize) {
                    for (Block block : blocks) {
                        block.cleanup();
                    }
                }
                throw e;
            }
        }
    }

    public void silentDisconnect() {
        try {
            disconnect();
        } catch (Throwable th) {
            LOG.debug("disconnect throw exception.", th);
        }
    }

    public void disconnect() throws SQLException {
        try {
            if (socket.isClosed()) {
                LOG.info("socket already closed, ignore");
                return;
            }
            LOG.trace("flush and close socket");
            serializer.flushToTarget(true);
            socket.close();
        } catch (IOException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    private void sendQuery(String id, int stage, NativeContext.ClientContext info, String query,
                           Map<SettingKey, Serializable> settings) throws SQLException {
        sendRequest(new QueryRequest(id, info, stage, compression, query, settings));
    }

    private void sendRequest(Request request) throws SQLException {
        try {
            LOG.trace("send request: {}", request.type());
            request.writeTo(serializer);
            serializer.flushToTarget(true);
        } catch (IOException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    private Response receiveResponse(Duration soTimeout, NativeContext.ServerContext info, boolean serialize) throws SQLException {
        return receiveResponse(soTimeout, info, serialize, null);
    }

    private Response receiveResponse(Duration soTimeout, NativeContext.ServerContext info, boolean serialize, Block block) throws SQLException {
        try {
            socket.setSoTimeout(((int) soTimeout.toMillis()));
            Response response = Response.readFrom(deserializer, info, serialize, block);
            LOG.trace("recv response: {}", response.type());
            return response;
        } catch (IOException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }
}
