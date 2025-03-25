/**
 * Clickhouse数据库TCP客户端的底层实现
 * 负责原生TCP协议通信，包括连接建立、数据传输和查询执行
 */
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
import com.berry.clickhouse.tcp.client.settings.ClickHouseClientConfig;
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

/**
 * Clickhouse数据库原生TCP协议客户端
 * 实现了底层的网络通信、请求发送和响应处理
 * 支持SSL安全连接和数据压缩
 */
public class NativeClient {

    /**
     * 日志记录器
     */
    private static final Logger LOG = LoggerFactory.getLogger(NativeClient.class);

    /**
     * 使用ClickHouse配置创建一个新的NativeClient实例
     * 
     * @param config ClickHouse配置
     * @return 新创建的NativeClient实例
     * @throws SQLException 如果连接失败
     */
    public static NativeClient connect(ClickHouseClientConfig config) throws SQLException {
        return connect(config.host(), config.port(), config);
    }

    /**
     * 使用指定的主机、端口和配置创建一个新的NativeClient实例
     * 
     * @param host 主机地址
     * @param port 端口号
     * @param config ClickHouse配置
     * @return 新创建的NativeClient实例
     * @throws SQLException 如果连接失败
     */
    public static NativeClient connect(String host, int port, ClickHouseClientConfig config) throws SQLException {
        try {
            SocketAddress endpoint = new InetSocketAddress(host, port);
            Socket socket;

            // 根据配置决定是否使用SSL
            boolean useSSL = config.ssl();
            if (useSSL) {
                LOG.debug("Client works in SSL mode!");
                SSLContext context = new SSLContextBuilder(config).getSSLContext();
                SSLSocketFactory factory = context.getSocketFactory();
                socket = (SSLSocket) factory.createSocket();
            } else {
                socket = new Socket();
            }
            
            // 配置Socket参数
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(ClickHouseDefines.SOCKET_SEND_BUFFER_BYTES);
            socket.setReceiveBufferSize(ClickHouseDefines.SOCKET_RECV_BUFFER_BYTES);
            socket.setKeepAlive(config.tcpKeepAlive());
            socket.connect(endpoint, (int) config.connectTimeout().toMillis());

            // 如果使用SSL，启动握手过程
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

    /**
     * Socket连接
     */
    private final Socket socket;
    
    /**
     * 本地Socket地址
     */
    private final SocketAddress address;
    
    /**
     * 是否启用压缩
     */
    private final boolean compression;
    
    /**
     * 二进制序列化器，用于发送数据
     */
    private final BinarySerializer serializer;
    
    /**
     * 二进制反序列化器，用于接收数据
     */
    private final BinaryDeserializer deserializer;

    /**
     * 使用已存在的Socket创建NativeClient
     * 
     * @param socket 已连接的Socket
     * @throws IOException 如果创建I/O流失败
     */
    private NativeClient(Socket socket) throws IOException {
        this.socket = socket;
        this.address = socket.getLocalSocketAddress();
        this.compression = ClickHouseDefines.COMPRESSION;

        // 初始化序列化器和反序列化器
        this.serializer = new BinarySerializer(new SocketBuffedWriter(socket), compression);
        this.deserializer = new BinaryDeserializer(new SocketBuffedReader(socket), compression);
    }

    /**
     * 获取客户端本地地址
     * 
     * @return 本地Socket地址
     */
    public SocketAddress address() {
        return address;
    }

    /**
     * 向服务器发送ping请求，检测连接是否有效
     * 
     * @param soTimeout Socket超时时间
     * @param info 服务器上下文信息
     * @return 如果ping成功则返回true，否则返回false
     */
    public boolean ping(Duration soTimeout, NativeContext.ServerContext info) {
        try {
            // 发送ping请求
            sendRequest(PingRequest.INSTANCE);
            while (true) {
                // 接收响应
                Response response = receiveResponse(soTimeout, info, true);

                // 如果收到pong响应，则ping成功
                if (response instanceof PongResponse)
                    return true;
                LOG.debug("expect pong, skip response: {}", response.type());
            }
        } catch (SQLException e) {
            LOG.warn(e.getMessage());
            return false;
        }
    }

    /**
     * 接收样本数据块，用于了解表结构
     * 
     * @param soTimeout Socket超时时间
     * @param info 服务器上下文信息
     * @return 接收到的样本数据块
     * @throws SQLException 如果接收失败
     */
    public Block receiveSampleBlock(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        while (true) {
            Response response = receiveResponse(soTimeout, info, true);
            if (response instanceof DataResponse) {
                return ((DataResponse) response).block();
            }
            LOG.debug("expect sample block, skip response: {}", response.type());
        }
    }

    /**
     * 发送Hello请求，进行客户端身份验证
     * 
     * @param client 客户端名称
     * @param reversion 客户端版本
     * @param db 数据库名称
     * @param user 用户名
     * @param password 密码
     * @throws SQLException 如果发送失败
     */
    public void sendHello(String client, long reversion, String db, String user, String password) throws SQLException {
        sendRequest(new HelloRequest(client, reversion, db, user, password));
    }

    /**
     * 发送查询请求
     * 
     * @param query SQL查询语句
     * @param info 客户端上下文信息
     * @param settings 查询设置
     * @throws SQLException 如果发送失败
     */
    public void sendQuery(String query, NativeContext.ClientContext info, Map<SettingKey, Serializable> settings) throws SQLException {
        sendQuery((String) settings.getOrDefault(SettingKey.query_id, UUID.randomUUID().toString()),
                QueryRequest.STAGE_COMPLETE, info, query, settings);
    }

    /**
     * 发送数据块
     * 
     * @param data 要发送的数据块
     * @throws SQLException 如果发送失败
     */
    public void sendData(Block data) throws SQLException {
        sendRequest(new DataRequest("", data));
    }

    /**
     * 接收Hello响应
     * 
     * @param soTimeout Socket超时时间
     * @param info 服务器上下文信息
     * @return Hello响应
     * @throws SQLException 如果接收失败或响应类型不匹配
     */
    public HelloResponse receiveHello(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        Response response = receiveResponse(soTimeout, info, true);
        Validate.isTrue(response instanceof HelloResponse, "Expect Hello Response.");
        return (HelloResponse) response;
    }

    /**
     * 接收流结束响应
     * 
     * @param soTimeout Socket超时时间
     * @param info 服务器上下文信息
     * @return 流结束响应
     * @throws SQLException 如果接收失败或响应类型不匹配
     */
    public EOFStreamResponse receiveEndOfStream(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        Response response = receiveResponse(soTimeout, info, true);
        Validate.isTrue(response instanceof EOFStreamResponse, "Expect EOFStream Response.");
        return (EOFStreamResponse) response;
    }

    /**
     * 接收查询结果
     * 
     * @param soTimeout Socket超时时间
     * @param info 服务器上下文信息
     * @param lazy 是否懒加载结果
     * @param serialize 是否序列化结果
     * @return 查询结果
     * @throws SQLException 如果接收失败
     */
    public QueryResult receiveQuery(Duration soTimeout, NativeContext.ServerContext info, boolean lazy, boolean serialize) throws SQLException {
        if (lazy) {
            // 懒加载模式
            return new ClickHouseQueryResult(() -> receiveResponse(soTimeout, info, serialize));
        } else {
            // 立即加载全部数据
            List<Block> blocks = new LinkedList<>();
            try {
                boolean atEnd = false;
                while (!atEnd) {
                    Block block = null;
                    if (blocks.size() > 1 && !serialize) {
                        block = blocks.get(1);
                    }

                    // 接收响应
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

                // 处理非序列化结果
                if (!serialize) {
                    for (Block block : blocks) {
                        block.rewind();
                    }
                    blocks.add(new Block());
                }
                return new ClickHouseQueryResult(blocks);
            } catch (Exception e) {
                // 出错时释放资源
                if (!blocks.isEmpty() && !serialize) {
                    for (Block block : blocks) {
                        block.cleanup();
                    }
                }
                throw e;
            }
        }
    }

    /**
     * 静默断开连接，忽略可能的异常
     */
    public void silentDisconnect() {
        try {
            disconnect();
        } catch (Throwable th) {
            LOG.debug("disconnect throw exception.", th);
        }
    }

    /**
     * 断开连接
     * 
     * @throws SQLException 如果断开连接失败
     */
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

    /**
     * 发送查询请求（内部方法）
     * 
     * @param id 查询ID
     * @param stage 查询执行阶段
     * @param info 客户端上下文信息
     * @param query SQL查询语句
     * @param settings 查询设置
     * @throws SQLException 如果发送失败
     */
    private void sendQuery(String id, int stage, NativeContext.ClientContext info, String query,
                           Map<SettingKey, Serializable> settings) throws SQLException {
        sendRequest(new QueryRequest(id, info, stage, compression, query, settings));
    }

    /**
     * 发送请求（内部方法）
     * 
     * @param request 要发送的请求
     * @throws SQLException 如果发送失败
     */
    private void sendRequest(Request request) throws SQLException {
        try {
            LOG.trace("send request: {}", request.type());
            request.writeTo(serializer);
            serializer.flushToTarget(true);
        } catch (IOException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    /**
     * 接收响应（内部方法）
     * 
     * @param soTimeout Socket超时时间
     * @param info 服务器上下文信息
     * @param serialize 是否序列化结果
     * @return 接收到的响应
     * @throws SQLException 如果接收失败
     */
    private Response receiveResponse(Duration soTimeout, NativeContext.ServerContext info, boolean serialize) throws SQLException {
        return receiveResponse(soTimeout, info, serialize, null);
    }

    /**
     * 接收响应，可指定数据块（内部方法）
     * 
     * @param soTimeout Socket超时时间
     * @param info 服务器上下文信息
     * @param serialize 是否序列化结果
     * @param block 可选的数据块，用于数据复用
     * @return 接收到的响应
     * @throws SQLException 如果接收失败
     */
    private Response receiveResponse(Duration soTimeout, NativeContext.ServerContext info, boolean serialize, Block block) throws SQLException {
        try {
            // 设置Socket超时时间
            socket.setSoTimeout(((int) soTimeout.toMillis()));
            // 从流中读取响应
            Response response = Response.readFrom(deserializer, info, serialize, block);
            LOG.trace("recv response: {}", response.type());
            return response;
        } catch (IOException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }
}
