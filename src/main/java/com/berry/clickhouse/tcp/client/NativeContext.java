/**
 * Clickhouse TCP客户端上下文类
 * 包含与Clickhouse服务器通信所需的客户端和服务器上下文信息
 */
package com.berry.clickhouse.tcp.client;

import com.berry.clickhouse.tcp.client.data.ColumnWriterBufferFactory;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;
import com.berry.clickhouse.tcp.client.settings.ClickHouseClientConfig;
import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;

import java.io.IOException;
import java.time.ZoneId;

/**
 * Native TCP协议通信的上下文类
 * 保存客户端与服务器交互所需的上下文信息
 * 包括服务器环境信息、客户端信息和连接对象
 */
public class NativeContext {

    /**
     * 客户端上下文信息
     */
    private final ClientContext clientCtx;
    
    /**
     * 服务器上下文信息
     */
    private final ServerContext serverCtx;
    
    /**
     * 原生客户端连接
     */
    private final NativeClient nativeClient;

    /**
     * 创建一个新的NativeContext实例
     * 
     * @param clientCtx 客户端上下文
     * @param serverCtx 服务器上下文
     * @param nativeClient 原生客户端连接
     */
    public NativeContext(ClientContext clientCtx, ServerContext serverCtx, NativeClient nativeClient) {
        this.clientCtx = clientCtx;
        this.serverCtx = serverCtx;
        this.nativeClient = nativeClient;
    }

    /**
     * 获取客户端上下文
     * 
     * @return 客户端上下文
     */
    public ClientContext clientCtx() {
        return clientCtx;
    }

    /**
     * 获取服务器上下文
     * 
     * @return 服务器上下文
     */
    public ServerContext serverCtx() {
        return serverCtx;
    }

    /**
     * 获取原生客户端连接
     * 
     * @return 原生客户端连接
     */
    public NativeClient nativeClient() {
        return nativeClient;
    }

    /**
     * 客户端上下文类
     * 包含客户端信息，用于与Clickhouse服务器进行握手和通信
     */
    public static class ClientContext {
        /**
         * TCP协议类型标识
         */
        public static final int TCP_KINE = 1;

        /**
         * 无查询状态
         */
        public static final byte NO_QUERY = 0;
        
        /**
         * 初始查询状态
         */
        public static final byte INITIAL_QUERY = 1;
        
        /**
         * 二级查询状态
         */
        public static final byte SECONDARY_QUERY = 2;

        /**
         * 客户端名称
         */
        private final String clientName;
        
        /**
         * 客户端主机名
         */
        private final String clientHostname;
        
        /**
         * 初始连接地址
         */
        private final String initialAddress;

        /**
         * 创建一个新的ClientContext实例
         * 
         * @param initialAddress 初始连接地址
         * @param clientHostname 客户端主机名
         * @param clientName 客户端名称
         */
        public ClientContext(String initialAddress, String clientHostname, String clientName) {
            this.clientName = clientName;
            this.clientHostname = clientHostname;
            this.initialAddress = initialAddress;
        }

        /**
         * 将客户端上下文信息写入二进制序列化器
         * 用于向服务器发送客户端信息
         * 
         * @param serializer 二进制序列化器
         * @throws IOException 如果写入失败
         */
        public void writeTo(BinarySerializer serializer) throws IOException {
            // 写入查询类型
            serializer.writeVarInt(ClientContext.INITIAL_QUERY);
            // 写入空字符串占位符
            serializer.writeUTF8StringBinary("");
            serializer.writeUTF8StringBinary("");
            // 写入初始地址
            serializer.writeUTF8StringBinary(initialAddress);

            // 对于TCP类型连接，写入相关信息
            serializer.writeVarInt(TCP_KINE);
            serializer.writeUTF8StringBinary("");
            serializer.writeUTF8StringBinary(clientHostname);
            serializer.writeUTF8StringBinary(clientName);
            // 写入客户端版本信息
            serializer.writeVarInt(ClickHouseDefines.MAJOR_VERSION);
            serializer.writeVarInt(ClickHouseDefines.MINOR_VERSION);
            serializer.writeVarInt(ClickHouseDefines.CLIENT_REVISION);
            serializer.writeUTF8StringBinary("");
        }
    }

    /**
     * 服务器上下文类
     * 包含服务器信息，用于解析服务器响应和适应服务器版本差异
     */
    public static class ServerContext {
        /**
         * 服务器主版本号
         */
        private final long majorVersion;
        
        /**
         * 服务器次版本号
         */
        private final long minorVersion;
        
        /**
         * 服务器修订版本号
         */
        private final long reversion;
        
        /**
         * 服务器时区
         */
        private final ZoneId timeZone;
        
        /**
         * 服务器显示名称
         */
        private final String displayName;
        
        /**
         * ClickHouse配置
         */
        private final ClickHouseClientConfig configure;
        
        /**
         * 列写入缓冲工厂
         */
        private ColumnWriterBufferFactory columnWriterBufferFactory;

        /**
         * 创建一个新的ServerContext实例
         * 
         * @param majorVersion 服务器主版本号
         * @param minorVersion 服务器次版本号
         * @param reversion 服务器修订版本号
         * @param configure ClickHouse配置
         * @param timeZone 服务器时区
         * @param displayName 服务器显示名称
         */
        public ServerContext(long majorVersion, long minorVersion, long reversion,
                             ClickHouseClientConfig configure,
                             ZoneId timeZone, String displayName, ColumnWriterBufferFactory columnWriterBufferFactory) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
            this.reversion = reversion;
            this.configure = configure;
            this.timeZone = timeZone;
            this.displayName = displayName;
            this.columnWriterBufferFactory = columnWriterBufferFactory;
        }

        /**
         * 获取服务器主版本号
         * 
         * @return 服务器主版本号
         */
        public long majorVersion() {
            return majorVersion;
        }

        /**
         * 获取服务器次版本号
         * 
         * @return 服务器次版本号
         */
        public long minorVersion() {
            return minorVersion;
        }

        /**
         * 获取服务器修订版本号
         * 
         * @return 服务器修订版本号
         */
        public long reversion() {
            return reversion;
        }

        /**
         * 获取服务器完整版本字符串
         * 
         * @return 格式为"主版本.次版本.修订版本"的版本字符串
         */
        public String version() {
            return majorVersion + "." + minorVersion + "." + reversion;
        }

        /**
         * 获取服务器时区
         * 
         * @return 服务器时区
         */
        public ZoneId timeZone() {
            return timeZone;
        }

        /**
         * 获取服务器显示名称
         * 
         * @return 服务器显示名称
         */
        public String displayName() {
            return displayName;
        }

        /**
         * 获取ClickHouse配置
         * 
         * @return ClickHouse配置
         */
        public ClickHouseClientConfig getConfigure() {
            return configure;
        }

        /**
         * 获取列写入缓冲工厂
         * 
         * @return 列写入缓冲工厂
         */
        public ColumnWriterBufferFactory getColumnWriterBufferFactory() {
            return columnWriterBufferFactory;
        }
    }
}
