/**
 * ClickHouse Hello响应类
 * 表示服务器对Hello请求的响应
 * 包含服务器信息，如名称、版本和时区
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;

import java.io.IOException;
import java.time.ZoneId;

/**
 * Hello响应实现类
 * 包含服务器返回的基本信息
 * 用于确认连接建立并获取服务器配置
 */
public class HelloResponse implements Response {

    /**
     * 从二进制流中读取HelloResponse对象
     * 读取服务器名称、版本信息、时区和显示名称
     * 
     * @param deserializer 二进制反序列化器
     * @return 新创建的HelloResponse对象
     * @throws IOException 如果读取操作失败
     */
    public static HelloResponse readFrom(BinaryDeserializer deserializer) throws IOException {
        String name = deserializer.readUTF8StringBinary();
        long majorVersion = deserializer.readVarInt();
        long minorVersion = deserializer.readVarInt();
        long serverReversion = deserializer.readVarInt();
        String serverTimeZone = getTimeZone(deserializer, serverReversion);
        String serverDisplayName = getDisplayName(deserializer, serverReversion);

        return new HelloResponse(name, majorVersion, minorVersion, serverReversion, serverTimeZone, serverDisplayName);
    }

    /**
     * 根据服务器版本获取时区信息
     * 对于支持时区的服务器版本读取时区信息，否则使用系统默认时区
     * 
     * @param deserializer 二进制反序列化器
     * @param serverReversion 服务器修订版本号
     * @return 服务器时区ID
     * @throws IOException 如果读取操作失败
     */
    private static String getTimeZone(BinaryDeserializer deserializer, long serverReversion) throws IOException {
        return serverReversion >= ClickHouseDefines.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE ?
                deserializer.readUTF8StringBinary() : ZoneId.systemDefault().getId();
    }

    /**
     * 根据服务器版本获取显示名称
     * 对于支持显示名称的服务器版本读取显示名称，否则使用"localhost"
     * 
     * @param deserializer 二进制反序列化器
     * @param serverReversion 服务器修订版本号
     * @return 服务器显示名称
     * @throws IOException 如果读取操作失败
     */
    private static String getDisplayName(BinaryDeserializer deserializer, long serverReversion) throws IOException {
        return serverReversion >= ClickHouseDefines.DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME ?
                deserializer.readUTF8StringBinary() : "localhost";
    }

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
     * 服务器名称
     */
    private final String serverName;
    
    /**
     * 服务器时区
     */
    private final String serverTimeZone;
    
    /**
     * 服务器显示名称
     */
    private final String serverDisplayName;

    /**
     * 创建一个新的HelloResponse实例
     * 
     * @param serverName 服务器名称
     * @param majorVersion 服务器主版本号
     * @param minorVersion 服务器次版本号
     * @param reversion 服务器修订版本号
     * @param serverTimeZone 服务器时区
     * @param serverDisplayName 服务器显示名称
     */
    public HelloResponse(
            String serverName, long majorVersion, long minorVersion, long reversion,
            String serverTimeZone,
            String serverDisplayName) {

        this.reversion = reversion;
        this.serverName = serverName;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.serverTimeZone = serverTimeZone;
        this.serverDisplayName = serverDisplayName;
    }

    /**
     * 获取响应类型
     * 
     * @return 响应类型（RESPONSE_HELLO）
     */
    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_HELLO;
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
     * 获取服务器名称
     * 
     * @return 服务器名称
     */
    public String serverName() {
        return serverName;
    }

    /**
     * 获取服务器时区
     * 
     * @return 服务器时区
     */
    public String serverTimeZone() {
        return serverTimeZone;
    }

    /**
     * 获取服务器显示名称
     * 
     * @return 服务器显示名称
     */
    public String serverDisplayName() {
        return serverDisplayName;
    }
}
