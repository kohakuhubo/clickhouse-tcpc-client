/**
 * ClickHouse TCP协议响应接口
 * 定义了所有从ClickHouse服务器接收的响应的基本结构
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.exception.NotImplementedException;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * ClickHouse客户端响应接口
 * 所有从服务器接收的响应都实现此接口
 */
public interface Response {

    /**
     * 获取响应类型
     * 
     * @return 响应类型枚举值
     */
    ProtoType type();

    /**
     * 从反序列化器读取并创建响应对象
     * 根据响应类型ID创建对应的响应实现类实例
     * 
     * @param deserializer 二进制反序列化器
     * @param info 服务器上下文信息
     * @param serialize 是否进行序列化处理
     * @param block 可选的数据块，用于复用已有对象
     * @return 响应对象
     * @throws IOException 如果读取操作失败
     * @throws SQLException 如果处理响应时发生SQL错误
     */
    static Response readFrom(BinaryDeserializer deserializer, NativeContext.ServerContext info, boolean serialize, Block block) throws IOException, SQLException {
        switch ((int) deserializer.readVarInt()) {
            case 0:
                return HelloResponse.readFrom(deserializer);
            case 1:
                return DataResponse.readFrom(deserializer, info, serialize, block);
            case 2:
                throw ExceptionResponse.readExceptionFrom(deserializer);
            case 3:
                return ProgressResponse.readFrom(deserializer);
            case 4:
                return PongResponse.readFrom(deserializer);
            case 5:
                return EOFStreamResponse.readFrom(deserializer);
            case 6:
                return ProfileInfoResponse.readFrom(deserializer);
            case 7:
                return TotalsResponse.readFrom(deserializer, info);
            case 8:
                return ExtremesResponse.readFrom(deserializer, info);
            case 9:
                throw new NotImplementedException("RESPONSE_TABLES_STATUS_RESPONSE");
            default:
                throw new IllegalStateException("Accept the id of response that is not recognized by Server.");
        }
    }

    /**
     * ClickHouse TCP协议响应类型枚举
     * 定义了服务器可能返回的各种响应类型及其对应的ID
     */
    enum ProtoType {
        /**
         * Hello响应，服务器返回的连接确认和服务器信息
         */
        RESPONSE_HELLO(0),
        
        /**
         * 数据响应，包含查询结果数据
         */
        RESPONSE_DATA(1),
        
        /**
         * 异常响应，服务器返回的错误信息
         */
        RESPONSE_EXCEPTION(2),
        
        /**
         * 进度响应，查询执行进度信息
         */
        RESPONSE_PROGRESS(3),
        
        /**
         * Pong响应，对Ping请求的回应
         */
        RESPONSE_PONG(4),
        
        /**
         * 流结束响应，表示数据流传输完成
         */
        RESPONSE_END_OF_STREAM(5),
        
        /**
         * 性能分析响应，包含查询执行性能信息
         */
        RESPONSE_PROFILE_INFO(6),
        
        /**
         * 汇总响应，包含聚合查询的汇总数据
         */
        RESPONSE_TOTALS(7),
        
        /**
         * 极值响应，包含查询结果的最大/最小值
         */
        RESPONSE_EXTREMES(8),
        
        /**
         * 表状态响应，包含表的状态信息
         */
        RESPONSE_TABLES_STATUS_RESPONSE(9);

        /**
         * 响应类型ID
         */
        private final int id;

        /**
         * 构造函数
         * 
         * @param id 响应类型ID
         */
        ProtoType(int id) {
            this.id = id;
        }

        /**
         * 获取响应类型ID
         * 
         * @return 响应类型ID
         */
        public long id() {
            return id;
        }
    }
}
