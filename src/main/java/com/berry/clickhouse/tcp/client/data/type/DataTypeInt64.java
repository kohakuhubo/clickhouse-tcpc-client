package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeInt64类实现了BaseDataTypeInt64接口
 * 表示ClickHouse中的Int64类型，使用Long表示
 */
public class DataTypeInt64 implements BaseDataTypeInt64<Long> {

    @Override
    public String name() {
        return "Int64"; // 返回数据类型名称
    }

    @Override
    public Long defaultValue() {
        return 0L; // 返回默认值
    }

    @Override
    public Class<Long> javaType() {
        return Long.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(Long data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeLong(data); // 序列化为二进制格式
    }

    @Override
    public Long deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readLong(); // 从二进制流反序列化
    }

    @Override
    public String[] getAliases() {
        return new String[]{"BIGINT"}; // 返回别名数组
    }

    @Override
    public Long deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().longValue(); // 从文本解析Long值
    }

    @Override
    public boolean isSigned() {
        return true; // Int64是有符号的
    }
}
