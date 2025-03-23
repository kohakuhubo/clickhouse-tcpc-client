package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.BytesHelper;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;

/**
 * DataTypeUInt64类实现了BaseDataTypeInt64接口
 * 表示ClickHouse中的UInt64类型，使用BigInteger表示
 */
public class DataTypeUInt64 implements BaseDataTypeInt64<BigInteger>, BytesHelper {

    @Override
    public String name() {
        return "UInt64"; // 返回数据类型名称
    }

    @Override
    public BigInteger defaultValue() {
        return BigInteger.ZERO; // 返回默认值
    }

    @Override
    public Class<BigInteger> javaType() {
        return BigInteger.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(BigInteger data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeLong(data.longValue()); // 序列化为二进制格式
    }

    @Override
    public BigInteger deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        long l = deserializer.readLong(); // 从二进制流反序列化
        return BigInteger.valueOf(l); // 返回UInt64值
    }

    @Override
    public String[] getAliases() {
        return new String[0]; // 返回别名数组
    }

    @Override
    public BigInteger deserializeText(SQLLexer lexer) throws SQLException {
        return BigInteger.valueOf(lexer.numberLiteral().longValue()); // 从文本解析BigInteger值
    }

    @Override
    public boolean isSigned() {
        return false; // UInt64是无符号的
    }
}
