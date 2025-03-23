package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeInt16类实现了BaseDataTypeInt16接口
 * 表示ClickHouse中的Int16类型，使用Short表示
 */
public class DataTypeInt16 implements BaseDataTypeInt16<Short> {

    @Override
    public String name() {
        return "Int16"; // 返回数据类型名称
    }

    @Override
    public Short defaultValue() {
        return 0; // 返回默认值
    }

    @Override
    public Class<Short> javaType() {
        return Short.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(Short data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeShort(data); // 序列化为二进制格式
    }

    @Override
    public Short deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readShort(); // 从二进制流反序列化
    }

    @Override
    public String[] getAliases() {
        return new String[]{"SMALLINT"}; // 返回别名数组
    }

    @Override
    public Short deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().shortValue(); // 从文本解析Short值
    }

    @Override
    public boolean isSigned() {
        return true; // Int16是有符号的
    }
}
