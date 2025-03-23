package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeFloat32类实现了IDataType接口
 * 表示ClickHouse中的Float32类型，使用Float表示
 */
public class DataTypeFloat32 implements IDataType<Float> {

    @Override
    public String name() {
        return "Float32"; // 返回数据类型名称
    }

    @Override
    public Float defaultValue() {
        return 0.0F; // 返回默认值
    }

    @Override
    public Class<Float> javaType() {
        return Float.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(Float data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeFloat(data); // 序列化为二进制格式
    }

    @Override
    public Float deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readFloat(); // 从二进制流反序列化
    }

    @Override
    public String[] getAliases() {
        return new String[]{"FLOAT"}; // 返回别名数组
    }

    @Override
    public Float deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().floatValue(); // 从文本解析Float值
    }

    @Override
    public boolean isSigned() {
        return true; // Float32是有符号的
    }
}
