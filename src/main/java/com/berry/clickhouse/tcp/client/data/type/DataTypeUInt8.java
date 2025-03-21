package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeUInt8 implements BaseDataTypeInt8<Short> {

    @Override
    public String name() {
        return "UInt8";
    }

    @Override
    public Short defaultValue() {
        return 0;
    }

    @Override
    public Class<Short> javaType() {
        return Short.class;
    }

    @Override
    public void serializeBinary(Short data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeByte(data.byteValue());
    }

    @Override
    public Short deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        byte b = deserializer.readByte();
        return (short) (b & 0xff);
    }

    @Override
    public Short deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().shortValue();
    }
}
