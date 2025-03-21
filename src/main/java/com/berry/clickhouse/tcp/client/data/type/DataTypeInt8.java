package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeInt8 implements BaseDataTypeInt8<Byte> {

    @Override
    public String name() {
        return "Int8";
    }

    @Override
    public Byte defaultValue() {
        return 0;
    }

    @Override
    public Class<Byte> javaType() {
        return Byte.class;
    }

    @Override
    public void serializeBinary(Byte data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeByte(data);
    }

    @Override
    public Byte deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        return deserializer.readByte();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"TINYINT"};
    }

    @Override
    public Byte deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().byteValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
