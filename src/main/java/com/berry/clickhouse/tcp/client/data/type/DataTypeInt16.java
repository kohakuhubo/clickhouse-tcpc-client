package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeInt16 implements BaseDataTypeInt16<Short> {

    @Override
    public String name() {
        return "Int16";
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
        serializer.writeShort(data);
    }

    @Override
    public Short deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readShort();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"SMALLINT"};
    }

    @Override
    public Short deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().shortValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
