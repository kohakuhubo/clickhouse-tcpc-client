package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeFloat32 implements IDataType<Float> {

    @Override
    public String name() {
        return "Float32";
    }

    @Override
    public Float defaultValue() {
        return 0.0F;
    }

    @Override
    public Class<Float> javaType() {
        return Float.class;
    }

    @Override
    public void serializeBinary(Float data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeFloat(data);
    }

    @Override
    public Float deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        return deserializer.readFloat();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"FLOAT"};
    }

    @Override
    public Float deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().floatValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
