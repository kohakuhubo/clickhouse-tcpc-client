package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;


public class DataTypeFloat64 implements IDataType<Double> {

    @Override
    public String name() {
        return "Float64";
    }

    @Override
    public Double defaultValue() {
        return 0.0D;
    }

    @Override
    public Class<Double> javaType() {
        return Double.class;
    }

    @Override
    public void serializeBinary(Double data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeDouble(data);
    }

    @Override
    public Double deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readDouble();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"DOUBLE"};
    }

    @Override
    public Double deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().doubleValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
