package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.DataTypeFactory;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeLowCardinality implements IDataType {

    public static DataTypeCreator<?> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        IDataType<?> nestedType = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeLowCardinality(
                "LowCardinality(" + nestedType.name() + ")", nestedType);
    };

    private final String name;
    private final IDataType nestedDataType;

    public DataTypeLowCardinality(String name, IDataType<?> nestedDataType) {
        this.name = name;
        this.nestedDataType = nestedDataType;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public Object defaultValue() {
        return this.nestedDataType.defaultValue();
    }

    @Override
    public Class<?> javaType() {
        return this.nestedDataType.javaType();
    }

    @Override
    public boolean nullable() {
        return this.nestedDataType.nullable();
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        this.nestedDataType.serializeBinary(data, serializer);
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        this.nestedDataType.serializeBinaryBulk(data, serializer);
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return this.nestedDataType.deserializeBinary(deserializer);
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] data = this.nestedDataType.deserializeBinaryBulk(rows, deserializer);
        return data;
    }

    @Override
    public boolean isSigned() {
        return this.nestedDataType.isSigned();
    }
}
