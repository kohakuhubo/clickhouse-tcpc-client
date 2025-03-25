package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.DataTypeFactory;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeSimpleAggregateFunction implements IDataType<Object> {

    public static DataTypeCreator<Object> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        String functionName = String.valueOf(lexer.bareWord());
        Validate.isTrue(lexer.character() == ',');
        IDataType<?> nestedType = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ')');

        String name = "SimpleAggregateFunction(" + functionName + ", " + nestedType.name() + ")";
        return new DataTypeSimpleAggregateFunction(nestedType, name);
    };

    private final String name;

    private IDataType<?> nestedType;

    public DataTypeSimpleAggregateFunction(IDataType<?> nestedType, String name) {
        this.nestedType = nestedType;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int byteSize() {
        return nestedType.byteSize();
    }

    @Override
    public Object defaultValue() {
        return nestedType.defaultValue();
    }

    @Override
    public Class<Object> javaType() {
        return Object.class;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        getElemDataType().serializeBinary(data, serializer);
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return getElemDataType().deserializeBinary(deserializer);
    }

    public IDataType getElemDataType() {
        return nestedType;
    }
}
