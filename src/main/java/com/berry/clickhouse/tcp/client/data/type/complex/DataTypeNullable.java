package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.DataTypeFactory;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeNullable implements IDataType {

    public static DataTypeCreator creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        IDataType nestedType = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeNullable(
                "Nullable(" + nestedType.name() + ")", nestedType, DataTypeFactory.get("UInt8", serverContext));
    };

    private static final Short IS_NULL = 1;
    private static final Short NON_NULL = 0;

    private final String name;
    private final IDataType nestedDataType;
    private final IDataType nullMapDataType;

    public IDataType getNestedDataType() {
        return nestedDataType;
    }

    public DataTypeNullable(String name, IDataType nestedDataType, IDataType nullMapIDataType) {
        this.name = name;
        this.nestedDataType = nestedDataType;
        this.nullMapDataType = nullMapIDataType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Object defaultValue() {
        return nestedDataType.defaultValue();
    }

    @Override
    public Class javaType() {
        return nestedDataType.javaType();
    }

    @Override
    public boolean nullable() {
        return true;
    }

    @Override
    public Object deserializeText(SQLLexer lexer) throws SQLException {
        if (lexer.isCharacter('n') || lexer.isCharacter('N')) {
            Validate.isTrue(Character.toLowerCase(lexer.character()) == 'n');
            Validate.isTrue(Character.toLowerCase(lexer.character()) == 'u');
            Validate.isTrue(Character.toLowerCase(lexer.character()) == 'l');
            Validate.isTrue(Character.toLowerCase(lexer.character()) == 'l');
            return null;
        }
        return nestedDataType.deserializeText(lexer);
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        this.nestedDataType.serializeBinary(data, serializer);
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        Short[] isNull = new Short[data.length];
        for (int i = 0; i < data.length; i++) {
            isNull[i] = (data[i] == null ? IS_NULL : NON_NULL);
            data[i] = data[i] == null ? nestedDataType.defaultValue() : data[i];
        }
        nullMapDataType.serializeBinaryBulk(isNull, serializer);
        nestedDataType.serializeBinaryBulk(data, serializer);
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        boolean isNull = (deserializer.readByte() == (byte) 1);
        if (isNull) {
            return null;
        }
        return this.nestedDataType.deserializeBinary(deserializer);
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] nullMap = nullMapDataType.deserializeBinaryBulk(rows, deserializer);

        Object[] data = nestedDataType.deserializeBinaryBulk(rows, deserializer);
        for (int i = 0; i < nullMap.length; i++) {
            if (IS_NULL.equals(nullMap[i])) {
                data[i] = null;
            }
        }
        return data;
    }

    @Override
    public boolean isSigned() {
        return nestedDataType.isSigned();
    }
}
