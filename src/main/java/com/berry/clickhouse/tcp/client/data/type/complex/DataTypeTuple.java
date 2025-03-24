package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.DataTypeFactory;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseStruct;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;

public class DataTypeTuple implements IDataType<Object[]> {

    public static DataTypeCreator<Object[]> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        List<IDataType<?>> nestedDataTypes = new ArrayList<>();

        for (; ; ) {
            nestedDataTypes.add(DataTypeFactory.get(lexer, serverContext));
            char delimiter = lexer.character();
            Validate.isTrue(delimiter == ',' || delimiter == ')');
            if (delimiter == ')') {
                StringBuilder builder = new StringBuilder("Tuple(");
                for (int i = 0; i < nestedDataTypes.size(); i++) {
                    if (i > 0)
                        builder.append(",");
                    builder.append(nestedDataTypes.get(i).name());
                }
                return new DataTypeTuple(builder.append(")").toString(), nestedDataTypes.toArray(new IDataType[0]));
            }
        }
    };

    private final String name;
    private final IDataType<?>[] nestedTypes;

    public DataTypeTuple(String name, IDataType<?>[] nestedTypes) {
        this.name = name;
        this.nestedTypes = nestedTypes;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Object[] defaultValue() {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].defaultValue();
        }
        return attrs;
    }

    @Override
    public Class<Object[]> javaType() {
        return Object[].class;
    }

    @Override
    public void serializeBinary(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            getNestedTypes()[i].serializeBinary(data[i], serializer);
        }
    }

    @Override
    public void serializeBinaryBulk(Object[][] data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            Object[] elemsData = new Object[data.length];
            for (int row = 0; row < data.length; row++) {
                elemsData[row] = data[row][i];
            }
            getNestedTypes()[i].serializeBinaryBulk(elemsData, serializer);
        }
    }

    @Override
    public Object[] deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].deserializeBinary(deserializer);
        }
        return attrs;
    }

    @Override
    public Object[][] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        return getRowsWithElems(rows, deserializer);
    }

    private Object[][] getRowsWithElems(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        Object[][] rowsWithElems = new Object[getNestedTypes().length][];
        for (int index = 0; index < getNestedTypes().length; index++) {
            rowsWithElems[index] = getNestedTypes()[index].deserializeBinaryBulk(rows, deserializer);
        }
        return rowsWithElems;
    }

    public IDataType[] getNestedTypes() {
        return nestedTypes;
    }
}
