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

public class DataTypeTuple implements IDataType<ClickHouseStruct> {

    public static DataTypeCreator<ClickHouseStruct> creator = (lexer, serverContext) -> {
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
    public ClickHouseStruct defaultValue() {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].defaultValue();
        }
        return new ClickHouseStruct("Tuple", attrs);
    }

    @Override
    public Class<ClickHouseStruct> javaType() {
        return ClickHouseStruct.class;
    }

    @Override
    public void serializeBinary(ClickHouseStruct data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            getNestedTypes()[i].serializeBinary(data.getAttributes()[i], serializer);
        }
    }

    @Override
    public void serializeBinaryBulk(ClickHouseStruct[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            Object[] elemsData = new Object[data.length];
            for (int row = 0; row < data.length; row++) {
                elemsData[row] = data[row].getAttributes()[i];
            }
            getNestedTypes()[i].serializeBinaryBulk(elemsData, serializer);
        }
    }

    @Override
    public ClickHouseStruct deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].deserializeBinary(deserializer);
        }
        return new ClickHouseStruct("Tuple", attrs);
    }

    @Override
    public ClickHouseStruct[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[][] rowsWithElems = getRowsWithElems(rows, deserializer);

        ClickHouseStruct[] rowsData = new ClickHouseStruct[rows];
        for (int row = 0; row < rows; row++) {
            Object[] elemsData = new Object[getNestedTypes().length];

            for (int elemIndex = 0; elemIndex < getNestedTypes().length; elemIndex++) {
                elemsData[elemIndex] = rowsWithElems[elemIndex][row];
            }
            rowsData[row] = new ClickHouseStruct("Tuple", elemsData);
        }
        return rowsData;
    }

    private Object[][] getRowsWithElems(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        Object[][] rowsWithElems = new Object[getNestedTypes().length][];
        for (int index = 0; index < getNestedTypes().length; index++) {
            rowsWithElems[index] = getNestedTypes()[index].deserializeBinaryBulk(rows, deserializer);
        }
        return rowsWithElems;
    }

    @Override
    public ClickHouseStruct deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '(');
        Object[] tupleData = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            if (i > 0)
                Validate.isTrue(lexer.character() == ',');
            tupleData[i] = getNestedTypes()[i].deserializeText(lexer);
        }
        Validate.isTrue(lexer.character() == ')');
        return new ClickHouseStruct("Tuple", tupleData);
    }

    public IDataType[] getNestedTypes() {
        return nestedTypes;
    }
}
