package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.DataTypeFactory;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.data.type.DataTypeInt64;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseArray;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataTypeArray implements IDataType<ClickHouseArray> {

    public static DataTypeCreator<ClickHouseArray> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        IDataType<?> arrayNestedType = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeArray("Array(" + arrayNestedType.name() + ")",
                arrayNestedType, (DataTypeInt64) DataTypeFactory.get("Int64", serverContext));
    };

    private final String name;
    private final ClickHouseArray defaultValue;
    private final IDataType<?> elemDataType;
    private final DataTypeInt64 offsetIDataType;

    public DataTypeArray(String name, IDataType<?> elemDataType, DataTypeInt64 offsetIDataType) throws SQLException {
        this.name = name;
        this.elemDataType = elemDataType;
        this.offsetIDataType = offsetIDataType;
        this.defaultValue = new ClickHouseArray(elemDataType, new Object[]{elemDataType.defaultValue()});
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ClickHouseArray defaultValue() {
        return defaultValue;
    }

    @Override
    public Class<ClickHouseArray> javaType() {
        return ClickHouseArray.class;
    }

    @Override
    public ClickHouseArray deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '[');
        List<Object> arrayData = new ArrayList<>();
        for (; ; ) {
            if (lexer.isCharacter(']')) {
                lexer.character();
                break;
            }
            if (lexer.isCharacter(',')) {
                lexer.character();
            }
            arrayData.add(elemDataType.deserializeText(lexer));
        }
        return new ClickHouseArray(elemDataType, arrayData.toArray());
    }

    @Override
    public void serializeBinary(ClickHouseArray data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object f : data.getArray()) {
            getElemDataType().serializeBinary(f, serializer);
        }
    }


    @Override
    public void serializeBinaryBulk(ClickHouseArray[] data, BinarySerializer serializer) throws SQLException, IOException {
        offsetIDataType.serializeBinary((long) data.length, serializer);
        getElemDataType().serializeBinaryBulk(data, serializer);
    }

    @Override
    public ClickHouseArray deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Long offset = offsetIDataType.deserializeBinary(deserializer);
        Object[] data = getElemDataType().deserializeBinaryBulk(offset.intValue(), deserializer);
        return new ClickHouseArray(elemDataType, data);
    }

    @Override
    public ClickHouseArray[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        ClickHouseArray[] arrays = new ClickHouseArray[rows];
        if (rows == 0) {
            return arrays;
        }

        int[] offsets = Arrays.stream(offsetIDataType.deserializeBinaryBulk(rows, deserializer)).mapToInt(value -> ((Long) value).intValue()).toArray();
        ClickHouseArray res = new ClickHouseArray(elemDataType,
                elemDataType.deserializeBinaryBulk(offsets[rows - 1], deserializer));

        for (int row = 0, lastOffset = 0; row < rows; row++) {
            int offset = offsets[row];
            arrays[row] = res.slice(lastOffset, offset - lastOffset);
            lastOffset = offset;
        }
        return arrays;
    }

    public IDataType getElemDataType() {
        return elemDataType;
    }
}
