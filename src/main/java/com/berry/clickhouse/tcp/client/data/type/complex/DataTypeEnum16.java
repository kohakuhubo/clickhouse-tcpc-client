package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;

public class DataTypeEnum16 implements IDataType<String> {

    public static DataTypeCreator<String> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        List<Short> enumValues = new ArrayList<>();
        List<String> enumNames = new ArrayList<>();

        for (int i = 0; i < 1 << 16; i++) {
            enumNames.add(lexer.stringLiteral());
            Validate.isTrue(lexer.character() == '=');
            enumValues.add(lexer.numberLiteral().shortValue());

            char character = lexer.character();
            Validate.isTrue(character == ',' || character == ')');

            if (character == ')') {
                StringBuilder builder = new StringBuilder("Enum16(");
                for (int index = 0; index < enumNames.size(); index++) {
                    if (index > 0)
                        builder.append(",");
                    builder.append("'").append(enumNames.get(index)).append("'")
                            .append(" = ").append(enumValues.get(index));
                }
                builder.append(")");
                return new DataTypeEnum16(builder.toString(),
                        enumNames.toArray(new String[0]), enumValues.toArray(new Short[0]));
            }
        }
        throw new SQLException("DataType Enum16 size must be less than 65535");
    };

    private final String name;
    private final Short[] values;
    private final String[] names;

    public DataTypeEnum16(String name, String[] names, Short[] values) {
        this.name = name;
        this.names = names;
        this.values = values;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int byteSize() {
        return Short.BYTES;
    }

    @Override
    public String defaultValue() {
        return names[0];
    }

    @Override
    public Class<String> javaType() {
        return String.class;
    }

    @Override
    public void serializeBinary(String data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < names.length; i++) {
            if (data.equals(names[i])) {
                serializer.writeShort(values[i]);
                return;
            }
        }

        StringBuilder message = new StringBuilder("Expected ");
        for (int i = 0; i < names.length; i++) {
            if (i > 0)
                message.append(" OR ");
            message.append(names[i]);
        }
        message.append(", but was ").append(data);

        throw new SQLException(message.toString());
    }

    @Override
    public String deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        short value = deserializer.readShort();
        for (int i = 0; i < values.length; i++) {
            if (values[i].equals(value)) {
                return names[i];
            }
        }
        throw new SQLException("");
    }
}
