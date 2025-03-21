package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.time.LocalDate;

public class DataTypeDate implements IDataType<LocalDate> {

    private static final LocalDate DEFAULT_VALUE = LocalDate.ofEpochDay(0);

    public DataTypeDate() {
    }

    @Override
    public String name() {
        return "Date";
    }

    @Override
    public LocalDate defaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public Class<LocalDate> javaType() {
        return LocalDate.class;
    }

    @Override
    public void serializeBinary(LocalDate data, BinarySerializer serializer) throws SQLException, IOException {
        long epochDay = data.toEpochDay();
        serializer.writeShort((short) epochDay);
    }

    @Override
    public LocalDate deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        short epochDay = deserializer.readShort();
        return LocalDate.ofEpochDay(epochDay & 0xFFFF);
    }

    @Override
    public String[] getAliases() {
        return new String[0];
    }

    @Override
    public LocalDate deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '\'');
        int year = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '-');
        int month = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '-');
        int day = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '\'');

        return LocalDate.of(year, month, day);
    }
}
