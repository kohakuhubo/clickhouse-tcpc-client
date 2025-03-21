package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.DateTimeUtil;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DataTypeDateTime implements IDataType<ZonedDateTime> {

    public static DataTypeCreator<ZonedDateTime> creator = (lexer, serverContext) -> {
        if (lexer.isCharacter('(')) {
            Validate.isTrue(lexer.character() == '(');
            String dataTimeZone = lexer.stringLiteral();
            Validate.isTrue(lexer.character() == ')');
            return new DataTypeDateTime("DateTime('" + dataTimeZone + "')", serverContext);
        }
        return new DataTypeDateTime("DateTime", serverContext);
    };

    private static final LocalDateTime EPOCH_LOCAL_DT = LocalDateTime.of(1970, 1, 1, 0, 0);
    private final String name;
    private final ZoneId tz;
    private final ZonedDateTime defaultValue;

    public DataTypeDateTime(String name, NativeContext.ServerContext serverContext) {
        this.name = name;
        this.tz = DateTimeUtil.chooseTimeZone(serverContext);
        this.defaultValue = EPOCH_LOCAL_DT.atZone(tz);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ZonedDateTime defaultValue() {
        return defaultValue;
    }

    @Override
    public Class<ZonedDateTime> javaType() {
        return ZonedDateTime.class;
    }

    @Override
    public ZonedDateTime deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '\'');
        int year = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '-');
        int month = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '-');
        int day = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.isWhitespace());
        int hours = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == ':');
        int minutes = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == ':');
        int seconds = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '\'');

        return ZonedDateTime.of(year, month, day, hours, minutes, seconds, 0, tz);
    }

    @Override
    public void serializeBinary(ZonedDateTime data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeInt((int) DateTimeUtil.toEpochSecond(data));
    }

    @Override
    public ZonedDateTime deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        int epochSeconds = deserializer.readInt();
        return DateTimeUtil.toZonedDateTime(epochSeconds, 0, tz);
    }

    @Override
    public String[] getAliases() {
        return new String[]{"TIMESTAMP"};
    }
}
