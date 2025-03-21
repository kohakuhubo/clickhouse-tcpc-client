package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.NativeContext.ServerContext;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.DateTimeUtil;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.StringView;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DataTypeDateTime64 implements IDataType<ZonedDateTime> {

    public static DataTypeCreator<ZonedDateTime> creator = (lexer, serverContext) -> {
        if (lexer.isCharacter('(')) {
            Validate.isTrue(lexer.character() == '(');
            int scale = lexer.numberLiteral().intValue();
            Validate.isTrue(scale >= DataTypeDateTime64.MIN_SCALE && scale <= DataTypeDateTime64.MAX_SCALA,
                    "scale=" + scale + " out of range [" + DataTypeDateTime64.MIN_SCALE + "," + DataTypeDateTime64.MAX_SCALA + "]");
            if (lexer.isCharacter(',')) {
                Validate.isTrue(lexer.character() == ',');
                Validate.isTrue(lexer.isWhitespace());
                String dataTimeZone = lexer.stringLiteral();
                Validate.isTrue(lexer.character() == ')');
                return new DataTypeDateTime64("DateTime64(" + scale + ", '" + dataTimeZone + "')", scale, serverContext);
            }

            Validate.isTrue(lexer.character() == ')');
            return new DataTypeDateTime64("DateTime64(" + scale + ")", scale, serverContext);
        }
        return new DataTypeDateTime64("DateTime64", DataTypeDateTime64.DEFAULT_SCALE, serverContext);
    };

    private static final LocalDateTime EPOCH_LOCAL_DT = LocalDateTime.of(1970, 1, 1, 0, 0);
    public static final int NANOS_IN_SECOND = 1_000_000_000;
    public static final int MILLIS_IN_SECOND = 1000;
    public static final int[] POW_10 = {1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000};
    public static final int MIN_SCALE = 0;
    public static final int MAX_SCALA = 9;
    public static final int DEFAULT_SCALE = 3;

    private final String name;
    private final int scale;
    private final ZoneId tz;
    private final ZonedDateTime defaultValue;

    public DataTypeDateTime64(String name, int scala, ServerContext serverContext) {
        this.name = name;
        this.scale = scala;
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
        StringView dataTypeName = lexer.bareWord();
        Validate.isTrue(dataTypeName.checkEquals("toDateTime64"));
        Validate.isTrue(lexer.character() == '(');
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
        BigDecimal _seconds = BigDecimal.valueOf(lexer.numberLiteral().doubleValue())
                .setScale(scale, BigDecimal.ROUND_HALF_UP);
        int second = _seconds.intValue();
        int nanos = _seconds.subtract(BigDecimal.valueOf(second)).movePointRight(9).intValue();
        Validate.isTrue(lexer.character() == '\'');
        Validate.isTrue(lexer.character() == ')');

        return ZonedDateTime.of(year, month, day, hours, minutes, second, nanos, tz);
    }

    @Override
    public void serializeBinary(ZonedDateTime data, BinarySerializer serializer) throws IOException {
        long epochSeconds = DateTimeUtil.toEpochSecond(data);
        int nanos = data.getNano();
        long value = (epochSeconds * NANOS_IN_SECOND + nanos) / POW_10[MAX_SCALA - scale];
        serializer.writeLong(value);
    }

    @Override
    public ZonedDateTime deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        long value = deserializer.readLong() * POW_10[MAX_SCALA - scale];
        long epochSeconds = value / NANOS_IN_SECOND;
        int nanos = (int) (value % NANOS_IN_SECOND);

        return DateTimeUtil.toZonedDateTime(epochSeconds, nanos, tz);
    }
}
