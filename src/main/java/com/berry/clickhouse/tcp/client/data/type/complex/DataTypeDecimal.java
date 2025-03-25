package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.BytesHelper;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLException;

import java.util.Locale;

public class DataTypeDecimal implements IDataType<BigDecimal>, BytesHelper {

    public static DataTypeCreator<BigDecimal> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        Number precision = lexer.numberLiteral();
        Validate.isTrue(lexer.character() == ',');
        Number scale = lexer.numberLiteral();
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeDecimal("Decimal(" + precision.intValue() + "," + scale.intValue() + ")",
                precision.intValue(), scale.intValue());
    };

    private final String name;
    private final int precision;
    private final int scale;
    private final BigDecimal scaleFactor;
    private final int nobits;
    private final int byteSize;

    // see https://clickhouse.tech/docs/en/sql-reference/data-types/decimal/
    public DataTypeDecimal(String name, int precision, int scale) {
        this.name = name;
        this.precision = precision;
        this.scale = scale;
        this.scaleFactor = BigDecimal.valueOf(Math.pow(10, scale));
        if (this.precision <= 9) {
            this.nobits = 32;
            this.byteSize = 4;
        } else if (this.precision <= 18) {
            this.nobits = 64;
            this.byteSize = 8;
        } else if (this.precision <= 38) {
            this.nobits = 128;
            this.byteSize = 16;
        } else if (this.precision <= 76) {
            this.nobits = 256;
            this.byteSize = 32;
        } else {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Precision[%d] is out of boundary.", precision));
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public int byteSize() {
        return this.byteSize;
    }

    @Override
    public BigDecimal defaultValue() {
        return BigDecimal.ZERO;
    }

    @Override
    public Class<BigDecimal> javaType() {
        return BigDecimal.class;
    }

    @Override
    public void serializeBinary(BigDecimal data, BinarySerializer serializer) throws IOException {
        BigDecimal targetValue = data.multiply(scaleFactor);
        switch (this.nobits) {
            case 32: {
                serializer.writeInt(targetValue.intValue());
                break;
            }
            case 64: {
                serializer.writeLong(targetValue.longValue());
                break;
            }
            case 128: {
                BigInteger res = targetValue.toBigInteger();
                serializer.writeLong(res.longValue());
                serializer.writeLong(res.shiftRight(64).longValue());
                break;
            }
            case 256: {
                BigInteger res = targetValue.toBigInteger();
                serializer.writeLong(targetValue.longValue());
                serializer.writeLong(res.shiftRight(64).longValue());
                serializer.writeLong(res.shiftRight(64 * 2).longValue());
                serializer.writeLong(res.shiftRight(64 * 3).longValue());
                break;
            }
            default: {
                throw new RuntimeException(String.format(Locale.ENGLISH,
                        "Unknown precision[%d] & scale[%d]", precision, scale));
            }
        }
    }

    @Override
    public BigDecimal deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        BigDecimal value;
        switch (this.nobits) {
            case 32: {
                int v = deserializer.readInt();
                value = BigDecimal.valueOf(v);
                value = value.divide(scaleFactor, scale, RoundingMode.HALF_UP);
                break;
            }
            case 64: {
                long v = deserializer.readLong();
                value = BigDecimal.valueOf(v);
                value = value.divide(scaleFactor, scale, RoundingMode.HALF_UP);
                break;
            }

            case 128: {
                long []array = new long[2];
                array[1] = deserializer.readLong();
                array[0] = deserializer.readLong();

                value = new BigDecimal(new BigInteger(getBytes(array)));
                value = value.divide(scaleFactor, scale, RoundingMode.HALF_UP);
                break;
            }

            case 256: {
                long []array = new long[4];
                array[3] = deserializer.readLong();
                array[2] = deserializer.readLong();
                array[1] = deserializer.readLong();
                array[0] = deserializer.readLong();

                value = new BigDecimal(new BigInteger(getBytes(array)));
                value = value.divide(scaleFactor, scale, RoundingMode.HALF_UP);
                break;
            }

            default: {
                throw new RuntimeException(String.format(Locale.ENGLISH,
                        "Unknown precision[%d] & scale[%d]", precision, scale));
            }
        }
        return value;
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
