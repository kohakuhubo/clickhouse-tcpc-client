/**
 * ClickHouse DateTime64数据类型的实现
 * 用于处理带有亚秒精度的日期时间数据
 * 支持0-9级精度(scale)，表示小数点后的位数
 * 默认以时区信息存储，对应Java中的ZonedDateTime
 */
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

/**
 * DateTime64数据类型实现
 * 处理ClickHouse中的DateTime64类型，支持亚秒精度的日期时间，对应Java中的ZonedDateTime
 * 格式：DateTime64(scale, [timezone])，其中scale为精度(0-9)，timezone为可选的时区
 */
public class DataTypeDateTime64 implements IDataType<ZonedDateTime> {

    /**
     * DateTime64类型创建器
     * 用于根据词法分析结果创建DataTypeDateTime64实例
     * 支持三种格式：
     * 1. DateTime64 - 使用默认精度和时区
     * 2. DateTime64(scale) - 指定精度，使用默认时区
     * 3. DateTime64(scale, 'timezone') - 同时指定精度和时区
     */
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

    /**
     * EPOCH起始时间(1970-01-01 00:00:00)的LocalDateTime对象
     */
    private static final LocalDateTime EPOCH_LOCAL_DT = LocalDateTime.of(1970, 1, 1, 0, 0);
    
    /**
     * 一秒中的纳秒数: 1,000,000,000
     */
    public static final int NANOS_IN_SECOND = 1_000_000_000;
    
    /**
     * 一秒中的毫秒数: 1,000
     */
    public static final int MILLIS_IN_SECOND = 1000;
    
    /**
     * 10的幂数组，用于处理不同精度的转换
     */
    public static final int[] POW_10 = {1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000};
    
    /**
     * 最小支持的精度: 0
     */
    public static final int MIN_SCALE = 0;
    
    /**
     * 最大支持的精度: 9
     */
    public static final int MAX_SCALA = 9;
    
    /**
     * 默认精度: 3 (毫秒级)
     */
    public static final int DEFAULT_SCALE = 3;

    /**
     * 数据类型名称
     */
    private final String name;
    
    /**
     * 精度，表示小数点后的位数
     */
    private final int scale;
    
    /**
     * 时区
     */
    private final ZoneId tz;
    
    /**
     * 默认值: 1970-01-01 00:00:00 (带时区)
     */
    private final ZonedDateTime defaultValue;

    /**
     * 创建DateTime64数据类型
     * 
     * @param name 类型名称，如"DateTime64(3)"
     * @param scala 精度，0-9之间的整数，表示小数点后的位数
     * @param serverContext 服务器上下文，用于获取时区信息
     */
    public DataTypeDateTime64(String name, int scala, ServerContext serverContext) {
        this.name = name;
        this.scale = scala;
        this.tz = DateTimeUtil.chooseTimeZone(serverContext);
        this.defaultValue = EPOCH_LOCAL_DT.atZone(tz);
    }

    /**
     * 获取数据类型名称
     * 
     * @return 数据类型名称，如"DateTime64(3)"或"DateTime64(3, 'UTC')"
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * 获取数据类型默认值
     * 
     * @return EPOCH时间(1970-01-01 00:00:00)，带时区
     */
    @Override
    public ZonedDateTime defaultValue() {
        return defaultValue;
    }

    /**
     * 获取数据类型对应的Java类
     * 
     * @return ZonedDateTime.class
     */
    @Override
    public Class<ZonedDateTime> javaType() {
        return ZonedDateTime.class;
    }

    /**
     * 将ZonedDateTime序列化为二进制格式
     * 将日期时间转换为从EPOCH开始的纳秒数，然后根据精度缩放
     * 
     * @param data 要序列化的ZonedDateTime
     * @param serializer 二进制序列化器
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    @Override
    public void serializeBinary(ZonedDateTime data, BinarySerializer serializer) throws IOException {
        long epochSeconds = DateTimeUtil.toEpochSecond(data);
        int nanos = data.getNano();
        long value = (epochSeconds * NANOS_IN_SECOND + nanos) / POW_10[MAX_SCALA - scale];
        serializer.writeLong(value);
    }

    /**
     * 从二进制流反序列化ZonedDateTime
     * 读取经过精度缩放的值，转换回秒和纳秒，然后构造ZonedDateTime
     * 
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的ZonedDateTime
     * @throws IOException 如果反序列化过程中发生I/O错误
     */
    @Override
    public ZonedDateTime deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        long value = deserializer.readLong() * POW_10[MAX_SCALA - scale];
        long epochSeconds = value / NANOS_IN_SECOND;
        int nanos = (int) (value % NANOS_IN_SECOND);

        return DateTimeUtil.toZonedDateTime(epochSeconds, nanos, tz);
    }
}
