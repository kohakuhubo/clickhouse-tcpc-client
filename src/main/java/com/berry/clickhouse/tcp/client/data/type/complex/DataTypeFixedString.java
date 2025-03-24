package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.BytesCharSeq;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

/**
 * ClickHouse FixedString 数据类型的实现
 * 用于处理固定长度的字符串数据类型，对应 ClickHouse 中的 FixedString(N)
 *
 * 特点：
 * 1. 固定长度字符串，长度在创建表时指定
 * 2. 如果字符串长度小于指定长度，将用零字节填充
 * 3. 如果字符串长度大于指定长度，将抛出异常
 *
 * 使用场景：
 * - 适用于存储固定长度的字符串，如 UUID、固定格式的代码等
 * - 相比可变长度的 String 类型，在某些场景下具有更好的性能
 */
public class DataTypeFixedString implements IDataType<CharSequence> {

    /**
     * 创建 FixedString 数据类型的工厂方法
     * 用于解析 FixedString(N) 格式的类型定义并创建相应的实例
     */
    public static DataTypeCreator<CharSequence> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        Number fixedStringN = lexer.numberLiteral();
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeFixedString("FixedString(" + fixedStringN.intValue() + ")", fixedStringN.intValue(), serverContext);
    };

    /**
     * 固定字符串的长度
     */
    private final int n;
    
    /**
     * 数据类型的完整名称，格式为 FixedString(N)
     */
    private final String name;
    
    /**
     * 该类型的默认值，为指定长度的零字节字符串
     */
    private final String defaultValue;
    
    /**
     * 用于字符串编解码的字符集
     */
    private final Charset charset;

    /**
     * 构造函数
     *
     * @param name 类型名称
     * @param n 固定字符串长度
     * @param serverContext 服务器上下文，用于获取配置信息
     */
    public DataTypeFixedString(String name, int n, NativeContext.ServerContext serverContext) {
        this.n = n;
        this.name = name;
        this.charset = serverContext.getConfigure().charset();

        byte[] data = new byte[n];
        for (int i = 0; i < n; i++) {
            data[i] = '\u0000';
        }
        this.defaultValue = new String(data, charset);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public Class<CharSequence> javaType() {
        return CharSequence.class;
    }

    /**
     * 将数据序列化为二进制格式
     * 支持 BytesCharSeq 和普通 CharSequence 两种输入类型
     *
     * @param data 要序列化的数据
     * @param serializer 二进制序列化器
     * @throws SQLException 当输入字符串长度超过固定长度时抛出
     * @throws IOException 序列化过程中发生 IO 错误时抛出
     */
    @Override
    public void serializeBinary(CharSequence data, BinarySerializer serializer) throws SQLException, IOException {
        if (data instanceof BytesCharSeq) {
            writeBytes((((BytesCharSeq) data).bytes()), serializer);
        } else {
            writeBytes(data.toString().getBytes(charset), serializer);
        }
    }

    /**
     * 将字节数组写入序列化器
     * 如果输入字节数组长度小于固定长度，将自动补零
     *
     * @param bs 要写入的字节数组
     * @param serializer 二进制序列化器
     * @throws IOException 写入过程中发生 IO 错误时抛出
     * @throws SQLException 输入字节数组长度超过固定长度时抛出
     */
    private void writeBytes(byte[] bs, BinarySerializer serializer) throws IOException, SQLException {
        byte[] res;
        if (bs.length > n) {
            throw new SQLException("The size of FixString column is too large, got " + bs.length);
        }
        if (bs.length == n) {
            res = bs;
        } else {
            res = new byte[n];
            System.arraycopy(bs, 0, res, 0, bs.length);
        }
        serializer.writeBytes(res);
    }

    /**
     * 从二进制格式反序列化数据
     *
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的字符串
     * @throws SQLException 反序列化过程中发生错误时抛出
     * @throws IOException 读取过程中发生 IO 错误时抛出
     */
    @Override
    public String deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return new String(deserializer.readBytes(n), charset);
    }
}
