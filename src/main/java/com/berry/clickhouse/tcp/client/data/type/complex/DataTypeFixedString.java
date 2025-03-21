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


public class DataTypeFixedString implements IDataType<CharSequence> {

    public static DataTypeCreator<CharSequence> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        Number fixedStringN = lexer.numberLiteral();
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeFixedString("FixedString(" + fixedStringN.intValue() + ")", fixedStringN.intValue(), serverContext);
    };

    private final int n;
    private final String name;
    private final String defaultValue;
    private final Charset charset;

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

    @Override
    public void serializeBinary(CharSequence data, BinarySerializer serializer) throws SQLException, IOException {
        if (data instanceof BytesCharSeq) {
            writeBytes((((BytesCharSeq) data).bytes()), serializer);
        } else {
            writeBytes(data.toString().getBytes(charset), serializer);
        }
    }

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

    @Override
    public String deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return new String(deserializer.readBytes(n), charset);
    }

    @Override
    public CharSequence deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.stringLiteral();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"BINARY"};
    }
}
