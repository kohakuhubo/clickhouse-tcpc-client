package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.BytesCharSeq;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;


public class DataTypeString implements IDataType<CharSequence> {

    public static DataTypeCreator<CharSequence> CREATOR = (lexer, serverContext) -> new DataTypeString(serverContext.getConfigure().charset());

    private final Charset charset;

    public DataTypeString(Charset charset) {
        this.charset = charset;
    }

    @Override
    public String name() {
        return "String";
    }

    @Override
    public String defaultValue() {
        return "";
    }

    @Override
    public Class<CharSequence> javaType() {
        return CharSequence.class;
    }

    @Override
    public void serializeBinary(CharSequence data, BinarySerializer serializer) throws SQLException, IOException {
        if (data instanceof BytesCharSeq) {
            serializer.writeBytesBinary(((BytesCharSeq) data).bytes());
        } else {
            serializer.writeStringBinary(data.toString(), charset);
        }
    }

    @Override
    public String deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] bs = deserializer.readBytesBinary();
        return new String(bs, charset);
    }

    @Override
    public CharSequence deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.stringView();
    }

    @Override
    public String[] getAliases() {
        return new String[]{
                "LONGBLOB",
                "MEDIUMBLOB",
                "TINYBLOB",
                "MEDIUMTEXT",
                "CHAR",
                "VARCHAR",
                "TEXT",
                "TINYTEXT",
                "LONGTEXT",
                "BLOB"};
    }

    @Override
    public boolean isFixedLength() {
        return false;
    }
}
