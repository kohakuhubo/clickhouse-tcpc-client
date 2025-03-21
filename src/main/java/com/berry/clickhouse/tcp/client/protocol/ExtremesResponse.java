package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public class ExtremesResponse implements Response {

    public static ExtremesResponse readFrom(
            BinaryDeserializer deserializer, NativeContext.ServerContext info) throws IOException, SQLException {
        return new ExtremesResponse(deserializer.readUTF8StringBinary(), Block.readFrom(deserializer, info, true));
    }

    private final String name;
    private final Block block;

    public ExtremesResponse(String name, Block block) {
        this.name = name;
        this.block = block;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_EXTREMES;
    }

    public String name() {
        return name;
    }

    public Block block() {
        return block;
    }
}
