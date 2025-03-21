package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataResponse implements Response {

    public static DataResponse readFrom(
            BinaryDeserializer deserializer, NativeContext.ServerContext info, boolean serialize, Block block) throws IOException, SQLException {

        String name = deserializer.readUTF8StringBinary();

        deserializer.maybeEnableCompressed();
        Block newBlock;
        if (!serialize && null != block) {
            newBlock = Block.readFrom(deserializer, block);
        } else {
            newBlock = Block.readFrom(deserializer, info, serialize);
        }
        deserializer.maybeDisableCompressed();
        return new DataResponse(name, newBlock);
    }

    private final String name;

    private final Block block;

    public DataResponse(String name, Block block) {
        this.name = name;
        this.block = block;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_DATA;
    }

    public String name() {
        return name;
    }

    public Block block() {
        return block;
    }
}
