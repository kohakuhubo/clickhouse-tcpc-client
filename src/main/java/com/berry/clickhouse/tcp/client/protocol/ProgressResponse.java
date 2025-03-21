package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;

public class ProgressResponse implements Response {

    public static ProgressResponse readFrom(BinaryDeserializer deserializer) throws IOException {
        return new ProgressResponse(
                deserializer.readVarInt(),
                deserializer.readVarInt(),
                deserializer.readVarInt()
        );
    }

    private final long newRows;
    private final long newBytes;
    private final long newTotalRows;

    public ProgressResponse(long newRows, long newBytes, long newTotalRows) {
        this.newRows = newRows;
        this.newBytes = newBytes;
        this.newTotalRows = newTotalRows;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_PROGRESS;
    }

    public long newRows() {
        return newRows;
    }

    public long newBytes() {
        return newBytes;
    }

    public long newTotalRows() {
        return newTotalRows;
    }

    @Override
    public String toString() {
        return "ProgressResponse {" +
                "newRows=" + newRows +
                ", newBytes=" + newBytes +
                ", newTotalRows=" + newTotalRows +
                '}';
    }
}
