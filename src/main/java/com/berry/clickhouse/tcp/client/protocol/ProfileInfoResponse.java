package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;

public class ProfileInfoResponse implements Response {

    public static ProfileInfoResponse readFrom(BinaryDeserializer deserializer) throws IOException {
        long rows = deserializer.readVarInt();
        long blocks = deserializer.readVarInt();
        long bytes = deserializer.readVarInt();
        long appliedLimit = deserializer.readVarInt();
        long rowsBeforeLimit = deserializer.readVarInt();
        boolean calculatedRowsBeforeLimit = deserializer.readBoolean();
        return new ProfileInfoResponse(rows, blocks, bytes, appliedLimit, rowsBeforeLimit, calculatedRowsBeforeLimit);
    }

    private final long rows;
    private final long blocks;
    private final long bytes;
    private final long appliedLimit;
    private final long rowsBeforeLimit;
    private final boolean calculatedRowsBeforeLimit;

    public ProfileInfoResponse(long rows, long blocks, long bytes,
                               long appliedLimit, long rowsBeforeLimit, boolean calculatedRowsBeforeLimit) {
        this.rows = rows;
        this.blocks = blocks;
        this.bytes = bytes;
        this.appliedLimit = appliedLimit;
        this.rowsBeforeLimit = rowsBeforeLimit;
        this.calculatedRowsBeforeLimit = calculatedRowsBeforeLimit;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_PROFILE_INFO;
    }

    public long rows() {
        return rows;
    }

    public long blocks() {
        return blocks;
    }

    public long bytes() {
        return bytes;
    }

    public long appliedLimit() {
        return appliedLimit;
    }

    public long rowsBeforeLimit() {
        return rowsBeforeLimit;
    }

    public boolean calculatedRowsBeforeLimit() {
        return calculatedRowsBeforeLimit;
    }
}
