package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;

public class PingRequest implements Request {

    public static final PingRequest INSTANCE = new PingRequest();

    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_PING;
    }

    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        // Nothing
    }
}
