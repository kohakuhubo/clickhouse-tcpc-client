package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public class PongResponse implements Response {

    public static final PongResponse INSTANCE = new PongResponse();

    public static PongResponse readFrom(BinaryDeserializer deserializer) throws IOException, SQLException {
        return INSTANCE;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_PONG;
    }
}
