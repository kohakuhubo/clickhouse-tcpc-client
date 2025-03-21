package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

public class EOFStreamResponse implements Response {

    public static final EOFStreamResponse INSTANCE = new EOFStreamResponse();

    public static Response readFrom(BinaryDeserializer deserializer) {
        return INSTANCE;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_END_OF_STREAM;
    }
}
