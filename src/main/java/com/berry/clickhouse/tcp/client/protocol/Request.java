package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public interface Request {

    ProtoType type();

    void writeImpl(BinarySerializer serializer) throws IOException, SQLException;

    default void writeTo(BinarySerializer serializer) throws IOException, SQLException {
        serializer.writeVarInt(type().id());
        this.writeImpl(serializer);
    }

    enum ProtoType {
        REQUEST_HELLO(0),
        REQUEST_QUERY(1),
        REQUEST_DATA(2),
        REQUEST_PING(4);

        private final int id;

        ProtoType(int id) {
            this.id = id;
        }

        public long id() {
            return id;
        }
    }
}
