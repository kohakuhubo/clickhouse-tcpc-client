package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinarySerializer;
import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;

import java.io.IOException;

public class HelloRequest implements Request {

    private final String clientName;
    private final long clientReversion;
    private final String defaultDatabase;
    private final String clientUsername;
    private final String clientPassword;

    public HelloRequest(String clientName, long clientReversion, String defaultDatabase,
                        String clientUsername, String clientPassword) {
        this.clientName = clientName;
        this.clientReversion = clientReversion;
        this.defaultDatabase = defaultDatabase;
        this.clientUsername = clientUsername;
        this.clientPassword = clientPassword;
    }

    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_HELLO;
    }

    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        serializer.writeUTF8StringBinary(ClickHouseDefines.NAME + " " + clientName);
        serializer.writeVarInt(ClickHouseDefines.MAJOR_VERSION);
        serializer.writeVarInt(ClickHouseDefines.MINOR_VERSION);
        serializer.writeVarInt(clientReversion);
        serializer.writeUTF8StringBinary(defaultDatabase);
        serializer.writeUTF8StringBinary(clientUsername);
        serializer.writeUTF8StringBinary(clientPassword);
    }
}
