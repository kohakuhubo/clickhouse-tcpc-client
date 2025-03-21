package com.berry.clickhouse.tcp.client.settings;

public class ClickHouseDefines {
    public static final String NAME = "ClickHouse";
    public static final String DEFAULT_CATALOG = "default";
    public static final String DEFAULT_DATABASE = "default";

    public static final int MAJOR_VERSION = 1;
    public static final int MINOR_VERSION = 1;
    public static final int CLIENT_REVISION = 54380;
    public static final int DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058;
    public static final int DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372;

    public static final int COMPRESSION_HEADER_LENGTH = 9;
    public static final int CHECKSUM_LENGTH = 16;

    public static boolean COMPRESSION = true;

    public static int SOCKET_SEND_BUFFER_BYTES = 1024 * 1024;
    public static int SOCKET_RECV_BUFFER_BYTES = 1024 * 1024;

    public static int MAX_BLOCK_BYTES = 10 * 1024 * 1024;
    public static int COLUMN_BUFFER_BYTES = 1024 * 1024;

    public static int DATA_TYPE_CACHE_SIZE = 1024;
}
