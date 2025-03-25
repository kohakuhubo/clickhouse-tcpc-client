package com.berry.clickhouse.tcp.client.settings;

/**
 * ClickHouseDefines类定义了ClickHouse的常量
 * 包括版本信息、默认值和其他配置
 */
public class ClickHouseDefines {
    public static final String NAME = "ClickHouse"; // ClickHouse名称
    public static final String DEFAULT_CATALOG = "default"; // 默认目录
    public static final String DEFAULT_DATABASE = "default"; // 默认数据库

    public static final int MAJOR_VERSION = 1; // 主版本号
    public static final int MINOR_VERSION = 1; // 次版本号
    public static final int CLIENT_REVISION = 54380; // 客户端修订号
    public static final int DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058; // 最小DBMS修订号
    public static final int DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372; // 最小DBMS修订号

    public static final int COMPRESSION_HEADER_LENGTH = 9; // 压缩头长度
    public static final int CHECKSUM_LENGTH = 16; // 校验和长度

    public static boolean COMPRESSION = false; // 是否启用压缩

    public static int SOCKET_SEND_BUFFER_BYTES = 1024 * 1024; // 套接字发送缓冲区大小
    public static int SOCKET_RECV_BUFFER_BYTES = 1024 * 1024; // 套接字接收缓冲区大小

    public static int MAX_BLOCK_BYTES = 10 * 1024 * 1024; // 最大块字节数
    public static int COLUMN_BUFFER_BYTES = 1024 * 1024; // 列缓冲区字节数

    public static int DATA_TYPE_CACHE_SIZE = 1024; // 数据类型缓存大小
}
