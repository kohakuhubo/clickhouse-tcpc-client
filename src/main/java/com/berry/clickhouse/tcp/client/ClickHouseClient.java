/**
 * ClickHouseClient是Clickhouse数据库TCP客户端的主要接口类
 * 提供连接池管理、数据块创建和插入等核心功能
 * 该客户端通过TCP协议与Clickhouse服务器通信，支持高性能数据操作
 */
package com.berry.clickhouse.tcp.client;

import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.data.BlockResultSet;
import com.berry.clickhouse.tcp.client.data.ColumnWriterBufferFactory;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseConnection;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseConnectionFactory;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseTableMetaData;
import com.berry.clickhouse.tcp.client.meta.ClickHouseTableMetaDataManager;
import com.berry.clickhouse.tcp.client.settings.ClickHouseConfig;
import com.berry.clickhouse.tcp.client.stream.QueryResult;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.nio.MappedByteBuffer;
import java.sql.SQLException;
import java.util.Set;

/**
 * ClickHouse TCP客户端的主要实现类
 * 负责管理ClickHouse连接池并提供数据操作接口
 * 支持创建数据块、执行查询和插入操作
 */
public class ClickHouseClient {

    /**
     * 获取表结构信息的SQL模板
     */
    private static final String GET_SAMPLE_BLOCK_SQL = "insert into %s values ";

    /**
     * ClickHouse连接池
     */
    private final GenericObjectPool<ClickHouseConnection> pool;

    /**
     * 服务器上下文信息
     */
    private final NativeContext.ServerContext serverContext;

    /**
     * 表元数据管理器
     */
    private final ClickHouseTableMetaDataManager metaDataManager;

    /**
     * 私有构造方法，通过Builder模式创建实例
     * 
     * @param pool ClickHouse连接池
     * @param serverContext 服务器上下文信息
     * @param metaDataManager 表元数据管理器
     */
    private ClickHouseClient(GenericObjectPool<ClickHouseConnection> pool, NativeContext.ServerContext serverContext,
                             ClickHouseTableMetaDataManager metaDataManager) {
        this.pool = pool;
        this.serverContext = serverContext;
        this.metaDataManager = metaDataManager;
    }

    /**
     * ClickHouseClient的构建器类
     * 使用Builder设计模式创建ClickHouseClient实例
     */
    public static final class Builder {
        /**
         * ClickHouse连接池
         */
        private GenericObjectPool<ClickHouseConnection> pool;
        
        /**
         * 服务器上下文信息
         */
        private NativeContext.ServerContext serverContext;
        
        /**
         * 表元数据管理器
         */
        private ClickHouseTableMetaDataManager metaDataManager;
        
        /**
         * ClickHouse配置信息
         */
        private ClickHouseConfig clickHouseConfig;

        /**
         * 构造函数，初始化表元数据管理器
         */
        public Builder() {
            this.metaDataManager = new ClickHouseTableMetaDataManager();
        }

        /**
         * 添加表元数据信息
         * 
         * @param metaData 表元数据
         * @return Builder实例，支持链式调用
         */
        public Builder addMetaData(ClickHouseTableMetaData metaData) {
            this.metaDataManager.register(metaData);
            return this;
        }

        /**
         * 设置ClickHouse配置信息
         * 
         * @param val ClickHouse配置
         * @return Builder实例，支持链式调用
         */
        public Builder clickHouseConfig(ClickHouseConfig val) {
            this.clickHouseConfig = val;
            return this;
        }

        /**
         * 构建ClickHouseClient实例
         * 
         * @return 新创建的ClickHouseClient实例
         * @throws Exception 如果连接失败或配置无效
         */
        public ClickHouseClient build() throws Exception {
            // 创建连接并验证连接有效性
            ClickHouseConnection clickHouseConnection = ClickHouseConnection.createClickHouseConnection(clickHouseConfig);
            if (!clickHouseConnection.ping(clickHouseConfig.connectTimeout())) {
                throw new Exception();
            }
            
            // 初始化列写入缓冲工厂
            ColumnWriterBufferFactory.getInstance(clickHouseConfig);
            serverContext = clickHouseConnection.serverContext();

            // 配置连接池参数
            GenericObjectPoolConfig<ClickHouseConnection> genericObjectPoolConfig = new GenericObjectPoolConfig<>();
            genericObjectPoolConfig.setMaxIdle(clickHouseConfig.getConnectionPoolMaxIdle());
            genericObjectPoolConfig.setMinIdle(clickHouseConfig.getConnectionPoolMinIdle());
            genericObjectPoolConfig.setMaxTotal(clickHouseConfig.getConnectionPoolTotal());

            // 创建连接池
            pool = new GenericObjectPool<>(new ClickHouseConnectionFactory(clickHouseConfig), genericObjectPoolConfig);
            pool.use(clickHouseConnection);
            return new ClickHouseClient(this.pool, this.serverContext, this.metaDataManager);
        }
    }

    /**
     * 获取表的样本数据块，用于了解表结构
     * 
     * @param tableName 表名
     * @return 表结构的样本数据块
     * @throws Exception 如果获取失败
     */
    private Block getSampleBlock(String tableName) throws Exception {
        ClickHouseConnection clickHouseConnection = null;
        try {
            clickHouseConnection = acquireConnection();
            return clickHouseConnection.getSampleBlock(String.format(GET_SAMPLE_BLOCK_SQL, tableName));
        } finally {
            if (null != clickHouseConnection) {
                returnConnection(clickHouseConnection);
            }
        }
    }

    /**
     * 为指定表创建一个空的数据块
     * 
     * @param tableName 表名
     * @return 为指定表创建的空数据块
     * @throws Exception 如果创建失败
     */
    public Block createBlock(String tableName) throws Exception {
        // 首先尝试从元数据管理器获取表结构
        ClickHouseTableMetaData clickHouseTableMetaData = this.metaDataManager.getTableMetaData(tableName);
        if (null != clickHouseTableMetaData) {
            return Block.createFrom(clickHouseTableMetaData, this.serverContext);
        } else {
            // 如果元数据不存在，则通过查询获取样本数据块
            Block block = getSampleBlock(tableName);
            block.initWriteBuffer();
            return block;
        }
    }

    /**
     * 创建数据块结果集
     * 
     * @param block 数据块
     * @param buffer 映射字节缓冲区
     * @param enableCompress 是否启用压缩
     * @param reserveDiffType 是否保留不同类型
     * @param exclude 要排除的列名集合
     * @param serializeCols 需要序列化的列名集合
     * @return 数据块结果集
     */
    public BlockResultSet createBlockResultSet(Block block, MappedByteBuffer buffer
            , boolean enableCompress, boolean reserveDiffType, Set<String> exclude, Set<String> serializeCols) {
        return new BlockResultSet(buffer, enableCompress, block, reserveDiffType, exclude, serializeCols);
    }

    /**
     * 创建数据块结果集，通过表名
     * 
     * @param tableName 表名
     * @param buffer 映射字节缓冲区
     * @param enableCompress 是否启用压缩
     * @param reserveDiffType 是否保留不同类型
     * @param exclude 要排除的列名集合
     * @param serializeCols 需要序列化的列名集合
     * @return 数据块结果集
     * @throws Exception 如果创建失败
     */
    public BlockResultSet createBlockResultSet(String tableName, MappedByteBuffer buffer
            , boolean enableCompress, boolean reserveDiffType, Set<String> exclude, Set<String> serializeCols) throws Exception {
        ClickHouseTableMetaData metaData = this.metaDataManager.getTableMetaData(tableName);
        if (null == metaData) {
            throw new Exception();
        }
        return new BlockResultSet(buffer, enableCompress, Block.createFrom(metaData, serverContext),
                reserveDiffType, exclude, serializeCols);
    }

    /**
     * 从连接池获取一个连接
     * 
     * @return ClickHouse连接
     * @throws Exception 如果获取连接失败
     */
    private ClickHouseConnection acquireConnection() throws Exception {
        if (null == pool)
            throw new Exception();
        return pool.borrowObject();
    }

    /**
     * 将连接归还到连接池
     * 
     * @param connection 要归还的连接
     * @throws Exception 如果归还连接失败
     */
    private void returnConnection(ClickHouseConnection connection) throws Exception {
        if (null == pool)
            throw new Exception();
        pool.returnObject(connection);
    }

    /**
     * 执行查询
     * 
     * @param query SQL查询语句
     * @return 查询结果
     * @throws SQLException 如果查询执行失败
     */
    private QueryResult query(String query) throws SQLException {
        return query(query);
    }

    /**
     * 执行查询，可指定是否序列化结果
     * 
     * @param query SQL查询语句
     * @param serialize 是否序列化结果
     * @return 查询结果
     * @throws Exception 如果查询执行失败
     */
    private QueryResult query(String query, boolean serialize) throws Exception {
        ClickHouseConnection clickHouseConnection = null;
        try {
            clickHouseConnection = acquireConnection();
            return clickHouseConnection.sendQueryRequest(query, clickHouseConnection.cfg(), false, serialize);
        } finally {
            if (null != clickHouseConnection) {
                returnConnection(clickHouseConnection);
            }
        }
    }

    /**
     * 插入数据块
     * 
     * @param block 要插入的数据块
     * @throws Exception 如果插入失败
     */
    public void insert(Block block) throws Exception {
        insert(block, true);
    }

    /**
     * 插入数据块，可指定是否清理数据块
     * 
     * @param block 要插入的数据块
     * @param clean 是否在插入后清理数据块
     * @throws Exception 如果插入失败
     */
    public void insert(Block block, boolean clean) throws Exception {
        ClickHouseConnection clickHouseConnection = null;
        boolean result = false;
        try {
            clickHouseConnection = acquireConnection();
            clickHouseConnection.sendInsertRequest(block);
            result = true;
        } finally {
            // 如果需要清理且操作成功，则清理数据块
            if (clean && result) {
                block.cleanup();
            }
            if (null != clickHouseConnection) {
                returnConnection(clickHouseConnection);
            }
        }
    }

    /**
     * 清理数据块资源
     * 
     * @param block 要清理的数据块
     */
    public void cleanBlock(Block block) {
        block.cleanup();
    }

    /**
     * 关闭客户端，释放连接池资源
     * 
     * @throws Exception 如果关闭失败
     */
    public void close() throws Exception {
        if (null == pool)
            throw new Exception();
        pool.close();
    }

}
