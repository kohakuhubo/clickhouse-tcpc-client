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

public class ClickHouseClient {

    private static final String GET_SAMPLE_BLOCK_SQL = "insert into %s values ";

    private final GenericObjectPool<ClickHouseConnection> pool;

    private final NativeContext.ServerContext serverContext;

    private final ClickHouseTableMetaDataManager metaDataManager;

    private ClickHouseClient(GenericObjectPool<ClickHouseConnection> pool, NativeContext.ServerContext serverContext,
                             ClickHouseTableMetaDataManager metaDataManager) {
        this.pool = pool;
        this.serverContext = serverContext;
        this.metaDataManager = metaDataManager;
    }

    public static final class Builder {
        private GenericObjectPool<ClickHouseConnection> pool;
        private NativeContext.ServerContext serverContext;
        private ClickHouseTableMetaDataManager metaDataManager;
        private ClickHouseConfig clickHouseConfig;

        public Builder() {
            this.metaDataManager = new ClickHouseTableMetaDataManager();
        }

        public Builder addMetaData(ClickHouseTableMetaData metaData) {
            this.metaDataManager.register(metaData);
            return this;
        }

        public Builder clickHouseConfig(ClickHouseConfig val) {
            this.clickHouseConfig = val;
            return this;
        }

        public ClickHouseClient build() throws Exception {
            ClickHouseConnection clickHouseConnection = ClickHouseConnection.createClickHouseConnection(clickHouseConfig);
            if (clickHouseConnection.ping(clickHouseConfig.connectTimeout())) {
                throw new Exception();
            }
            ColumnWriterBufferFactory.getInstance(clickHouseConfig);
            serverContext = clickHouseConnection.serverContext();

            GenericObjectPoolConfig<ClickHouseConnection> genericObjectPoolConfig = new GenericObjectPoolConfig<>();
            genericObjectPoolConfig.setMaxIdle(clickHouseConfig.getConnectionPoolMaxIdle());
            genericObjectPoolConfig.setMinIdle(clickHouseConfig.getConnectionPoolMinIdle());
            genericObjectPoolConfig.setMaxTotal(clickHouseConfig.getConnectionPoolTotal());

            pool = new GenericObjectPool<>(new ClickHouseConnectionFactory(clickHouseConfig), genericObjectPoolConfig);
            pool.use(clickHouseConnection);
            return new ClickHouseClient(this.pool, this.serverContext, this.metaDataManager);
        }
    }

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

    public Block createBlock(String tableName) throws Exception {
        ClickHouseTableMetaData clickHouseTableMetaData = this.metaDataManager.getTableMetaData(tableName);
        if (null != clickHouseTableMetaData) {
            return Block.createFrom(clickHouseTableMetaData, this.serverContext);
        } else {
            Block block = getSampleBlock(tableName);
            block.initWriteBuffer();
            return block;
        }
    }

    public BlockResultSet createBlockResultSet(Block block, MappedByteBuffer buffer
            , boolean enableCompress, boolean reserveDiffType, Set<String> exclude, Set<String> serializeCols) {
        return new BlockResultSet(buffer, enableCompress, block, reserveDiffType, exclude, serializeCols);
    }

    public BlockResultSet createBlockResultSet(String tableName, MappedByteBuffer buffer
            , boolean enableCompress, boolean reserveDiffType, Set<String> exclude, Set<String> serializeCols) throws Exception {
        ClickHouseTableMetaData metaData = this.metaDataManager.getTableMetaData(tableName);
        if (null == metaData) {
            throw new Exception();
        }
        return new BlockResultSet(buffer, enableCompress, Block.createFrom(metaData, serverContext),
                reserveDiffType, exclude, serializeCols);
    }

    private ClickHouseConnection acquireConnection() throws Exception {
        if (null == pool)
            throw new Exception();
        return pool.borrowObject();
    }

    private void returnConnection(ClickHouseConnection connection) throws Exception {
        if (null == pool)
            throw new Exception();
        pool.returnObject(connection);
    }

    private QueryResult query(String query) throws SQLException {
        return query(query);
    }

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

    public void insert(Block block) throws Exception {
        insert(block, true);
    }

    public void insert(Block block, boolean clean) throws Exception {
        ClickHouseConnection clickHouseConnection = null;
        boolean result = false;
        try {
            clickHouseConnection = acquireConnection();
            clickHouseConnection.sendInsertRequest(block);
            result = true;
        } finally {
            if (clean && result) {
                block.cleanup();
            }
            if (null != clickHouseConnection) {
                returnConnection(clickHouseConnection);
            }
        }
    }

    public void cleanBlock(Block block) {
        block.cleanup();
    }

    public void close() throws Exception {
        if (null == pool)
            throw new Exception();
        pool.close();
    }

}
