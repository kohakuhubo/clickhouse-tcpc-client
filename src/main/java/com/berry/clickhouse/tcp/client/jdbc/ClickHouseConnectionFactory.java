package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.settings.ClickHouseClientConfig;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * ClickHouseConnectionFactory类用于创建和管理ClickHouseConnection的池
 * 继承自BasePooledObjectFactory以支持对象池的功能
 */
public class ClickHouseConnectionFactory extends BasePooledObjectFactory<ClickHouseConnection> {

    private final ClickHouseClientConfig config; // 连接配置

    /**
     * 构造函数，初始化连接工厂
     * 
     * @param config ClickHouse配置
     */
    public ClickHouseConnectionFactory(ClickHouseClientConfig config) {
        this.config = config;
    }

    /**
     * 创建新的ClickHouseConnection实例
     * 
     * @return 新创建的ClickHouseConnection实例
     * @throws Exception 如果创建连接时发生错误
     */
    @Override
    public ClickHouseConnection create() throws Exception {
        return ClickHouseConnection.createClickHouseConnection(this.config);
    }

    /**
     * 包装ClickHouseConnection实例为PooledObject
     * 
     * @param clickHouseConnection ClickHouseConnection实例
     * @return PooledObject实例
     */
    @Override
    public PooledObject<ClickHouseConnection> wrap(ClickHouseConnection clickHouseConnection) {
        return new DefaultPooledObject<>(clickHouseConnection);
    }

    /**
     * 销毁PooledObject
     * 
     * @param p PooledObject实例
     * @param destroyMode 销毁模式
     * @throws Exception 如果销毁时发生错误
     */
    @Override
    public void destroyObject(PooledObject<ClickHouseConnection> p, DestroyMode destroyMode) throws Exception {
        p.getObject().close(); // 关闭连接
        super.destroyObject(p, destroyMode);
    }
}
