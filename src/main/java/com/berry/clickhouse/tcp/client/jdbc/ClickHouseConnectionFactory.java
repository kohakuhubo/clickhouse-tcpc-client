package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.settings.ClickHouseConfig;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class ClickHouseConnectionFactory extends BasePooledObjectFactory<ClickHouseConnection> {

    private final ClickHouseConfig config;

    public ClickHouseConnectionFactory(ClickHouseConfig config) {
        this.config = config;
    }

    @Override
    public ClickHouseConnection create() throws Exception {
        return ClickHouseConnection.createClickHouseConnection(this.config);
    }

    @Override
    public PooledObject<ClickHouseConnection> wrap(ClickHouseConnection clickHouseConnection) {
        return new DefaultPooledObject<>(clickHouseConnection);
    }

    @Override
    public void destroyObject(PooledObject<ClickHouseConnection> p, DestroyMode destroyMode) throws Exception {
        p.getObject().close();
        super.destroyObject(p, destroyMode);
    }
}
