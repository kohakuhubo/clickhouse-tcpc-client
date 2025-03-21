package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.data.Block;

import java.sql.SQLException;

public interface NativeInputFormat extends InputFormat<Block, SQLException> {

    @Override
    default String name() {
        return "Native";
    }

    void fill(Block block) throws SQLException;
}
