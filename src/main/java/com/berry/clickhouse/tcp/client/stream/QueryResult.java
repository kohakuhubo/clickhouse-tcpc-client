package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.misc.CheckedIterator;
import com.berry.clickhouse.tcp.client.protocol.DataResponse;

import java.sql.SQLException;

public interface QueryResult {

    Block header() throws SQLException;

    CheckedIterator<DataResponse, SQLException> data();
}
