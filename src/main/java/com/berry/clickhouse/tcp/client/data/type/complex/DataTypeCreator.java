package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;

import java.sql.SQLException;

@FunctionalInterface
public interface DataTypeCreator<CK> {

    IDataType<CK> createDataType(SQLLexer lexer, NativeContext.ServerContext serverContext) throws SQLException;
}
