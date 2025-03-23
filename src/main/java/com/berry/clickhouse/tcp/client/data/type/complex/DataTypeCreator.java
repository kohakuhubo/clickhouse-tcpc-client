/**
 * ClickHouse数据类型创建器接口
 * 用于根据SQL词法分析结果动态创建特定的数据类型实例
 * 支持复杂类型及其参数的解析和创建，如Array(T)、Tuple(T1,T2)等
 */
package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;

import java.sql.SQLException;

/**
 * 数据类型创建器接口
 * 提供一个函数式接口，用于解析SQL语句中的数据类型定义并创建对应的数据类型实例
 * 每种复杂数据类型都需要实现自己的创建器，用于解析其特有的参数格式
 *
 * @param <CK> 数据类型对应的Java类型
 */
@FunctionalInterface
public interface DataTypeCreator<CK> {

    /**
     * 创建数据类型实例
     * 根据SQL词法分析器的当前状态，解析数据类型定义并创建对应的数据类型实例
     *
     * @param lexer SQL词法分析器，提供输入的解析位置
     * @param serverContext 服务器上下文，包含服务器配置信息如时区等
     * @return 创建的数据类型实例
     * @throws SQLException 如果解析过程中发生错误
     */
    IDataType<CK> createDataType(SQLLexer lexer, NativeContext.ServerContext serverContext) throws SQLException;
}
