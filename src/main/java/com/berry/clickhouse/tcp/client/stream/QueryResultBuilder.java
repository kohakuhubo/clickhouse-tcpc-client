package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.*;
import com.berry.clickhouse.tcp.client.misc.CheckedIterator;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.protocol.DataResponse;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * QueryResultBuilder类用于构建查询结果
 * 提供方法来设置列名、列类型和添加行数据
 */
public class QueryResultBuilder {

    private final int columnNum; // 列数
    private final NativeContext.ServerContext serverContext; // 服务器上下文
    private List<String> columnNames; // 列名列表
    private List<IDataType> columnTypes; // 列类型列表
    private final List<List<?>> rows = new ArrayList<>(); // 行数据列表

    /**
     * 创建QueryResultBuilder实例
     * 
     * @param columnsNum 列数
     * @param serverContext 服务器上下文
     * @return QueryResultBuilder实例
     */
    public static QueryResultBuilder builder(int columnsNum, NativeContext.ServerContext serverContext) {
        return new QueryResultBuilder(columnsNum, serverContext);
    }

    private QueryResultBuilder(int columnNum, NativeContext.ServerContext serverContext) {
        this.columnNum = columnNum; // 初始化列数
        this.serverContext = serverContext; // 初始化服务器上下文
    }

    /**
     * 设置列名
     * 
     * @param names 列名数组
     * @return QueryResultBuilder实例
     */
    public QueryResultBuilder columnNames(String... names) {
        return columnNames(Arrays.asList(names)); // 转换为列表并设置列名
    }

    /**
     * 设置列名
     * 
     * @param names 列名列表
     * @return QueryResultBuilder实例
     */
    public QueryResultBuilder columnNames(List<String> names) {
        Validate.ensure(names.size() == columnNum, "size mismatch, req: " + columnNum + " got: " + names.size()); // 验证列名数量
        this.columnNames = names; // 设置列名
        return this;
    }

    /**
     * 设置列类型
     * 
     * @param types 列类型数组
     * @return QueryResultBuilder实例
     * @throws SQLException SQL异常
     */
    public QueryResultBuilder columnTypes(String... types) throws SQLException {
        return columnTypes(Arrays.asList(types)); // 转换为列表并设置列类型
    }

    /**
     * 设置列类型
     * 
     * @param types 列类型列表
     * @return QueryResultBuilder实例
     * @throws SQLException SQL异常
     */
    public QueryResultBuilder columnTypes(List<String> types) throws SQLException {
        Validate.ensure(types.size() == columnNum, "size mismatch, req: " + columnNum + " got: " + types.size()); // 验证列类型数量
        this.columnTypes = new ArrayList<>(columnNum); // 初始化列类型列表
        for (int i = 0; i < columnNum; i++) {
            columnTypes.add(DataTypeFactory.get(types.get(i), serverContext)); // 获取列类型
        }
        return this;
    }

    /**
     * 添加行数据
     * 
     * @param row 行数据数组
     * @return QueryResultBuilder实例
     */
    public QueryResultBuilder addRow(Object... row) {
        return addRow(Arrays.asList(row)); // 转换为列表并添加行数据
    }

    /**
     * 添加行数据
     * 
     * @param row 行数据列表
     * @return QueryResultBuilder实例
     */
    public QueryResultBuilder addRow(List<?> row) {
        Validate.ensure(row.size() == columnNum, "size mismatch, req: " + columnNum + " got: " + row.size()); // 验证行数据数量
        rows.add(row); // 添加行数据
        return this;
    }

    /**
     * 构建查询结果
     * 
     * @return 查询结果
     * @throws SQLException SQL异常
     */
    public QueryResult build() throws SQLException {
        Validate.ensure(columnNames != null, "columnNames is null"); // 验证列名是否为空
        Validate.ensure(columnTypes != null, "columnTypes is null"); // 验证列类型是否为空

        IColumn[] headerColumns = new IColumn[columnNum]; // 头部列数组
        Object[] emptyObjects = new Object[columnNum]; // 空对象数组
        for (int c = 0; c < columnNum; c++) {
            headerColumns[c] = ColumnFactory.createColumn(columnNames.get(c), columnTypes.get(c), null, emptyObjects); // 创建头部列
        }
        Block headerBlock = new Block(0, headerColumns); // 创建头部块

        IColumn[] dataColumns = new IColumn[columnNum]; // 数据列数组
        for (int c = 0; c < columnNum; c++) {
            Object[] columnObjects = new Object[rows.size()]; // 列对象数组
            for (int r = 0; r < rows.size(); r++) {
                columnObjects[r] = rows.get(r).get(c); // 获取行数据
            }
            dataColumns[c] = ColumnFactory.createColumn(columnNames.get(c), columnTypes.get(c), null, columnObjects); // 创建数据列
        }
        Block dataBlock = new Block(rows.size(), dataColumns); // 创建数据块

        return new QueryResult() {

            @Override
            public Block header() throws SQLException {
                return headerBlock; // 返回头部块
            }

            @Override
            public CheckedIterator<DataResponse, SQLException> data() {
                DataResponse data = new DataResponse("client_build", dataBlock); // 创建数据响应

                return new CheckedIterator<DataResponse, SQLException>() {

                    private final DataResponse dataResponse = data; // 数据响应
                    private boolean beforeFirst = true; // 是否在第一项之前

                    public boolean hasNext() {
                        return (beforeFirst); // 检查是否有下一个元素
                    }

                    public DataResponse next() {
                        if (!beforeFirst) {
                            throw new NoSuchElementException(); // 抛出无元素异常
                        }
                        beforeFirst = false; // 设置为已遍历
                        return dataResponse; // 返回数据响应
                    }
                };
            }
        };
    }
}
