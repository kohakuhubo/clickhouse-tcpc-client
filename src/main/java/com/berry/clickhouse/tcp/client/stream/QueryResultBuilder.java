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

public class QueryResultBuilder {

    private final int columnNum;
    private final NativeContext.ServerContext serverContext;
    private List<String> columnNames;
    private List<IDataType> columnTypes;
    private final List<List<?>> rows = new ArrayList<>();

    public static QueryResultBuilder builder(int columnsNum, NativeContext.ServerContext serverContext) {
        return new QueryResultBuilder(columnsNum, serverContext);
    }

    private QueryResultBuilder(int columnNum, NativeContext.ServerContext serverContext) {
        this.columnNum = columnNum;
        this.serverContext = serverContext;
    }

    public QueryResultBuilder columnNames(String... names) {
        return columnNames(Arrays.asList(names));
    }

    public QueryResultBuilder columnNames(List<String> names) {
        Validate.ensure(names.size() == columnNum, "size mismatch, req: " + columnNum + " got: " + names.size());
        this.columnNames = names;
        return this;
    }

    public QueryResultBuilder columnTypes(String... types) throws SQLException {
        return columnTypes(Arrays.asList(types));
    }

    public QueryResultBuilder columnTypes(List<String> types) throws SQLException {
        Validate.ensure(types.size() == columnNum, "size mismatch, req: " + columnNum + " got: " + types.size());
        this.columnTypes = new ArrayList<>(columnNum);
        for (int i = 0; i < columnNum; i++) {
            columnTypes.add(DataTypeFactory.get(types.get(i), serverContext));
        }
        return this;
    }

    public QueryResultBuilder addRow(Object... row) {
        return addRow(Arrays.asList(row));
    }

    public QueryResultBuilder addRow(List<?> row) {
        Validate.ensure(row.size() == columnNum, "size mismatch, req: " + columnNum + " got: " + row.size());
        rows.add(row);
        return this;
    }

    public QueryResult build() throws SQLException {
        Validate.ensure(columnNames != null, "columnNames is null");
        Validate.ensure(columnTypes != null, "columnTypes is null");

        IColumn[] headerColumns = new IColumn[columnNum];
        Object[] emptyObjects = new Object[columnNum];
        for (int c = 0; c < columnNum; c++) {
            headerColumns[c] = ColumnFactory.createColumn(columnNames.get(c), columnTypes.get(c), null, emptyObjects, false);
        }
        Block headerBlock = new Block(0, headerColumns);

        IColumn[] dataColumns = new IColumn[columnNum];
        for (int c = 0; c < columnNum; c++) {
            Object[] columnObjects = new Object[rows.size()];
            for (int r = 0; r < rows.size(); r++) {
                columnObjects[r] = rows.get(r).get(c);
            }
            dataColumns[c] = ColumnFactory.createColumn(columnNames.get(c), columnTypes.get(c), null, columnObjects, false);
        }
        Block dataBlock = new Block(rows.size(), dataColumns);

        return new QueryResult() {

            @Override
            public Block header() throws SQLException {
                return headerBlock;
            }

            @Override
            public CheckedIterator<DataResponse, SQLException> data() {
                DataResponse data = new DataResponse("client_build", dataBlock);

                return new CheckedIterator<DataResponse, SQLException>() {

                    private final DataResponse dataResponse = data;
                    private boolean beforeFirst = true;

                    public boolean hasNext() {
                        return (beforeFirst);
                    }

                    public DataResponse next() {
                        if (!beforeFirst) {
                            throw new NoSuchElementException();
                        }
                        beforeFirst = false;
                        return dataResponse;
                    }
                };
            }
        };
    }
}
