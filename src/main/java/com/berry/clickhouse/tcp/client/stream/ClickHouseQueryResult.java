package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.misc.CheckedIterator;
import com.berry.clickhouse.tcp.client.misc.CheckedSupplier;
import com.berry.clickhouse.tcp.client.protocol.DataResponse;
import com.berry.clickhouse.tcp.client.protocol.EOFStreamResponse;
import com.berry.clickhouse.tcp.client.protocol.ProgressResponse;
import com.berry.clickhouse.tcp.client.protocol.Response;
import com.berry.clickhouse.tcp.client.protocol.listener.ProgressListener;

import java.sql.SQLException;
import java.util.List;


public class ClickHouseQueryResult implements QueryResult {
    private final CheckedSupplier<Response, SQLException> responseSupplier;
    private ProgressListener progressListener;
    private Block header;
    private Block currentBlock;
    private List<Block> blocks;
    private boolean atEnd;
    private int nextIndexBlockIndex = 1;
    private CheckedIterator<DataResponse, SQLException> data;

    public ClickHouseQueryResult(List<Block> blocks) {
        this.responseSupplier = null;
        this.blocks = blocks;
    }

    public ClickHouseQueryResult(CheckedSupplier<Response, SQLException> responseSupplier) {
        this.responseSupplier = responseSupplier;
        this.data = data();
    }

    public void setProgressListener(ProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    @Override
    public Block header() throws SQLException {
        ensureHeaderConsumed();
        return header;
    }

    @Override
    public CheckedIterator<DataResponse, SQLException> data() {
        return new CheckedIterator<DataResponse, SQLException>() {

            private DataResponse current;

            @Override
            public boolean hasNext() throws SQLException {
                return current != null || fill() != null;
            }

            @Override
            public DataResponse next() throws SQLException {
                return drain();
            }

            private DataResponse fill() throws SQLException {
                ensureHeaderConsumed();
                return current = consumeDataResponse();
            }

            private DataResponse drain() throws SQLException {
                if (current == null) {
                    fill();
                }

                DataResponse top = current;
                current = null;
                return top;
            }
        };
    }

    private void ensureHeaderConsumed() throws SQLException {
        if (header == null) {
            DataResponse firstDataResponse = consumeDataResponse();
            header = firstDataResponse != null ? firstDataResponse.block() : new Block();
        }
    }

    private DataResponse consumeDataResponse() throws SQLException {
        long readRows = 0;
        long readBytes = 0;
        while (!atEnd) {
            Response response = responseSupplier.get();
            if (response instanceof DataResponse) {
                DataResponse dataResponse = (DataResponse) response;
                dataResponse.block().setProgress(readRows, readBytes);
                return dataResponse;
            } else if (response instanceof EOFStreamResponse || response == null) {
                atEnd = true;
            } else if (response instanceof ProgressResponse) {
                if (progressListener != null) {
                    progressListener.onProgress((ProgressResponse) response);
                }
                readRows += ((ProgressResponse) response).newRows();
                readBytes += ((ProgressResponse) response).newBytes();
            }
        }

        return null;
    }
}
