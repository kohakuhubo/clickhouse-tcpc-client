package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.MappedByteBufferReader;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.sql.SQLException;
import java.util.Set;

public class BlockResultSet {

    private BinaryDeserializer deserializer;

    private Block block;

    private boolean reserveDiffType;

    private boolean hasNext = true;

    private Set<String> exclude;

    private Set<String> serializedCols;

    private int rows = 0;

    public BlockResultSet(MappedByteBuffer buffer, boolean enableCompress, Block block, boolean reserveDiffType,
                          Set<String> exclude, Set<String> serializedCols) {
        this.block = block;
        this.reserveDiffType = reserveDiffType;
        this.exclude = exclude;
        this.serializedCols = serializedCols;
        this.deserializer = new BinaryDeserializer(new MappedByteBufferReader(buffer), enableCompress);
    }

    public boolean hasNext() throws SQLException, IOException {
        if (!hasNext) {
            return false;
        }

        try {
            this.rows = Block.readAndDecompressFrom(this.deserializer, this.block, this.reserveDiffType, this.exclude, this.serializedCols);
        } catch (EOFException e) {
            this.hasNext = false;
        }
        return this.hasNext;
    }

    public Block next() {
        return this.block;
    }

    public void setBlock(Block block) {
        this.block = block;
    }

    public int getRows() {
        return rows;
    }
}
