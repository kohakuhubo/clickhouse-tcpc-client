package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.MappedByteBufferReader;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.sql.SQLException;
import java.util.Set;

/**
 * BlockResultSet类用于处理ClickHouse数据块的结果集
 * 提供对数据块的迭代访问，支持读取和反序列化
 */
public class BlockResultSet {

    private BinaryDeserializer deserializer; // 二进制反序列化器
    private Block block; // 数据块
    private boolean reserveDiffType; // 是否保留不同类型
    private boolean hasNext = true; // 是否有下一个数据块
    private Set<String> exclude; // 排除的列名集合
    private Set<String> serializedCols; // 需要序列化的列名集合
    private int rows = 0; // 读取的行数

    /**
     * 构造函数，初始化BlockResultSet
     * 
     * @param buffer 数据缓冲区
     * @param enableCompress 是否启用压缩
     * @param block 数据块
     * @param reserveDiffType 是否保留不同类型
     * @param exclude 排除的列名集合
     * @param serializedCols 需要序列化的列名集合
     */
    public BlockResultSet(MappedByteBuffer buffer, boolean enableCompress, Block block, boolean reserveDiffType,
                          Set<String> exclude, Set<String> serializedCols) {
        this.block = block;
        this.reserveDiffType = reserveDiffType;
        this.exclude = exclude;
        this.serializedCols = serializedCols;
        this.deserializer = new BinaryDeserializer(new MappedByteBufferReader(buffer), enableCompress);
    }

    /**
     * 检查是否有下一个数据块
     * 
     * @return 如果有下一个数据块则返回true，否则返回false
     * @throws SQLException 如果发生SQL错误
     * @throws IOException 如果发生I/O错误
     */
    public boolean hasNext() throws SQLException, IOException {
        if (!hasNext) {
            return false;
        }

        try {
            this.rows = Block.readAndDecompressFrom(this.deserializer, this.block, this.reserveDiffType, this.exclude, this.serializedCols);
        } catch (EOFException e) {
            this.hasNext = false; // 到达文件末尾
        }
        return this.hasNext;
    }

    /**
     * 获取下一个数据块
     * 
     * @return 当前数据块
     */
    public Block next() {
        return this.block;
    }

    /**
     * 设置当前数据块
     * 
     * @param block 新的数据块
     */
    public void setBlock(Block block) {
        this.block = block;
    }

    /**
     * 获取读取的行数
     * 
     * @return 读取的行数
     */
    public int getRows() {
        return rows;
    }
}
