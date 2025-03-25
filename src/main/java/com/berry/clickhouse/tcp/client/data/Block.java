/**
 * ClickHouse数据块类
 * 是数据传输和处理的核心组件，代表ClickHouse数据的容器
 * 用于在客户端和服务器之间传输数据，支持序列化和反序列化
 */
package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.EmptyReadWriter;
import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.BlockSettings.Setting;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseConnection;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseTableMetaData;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;
import com.berry.clickhouse.tcp.client.util.BinarySerializerUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据块类
 * 表示ClickHouse中的一个数据块，包含列和行的集合
 * 支持数据的读写、序列化和反序列化
 */
public class Block {

    /**
     * 服务器上下文信息
     */
    private NativeContext.ServerContext serverContext;

    /**
     * 列名到列对象的映射
     */
    private Map<String, IColumn> columnMap;

    /**
     * 数据块的模式（读模式或写模式）
     */
    private BlockDataModel model;

    /**
     * 是否序列化列数据
     */
    private boolean serializeCols;

    /**
     * 表元数据信息
     */
    private ClickHouseTableMetaData tableMetaData;

    /**
     * 链接
     */
    private ClickHouseConnection connection;

    /**
     * 从列数组创建数据块
     * 
     * @param columns 列数组
     * @param serverContext 服务器上下文
     * @return 创建的数据块
     * @throws SQLException 如果创建过程中发生错误
     */
    public static Block createFrom(IColumn[] columns, NativeContext.ServerContext serverContext) throws SQLException {
        Map<String, IColumn> columnMap = new HashMap<>(columns.length);
        ColumnWriterBufferFactory factory = serverContext.getColumnWriterBufferFactory();
        for (int i = 0; i < columns.length; i++) {
            IColumn column = columns[i];
            if (null == column.getColumnWriterBuffer()) {
                column.setColumnWriterBuffer(factory.getBuffer(column), factory);
            }
            columnMap.put(column.name(), column);
        }
        return new Block(0, columns, new BlockSettings(BlockSettings.Setting.defaultValues()), serverContext
                , columnMap, BlockDataModel.WRITE, false, null);
    }

    /**
     * 从表元数据创建数据块
     * 
     * @param metaData 表元数据
     * @param serverContext 服务器上下文
     * @return 创建的数据块
     * @throws SQLException 如果创建过程中发生错误
     */
    public static Block createFrom(ClickHouseTableMetaData metaData, NativeContext.ServerContext serverContext) throws SQLException {
        List<String> names = metaData.getColumnNames();
        byte[][] namesBytes = metaData.getColNameBytes();
        List<String> types = metaData.getColumnTypes();
        int columnCnt = names.size();

        Map<String, IColumn> columnMap = new HashMap<>(columnCnt);

        ColumnWriterBufferFactory factory = serverContext.getColumnWriterBufferFactory();
        IColumn[] columns = new IColumn[columnCnt];
        for (int i = 0; i < columnCnt; i++) {
            IDataType<?> dataType = DataTypeFactory.get(types.get(i), serverContext);
            String columnName = names.get(i);
            IColumn column = ColumnFactory.createColumn(names.get(i), dataType, namesBytes[i], null);
            column.setColumnWriterBuffer(factory.getBuffer(column), factory);
            columns[i] = column;
            columnMap.put(columnName, column);
        }
        return new Block(0, columns, new BlockSettings(BlockSettings.Setting.defaultValues()), serverContext
                , columnMap, BlockDataModel.WRITE, false, metaData);
    }

    /**
     * 从二进制流读取并解压数据到数据块
     * 
     * @param deserializer 二进制反序列化器
     * @param block 目标数据块
     * @param reserveDiffType 是否保留不同类型
     * @param exclude 排除的列名集合
     * @param serializeCols 需要序列化的列名集合
     * @return 读取的行数
     * @throws IOException 如果读取过程中发生I/O错误
     * @throws SQLException 如果读取过程中发生SQL错误
     */
    public static int readAndDecompressFrom(BinaryDeserializer deserializer, Block block, boolean reserveDiffType,
                                            Set<String> exclude, Set<String> serializeCols) throws IOException, SQLException {
        deserializer.maybeEnableCompressed();
        int rowCnt = readFromInputStream(deserializer, block, reserveDiffType, exclude, serializeCols);
        deserializer.maybeDisableCompressed();
        return rowCnt;
    }

    private static int readFromInputStream(BinaryDeserializer deserializer, Block block, boolean reserveDiffType,
                                           Set<String> exclude, Set<String> serializeCols) throws IOException, SQLException {
        int columnCnt = (int) deserializer.readVarInt();
        int rowCnt = (int) deserializer.readVarInt();

        for (int i = 0; i < columnCnt; i++) {
            String name = deserializer.readUTF8StringBinary();
            String type = deserializer.readUTF8StringBinary();

            IColumn column = block.getColumnMap().get(name);
            if (rowCnt > 0) {
                if (exclude.contains(name)) {
                    IDataType dataType = DataTypeFactory.get(type, block.serverContext);
                    dataType.deserializeBinaryBulk(rowCnt, EmptyReadWriter.DEFAULT, deserializer);
                } else if (type.equals(column.type().name())) {
                    if (serializeCols.contains(name)) {
                        Object[] values = column.type().deserializeBinaryBulk(rowCnt, deserializer);
                        column.setValues(values);
                    } else {
                        column.read(rowCnt, deserializer);
                        column.addRowCnt(rowCnt);
                    }
                } else {
                    IDataType<?> dataType = DataTypeFactory.get(type, block.serverContext);
                    if (reserveDiffType) {
                        Object[] values = column.type().deserializeBinaryBulk(rowCnt, deserializer);
                        column.setValues(values);
                    } else {
                        dataType.deserializeBinaryBulk(rowCnt, EmptyReadWriter.DEFAULT, deserializer);
                    }
                }
            }
        }
        return rowCnt;
    }

    public static Block readFromInputStream(BinaryDeserializer deserializer, Block block) throws IOException, SQLException {
        int columnCnt = (int) deserializer.readVarInt();
        int rowCnt = (int) deserializer.readVarInt();

        for (int i = 0; i < columnCnt; i++) {
            String name = deserializer.readUTF8StringBinary();
            deserializer.readUTF8StringBinary();
            IColumn column = block.getColumnMap().get(name);
            if (rowCnt > 0 && null != column) {
                column.read(rowCnt, deserializer);
                column.addRowCnt(rowCnt);
            }
        }
        block.rowCnt += rowCnt;
        return block;
    }

    public static Block readFrom(BinaryDeserializer deserializer, Block block) throws IOException, SQLException {
        BlockSettings.readFrom(deserializer);
        return readFromInputStream(deserializer, block);
    }

    public static Block readFrom(BinaryDeserializer deserializer,
                                 NativeContext.ServerContext serverContext, boolean serialize) throws IOException, SQLException {
        BlockSettings info = BlockSettings.readFrom(deserializer);

        int columnCnt = (int) deserializer.readVarInt();
        int rowCnt = (int) deserializer.readVarInt();

        IColumn[] columns = new IColumn[columnCnt];
        Map<String, IColumn> columnMap = new HashMap<>((rowCnt > 0) ? columnCnt : 0);
        if (!serialize) {
            ColumnWriterBufferFactory factory = serverContext.getColumnWriterBufferFactory();
            for (int i = 0; i < columnCnt; i++) {
                String name = deserializer.readUTF8StringBinary();
                String type = deserializer.readUTF8StringBinary();
                IDataType<?> dataType = DataTypeFactory.get(type, serverContext);
                IColumn column = ColumnFactory.createColumn(name, dataType, BinarySerializerUtil.serializeString(name), null);
                if (rowCnt > 0) {
                    column.setColumnWriterBuffer(factory.getBuffer(column), factory);
                    column.read(rowCnt, deserializer);
                    column.addRowCnt(rowCnt);

                }
                columnMap.put(column.name(), column);
                columns[i] = column;
            }
        } else {
            for (int i = 0; i < columnCnt; i++) {
                String name = deserializer.readUTF8StringBinary();
                String type = deserializer.readUTF8StringBinary();
                IDataType<?> dataType = DataTypeFactory.get(type, serverContext);
                Object[] arr = null;
                if (rowCnt > 0) {
                    arr = dataType.deserializeBinaryBulk(rowCnt, deserializer);
                }
                IColumn column = ColumnFactory.createColumn(name, dataType, BinarySerializerUtil.serializeString(name), arr);
                columns[i] = column;
                column.addRowCnt(rowCnt);
                columnMap.put(columns[i].name(), column);
            }
        }
        return new Block(rowCnt, columns, info, serverContext
                , columnMap, BlockDataModel.READ, serialize, null);
    }

    private final IColumn[] columns;
    private final BlockSettings settings;
    private final Map<String, Integer> nameAndPositions;
    private final Object[] rowData;
    private final int[] placeholderIndexes;
    private int rowCnt;
    private long readRows = 0;
    private long readBytes = 0;

    public Block() {
        this(0, new IColumn[0]);
    }

    public Block(int rowCnt, IColumn[] columns) {
        this(rowCnt, columns, new BlockSettings(Setting.defaultValues()));
    }

    public Block(int rowCnt, IColumn[] columns, BlockSettings settings, NativeContext.ServerContext serverContext,
                 Map<String, IColumn> columnMap, BlockDataModel model, boolean serializeCols, ClickHouseTableMetaData tableMetaData) {
        this(rowCnt, columns, settings);
        this.serverContext = serverContext;
        this.columnMap = columnMap;
        this.model = model;
        this.serializeCols = serializeCols;
        this.tableMetaData = tableMetaData;
    }

    public Block(int rowCnt, IColumn[] columns, BlockSettings settings) {
        this.rowCnt = rowCnt;
        this.columns = columns;
        this.settings = settings;

        this.rowData = new Object[columns.length];
        this.nameAndPositions = new HashMap<>();
        this.placeholderIndexes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            nameAndPositions.put(columns[i].name(), i + 1);
            placeholderIndexes[i] = i;
        }
    }

    public ClickHouseConnection getConnection() {
        return connection;
    }

    public void setConnection(ClickHouseConnection connection) {
        this.connection = connection;
    }

    public int rowCnt() {
        if (isWrite()) {
            return loadRowsCnt();
        }
        return rowCnt;
    }

    private int loadRowsCnt() {
        if (null == columns)
            return 0;
        int tmpRowsCnt = -1;
        for (IColumn column : columns) {
            if (null == column)
                continue;
            if (column.rowCnt() < tmpRowsCnt || tmpRowsCnt == -1)
                tmpRowsCnt = column.rowCnt();
        }
        if (tmpRowsCnt == -1)
            return 0;
        return tmpRowsCnt;
    }

    /**
     * 添加一行数据
     * 
     * @throws SQLException 如果添加失败
     */
    public void appendRow() throws SQLException {
        int i = 0;
        try {
            for (; i < columns.length; i++) {
                columns[i].write(rowData[i]);
            }
            rowCnt++;
        } catch (IOException | ClassCastException e) {
            throw new SQLException("Exception processing value " + rowData[i] + " for column: " + columns[i].name(), e);
        }
    }

    public void setObject(int columnIdx, Object object) {
        rowData[columnIdx] = object;
    }

    public int paramIdx2ColumnIdx(int paramIdx) {
        return placeholderIndexes[paramIdx];
    }

    public void incPlaceholderIndexes(int columnIdx) {
        for (int i = columnIdx; i < placeholderIndexes.length; i++) {
            placeholderIndexes[i] += 1;
        }
    }

    /**
     * 将数据块写入二进制序列化器
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入过程中发生I/O错误
     * @throws SQLException 如果写入过程中发生SQL错误
     */
    public void writeTo(BinarySerializer serializer) throws IOException, SQLException {
        settings.writeTo(serializer);

        this.rowCnt = loadRowsCnt();
        serializer.writeVarInt(columns.length);
        serializer.writeVarInt(rowCnt);

        for (IColumn column : columns) {
            column.flushToSerializer(serializer, true);
        }
    }

    /**
     * 获取指定索引的列
     * 
     * @param columnIdx 列索引
     * @return 列对象
     * @throws SQLException 如果列索引无效
     */
    public IColumn getColumn(int columnIdx) throws SQLException {
        Validate.isTrue(columnIdx < columns.length,
                "Position " + columnIdx +
                        " is out of bound in Block.getByPosition, max position = " + (columns.length - 1));
        return columns[columnIdx];
    }

    public IColumn getColumn(String columnName) throws SQLException {
        return columns[getPositionByName(columnName) - 1];
    }

    public int getPositionByName(String columnName) throws SQLException {
        Validate.isTrue(nameAndPositions.containsKey(columnName), "Column '" + columnName + "' does not exist");
        return nameAndPositions.get(columnName);
    }

    public Object getObject(int columnIdx) throws SQLException {
        Validate.isTrue(columnIdx < columns.length,
                "Position " + columnIdx +
                        " is out of bound in Block.getByPosition, max position = " + (columns.length - 1));
        return rowData[columnIdx];
    }

    /**
     * 初始化写入缓冲区
     */
    public void initWriteBuffer() {
        ColumnWriterBufferFactory bufferFactory = this.serverContext.getColumnWriterBufferFactory();
        for (IColumn column : columns) {
            ColumnWriterBuffer writeBuffer = column.getColumnWriterBuffer();

            if (writeBuffer != null) {
                bufferFactory.recycleBuffer(writeBuffer);
            }
            column.setColumnWriterBuffer(bufferFactory.getBuffer(column), bufferFactory);
        }
    }

    private IColumn getIColumn(String columnName) {
        if (null == this.columnMap || this.columnMap.isEmpty()) {
            return null;
        }
        return this.columnMap.get(columnName);
    }

    public Object[] read(String columnName) {
        IColumn column = getIColumn(columnName);
        if (null == column) {
            return null;
        }
        return column.values();
    }

    /**
     * 清理数据块资源
     */
    public void cleanup(int[] columnNums) {
        if (null == this.serverContext || null == columns || null == columnNums || columnNums.length == 0) {
            return;
        }
        ColumnWriterBufferFactory factory = this.serverContext.getColumnWriterBufferFactory();
        for (int index : columnNums) {
            if (index >= columns.length) {
                continue;
            }
            IColumn column = columns[index];
            if (null == column) {
                continue;
            }

            column.recycleColumnWriterBuffer(factory);
            this.columnMap.remove(column.name());
            columns[index] = null;
        }

    }

    public void cleanup(int columnNum) {
        if (null == this.serverContext || null == columns) {
            return;
        }

        if (columnNum < columns.length) {
            IColumn column = columns[columnNum];
            if (null != column) {
                column.recycleColumnWriterBuffer(this.serverContext.getColumnWriterBufferFactory());
                this.columnMap.remove(column.name());
                columns[columnNum] = null;
            }
        }
        movePosition();
    }


    private void movePosition() {
        int preNullIndex = -1;
        for (int i = 0; i < columns.length; i++) {
            if (preNullIndex == -1 && null != columns[i]) {
                preNullIndex = i;
            }

            if (null != columns[i] && preNullIndex >= 0) {
                columns[preNullIndex] = columns[i];
                columns[i] = null;
                preNullIndex++;
            }
        }
    }

    /**
     * 清理数据块资源
     */
    public void cleanup() {

        if (null == columns) {
            return;
        }

        ColumnWriterBufferFactory bufferFactory = this.serverContext.getColumnWriterBufferFactory();
        for (IColumn column : columns) {
            ColumnWriterBuffer writeBuffer = column.getColumnWriterBuffer();
            if (writeBuffer != null) {
                bufferFactory.recycleBuffer(writeBuffer);
                column.setColumnWriterBuffer(null);
            }
        }

        this.columnMap.clear();
    }

    /**
     * 清理数据块数据
     */
    public void cleanData() {
        if (null == columns) {
            return;
        }

        for (IColumn column : columns) {
            if (null != column) {
                column.clear();
            }
        }

    }

    /**
     * 回滚数据块操作
     */
    public void rollback() {
        if (null == columns) {
            return;
        }

        for (int i = 0; i < columns.length; i++) {
            IColumn column = columns[i];
            if (null != column) {
                column.rewind();
            }
        }
    }

    /**
     * 重置数据块位置
     */
    public void rewind() {
        this.model = BlockDataModel.READ;
        int tmpRowsCnt = -1;
        if (null == columns) {
            return;
        }

        for (IColumn column : columns) {
            if (null != column && null != column.getColumnWriterBuffer()) {
                column.rewind();
                if (column.rowCnt() < tmpRowsCnt || tmpRowsCnt == -1) {
                    tmpRowsCnt = column.rowCnt();
                }
            }
        }
        if (tmpRowsCnt == -1) {
            tmpRowsCnt = 0;
        }
        this.rowCnt = tmpRowsCnt;
    }

    public int columnCnt() {
        if (null == columns) {
            return 0;
        }
        return columns.length;
    }

    public void setProgress(long readRows, long readBytes) {
        this.readRows = readRows;
        this.readBytes = readBytes;
    }

    public long readRows() {
        return readRows;
    }

    public long readBytes() {
        return readBytes;
    }

    public Map<String, IColumn> getColumnMap() {
        return columnMap;
    }

    public boolean isSerializeCols() {
        return serializeCols;
    }

    public boolean isRead() {
        return this.model == BlockDataModel.READ;
    }

    public boolean isWrite() {
        return this.model == BlockDataModel.READ;
    }

    public ClickHouseTableMetaData getTableMetaData() {
        return tableMetaData;
    }
}
