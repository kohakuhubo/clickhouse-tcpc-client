package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class Column extends AbstractColumn {

    public Column(String name, IDataType<?> type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values);
        this.values = values;
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        type().serializeBinary(object, buffer.column);
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws IOException, SQLException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        if (now) {
            buffer.writeTo(serializer);
        }
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        if (null != this.buffer) {
            factory.recycleBuffer(buffer);
            this.setColumnWriterBuffer(null);
        }
    }

    @Override
    public void rewind() {
        this.buffer.rewind();
    }
}
