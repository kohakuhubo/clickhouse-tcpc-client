package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class SocketBuffedReader implements BuffedReader {

    private final int capacity;
    private final byte[] buf;
    private final InputStream in;

    private int limit;
    private int position;

    public SocketBuffedReader(Socket socket) throws IOException {
        this(socket.getInputStream(), ClickHouseDefines.SOCKET_RECV_BUFFER_BYTES);
    }

    SocketBuffedReader(InputStream in, int capacity) {
        this.limit = 0;
        this.position = 0;
        this.capacity = capacity;

        this.in = in;
        this.buf = new byte[capacity];
    }

    @Override
    public int readBinary() throws IOException {
        if (!remaining() && !refill()) {
            throw new EOFException("Attempt to read after eof.");
        }

        return buf[position++] & 0xFF;
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        for (int i = 0; i < bytes.length; ) {
            if (!remaining() && !refill()) {
                throw new EOFException("Attempt to read after eof.");
            }

            int pending = bytes.length - i;
            int fillLength = Math.min(pending, limit - position);

            if (fillLength > 0) {
                System.arraycopy(buf, position, bytes, i, fillLength);

                i += fillLength;
                this.position += fillLength;
            }
        }
        return bytes.length;
    }

    @Override
    public void rewind() {

    }

    @Override
    public void clear() {

    }

    private boolean remaining() {
        return position < limit;
    }

    private boolean refill() throws IOException {
        if (!remaining() && (limit = in.read(buf, 0, capacity)) <= 0) {
            throw new EOFException("Attempt to read after eof.");
        }
        position = 0;
        return true;
    }
}
