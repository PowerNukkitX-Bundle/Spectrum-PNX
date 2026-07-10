package org.powernukkitx.spectrum.spectral;

import org.powernukkitx.spectrum.spectral.protocol.Frame;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class Stream {
    private final long streamId;
    private final Connection connection;

    private long sendSequence = 0L;
    private long expected = 0L;
    private final Map<Long, byte[]> reorder = new HashMap<>();

    private Consumer<byte[]> reader;
    private Runnable closeHandler;
    private boolean closed = false;

    Stream(long streamId, Connection connection) {
        this.streamId = streamId;
        this.connection = connection;
    }

    public long streamId() {
        return this.streamId;
    }

    public void setReader(Consumer<byte[]> reader) {
        this.reader = reader;
    }

    public void setCloseHandler(Runnable closeHandler) {
        this.closeHandler = closeHandler;
    }

    public void write(byte[] data) {
        if (this.closed) {
            return;
        }
        int chunkSize = this.connection.streamChunkSize();
        int offset = 0;
        while (offset < data.length) {
            int len = Math.min(chunkSize, data.length - offset);
            byte[] chunk = new byte[len];
            System.arraycopy(data, offset, chunk, 0, len);
            this.connection.queueStreamData(new Frame.StreamData(this.streamId, this.sendSequence++, chunk));
            offset += len;
        }
    }

    void onData(long sequenceId, byte[] payload) {
        if (sequenceId < this.expected) {
            return;
        }
        if (sequenceId == this.expected) {
            deliver(payload);
            this.expected++;
            byte[] next;
            while ((next = this.reorder.remove(this.expected)) != null) {
                deliver(next);
                this.expected++;
            }
        } else {
            this.reorder.putIfAbsent(sequenceId, payload);
        }
    }

    private void deliver(byte[] payload) {
        if (this.reader != null) {
            this.reader.accept(payload);
        }
    }

    public void close() {
        internalClose(true);
    }

    void internalClose(boolean notifyPeer) {
        if (this.closed) {
            return;
        }
        this.closed = true;
        if (notifyPeer) {
            this.connection.sendReliableFrame(new Frame.StreamClose(this.streamId));
        }
        this.connection.removeStream(this.streamId);
        if (this.closeHandler != null) {
            this.closeHandler.run();
        }
    }

    public boolean isClosed() {
        return this.closed;
    }
}
