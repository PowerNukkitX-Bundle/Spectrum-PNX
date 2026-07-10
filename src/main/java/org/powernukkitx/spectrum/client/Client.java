package org.powernukkitx.spectrum.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.powernukkitx.spectrum.spectral.Stream;

import java.util.function.Consumer;

/**
 * Wraps one spectral {@link Stream} with Spectrum's per-message framing:
 * inbound  (proxy -> server): [uint32 BE length][snappy(rawPacket)]
 * outbound (server -> proxy): [uint32 BE length = len+1][decode flag byte][snappy(rawPacket)]
 */
public final class Client {
    private static final int DECODE_NEEDED = 0x00;
    private static final int DECODE_NOT_NEEDED = 0x01;

    private final Stream stream;
    private final ByteBuf buffer = Unpooled.buffer();
    private Consumer<byte[]> reader;

    public Client(Stream stream) {
        this.stream = stream;
        this.stream.setReader(this::onStreamData);
    }

    public Stream stream() {
        return this.stream;
    }

    public void setReader(Consumer<byte[]> reader) {
        this.reader = reader;
    }

    public void setCloseHandler(Runnable handler) {
        this.stream.setCloseHandler(handler);
    }

    private void onStreamData(byte[] chunk) {
        this.buffer.writeBytes(chunk);
        while (this.buffer.readableBytes() >= Integer.BYTES) {
            int length = this.buffer.getInt(this.buffer.readerIndex());
            if (this.buffer.readableBytes() < Integer.BYTES + length) {
                break;
            }
            this.buffer.skipBytes(Integer.BYTES);
            byte[] compressed = new byte[length];
            this.buffer.readBytes(compressed);
            if (this.reader != null) {
                byte[] packet = Snappy.decompress(compressed, 0, compressed.length);
                this.reader.accept(packet);
            }
        }
        this.buffer.discardReadBytes();
    }

    public void write(byte[] rawPacket, boolean decodeNeeded) {
        byte[] compressed = Snappy.compress(rawPacket);
        ByteBuf out = Unpooled.buffer(Integer.BYTES + 1 + compressed.length);
        out.writeInt(compressed.length + 1);
        out.writeByte(decodeNeeded ? DECODE_NEEDED : DECODE_NOT_NEEDED);
        out.writeBytes(compressed);
        byte[] framed = new byte[out.readableBytes()];
        out.readBytes(framed);
        out.release();
        this.stream.write(framed);
    }

    public void close() {
        this.stream.close();
        this.buffer.release();
    }
}
