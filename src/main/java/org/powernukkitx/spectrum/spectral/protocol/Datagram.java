package org.powernukkitx.spectrum.spectral.protocol;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public final class Datagram {

    private Datagram() {
    }

    public static void writeHeader(ByteBuf buf, long connectionId, long sequenceId) {
        buf.writeBytes(Protocol.MAGIC);
        buf.writeLongLE(connectionId);
        buf.writeIntLE((int) sequenceId);
    }

    public static Long peekConnectionId(ByteBuf buf) {
        if (buf.readableBytes() < Protocol.PACKET_HEADER_SIZE) {
            return null;
        }
        int start = buf.readerIndex();
        for (int i = 0; i < Protocol.MAGIC.length; i++) {
            if (buf.getByte(start + i) != Protocol.MAGIC[i]) {
                return null;
            }
        }
        return buf.getLongLE(start + Protocol.MAGIC.length);
    }

    public static long readSequenceId(ByteBuf buf) {
        return buf.getUnsignedIntLE(buf.readerIndex() + Protocol.MAGIC.length + Long.BYTES);
    }

    public static List<Frame> decodeFrames(ByteBuf buf) {
        buf.skipBytes(Protocol.PACKET_HEADER_SIZE);
        List<Frame> frames = new ArrayList<>();
        while (buf.readableBytes() >= Integer.BYTES) {
            int frameId = buf.readIntLE();
            try {
                frames.add(Frame.decode(frameId, buf));
            } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                break;
            }
        }
        return frames;
    }
}
