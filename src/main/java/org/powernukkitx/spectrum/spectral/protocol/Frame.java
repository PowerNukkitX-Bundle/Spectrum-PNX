package org.powernukkitx.spectrum.spectral.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public sealed interface Frame permits
        Frame.Acknowledgement,
        Frame.ConnectionRequest,
        Frame.ConnectionResponse,
        Frame.ConnectionClose,
        Frame.StreamRequest,
        Frame.StreamResponse,
        Frame.StreamData,
        Frame.StreamClose,
        Frame.MTURequest,
        Frame.MTUResponse {

    int id();

    void encode(ByteBuf buf);

    default void write(ByteBuf buf) {
        buf.writeIntLE(this.id());
        this.encode(buf);
    }

    static Frame decode(int id, ByteBuf buf) {
        return switch (id) {
            case FrameIds.ACKNOWLEDGEMENT -> Acknowledgement.read(buf);
            case FrameIds.CONNECTION_REQUEST -> new ConnectionRequest();
            case FrameIds.CONNECTION_RESPONSE -> ConnectionResponse.read(buf);
            case FrameIds.CONNECTION_CLOSE -> ConnectionClose.read(buf);
            case FrameIds.STREAM_REQUEST -> StreamRequest.read(buf);
            case FrameIds.STREAM_RESPONSE -> StreamResponse.read(buf);
            case FrameIds.STREAM_DATA -> StreamData.read(buf);
            case FrameIds.STREAM_CLOSE -> StreamClose.read(buf);
            case FrameIds.MTU_REQUEST -> MTURequest.read(buf);
            case FrameIds.MTU_RESPONSE -> MTUResponse.read(buf);
            default -> throw new IllegalArgumentException("Unknown spectral frame id: " + id);
        };
    }

    record Acknowledgement(long delayMicros, long max, long[] rangeStarts, long[] rangeEnds) implements Frame {
        @Override
        public int id() {
            return FrameIds.ACKNOWLEDGEMENT;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeLongLE(this.delayMicros);
            buf.writeIntLE((int) this.max);
            buf.writeIntLE(this.rangeStarts.length);
            for (int i = 0; i < this.rangeStarts.length; i++) {
                buf.writeIntLE((int) this.rangeStarts[i]);
                buf.writeIntLE((int) this.rangeEnds[i]);
            }
        }

        static Acknowledgement read(ByteBuf buf) {
            long delay = buf.readLongLE();
            long max = buf.readUnsignedIntLE();
            int count = (int) buf.readUnsignedIntLE();
            if (count > Protocol.MAX_ACK_RANGES) {
                throw new IllegalArgumentException("Too many ack ranges: " + count);
            }
            long[] starts = new long[count];
            long[] ends = new long[count];
            for (int i = 0; i < count; i++) {
                starts[i] = buf.readUnsignedIntLE();
                ends[i] = buf.readUnsignedIntLE();
            }
            return new Acknowledgement(delay, max, starts, ends);
        }
    }

    record ConnectionRequest() implements Frame {
        @Override
        public int id() {
            return FrameIds.CONNECTION_REQUEST;
        }

        @Override
        public void encode(ByteBuf buf) {
        }
    }

    record ConnectionResponse(long connectionId, int response) implements Frame {
        @Override
        public int id() {
            return FrameIds.CONNECTION_RESPONSE;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeLongLE(this.connectionId);
            buf.writeByte(this.response);
        }

        static ConnectionResponse read(ByteBuf buf) {
            return new ConnectionResponse(buf.readLongLE(), buf.readUnsignedByte());
        }
    }

    record ConnectionClose(int code, String message) implements Frame {
        @Override
        public int id() {
            return FrameIds.CONNECTION_CLOSE;
        }

        @Override
        public void encode(ByteBuf buf) {
            byte[] bytes = this.message.getBytes(StandardCharsets.UTF_8);
            buf.writeByte(this.code);
            buf.writeIntLE(bytes.length);
            buf.writeBytes(bytes);
        }

        static ConnectionClose read(ByteBuf buf) {
            int code = buf.readUnsignedByte();
            int len = (int) buf.readUnsignedIntLE();
            byte[] bytes = new byte[len];
            buf.readBytes(bytes);
            return new ConnectionClose(code, new String(bytes, StandardCharsets.UTF_8));
        }
    }

    record StreamRequest(long streamId) implements Frame {
        @Override
        public int id() {
            return FrameIds.STREAM_REQUEST;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeLongLE(this.streamId);
        }

        static StreamRequest read(ByteBuf buf) {
            return new StreamRequest(buf.readLongLE());
        }
    }

    record StreamResponse(long streamId, int response) implements Frame {
        @Override
        public int id() {
            return FrameIds.STREAM_RESPONSE;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeLongLE(this.streamId);
            buf.writeByte(this.response);
        }

        static StreamResponse read(ByteBuf buf) {
            return new StreamResponse(buf.readLongLE(), buf.readUnsignedByte());
        }
    }

    record StreamData(long streamId, long sequenceId, byte[] payload) implements Frame {
        @Override
        public int id() {
            return FrameIds.STREAM_DATA;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeLongLE(this.streamId);
            buf.writeIntLE((int) this.sequenceId);
            buf.writeIntLE(this.payload.length);
            buf.writeBytes(this.payload);
        }

        static StreamData read(ByteBuf buf) {
            long streamId = buf.readLongLE();
            long seq = buf.readUnsignedIntLE();
            int len = (int) buf.readUnsignedIntLE();
            byte[] payload = new byte[len];
            buf.readBytes(payload);
            return new StreamData(streamId, seq, payload);
        }
    }

    record StreamClose(long streamId) implements Frame {
        @Override
        public int id() {
            return FrameIds.STREAM_CLOSE;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeLongLE(this.streamId);
        }

        static StreamClose read(ByteBuf buf) {
            return new StreamClose(buf.readLongLE());
        }
    }

    record MTURequest(long mtu) implements Frame {
        @Override
        public int id() {
            return FrameIds.MTU_REQUEST;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeLongLE(this.mtu);
            int pad = (int) (this.mtu - 8);
            if (pad > 0) {
                buf.writeZero(pad);
            }
        }

        static MTURequest read(ByteBuf buf) {
            long mtu = buf.readLongLE();
            int pad = (int) (mtu - 8);
            if (pad > 0) {
                buf.skipBytes(pad);
            }
            return new MTURequest(mtu);
        }
    }

    record MTUResponse(long mtu) implements Frame {
        @Override
        public int id() {
            return FrameIds.MTU_RESPONSE;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeLongLE(this.mtu);
        }

        static MTUResponse read(ByteBuf buf) {
            return new MTUResponse(buf.readLongLE());
        }
    }
}
