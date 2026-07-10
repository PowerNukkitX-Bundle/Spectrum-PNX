package org.powernukkitx.spectrum.spectral;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.powernukkitx.spectrum.spectral.protocol.Datagram;
import org.powernukkitx.spectrum.spectral.protocol.Frame;
import org.powernukkitx.spectrum.spectral.protocol.FrameIds;
import org.powernukkitx.spectrum.spectral.protocol.Protocol;
import org.powernukkitx.spectrum.spectral.reliable.AckQueue;
import org.powernukkitx.spectrum.spectral.reliable.Congestion;
import org.powernukkitx.spectrum.spectral.reliable.ReceiveQueue;
import org.powernukkitx.spectrum.spectral.reliable.RetransmissionQueue;
import org.powernukkitx.spectrum.spectral.reliable.RTT;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class Connection {
    // 4 (frame id) + 8 (streamId) + 4 (seq) + 4 (length)
    private static final int STREAM_DATA_OVERHEAD = 20;

    private final long connectionId;
    private final InetSocketAddress remote;
    private final Transport transport;

    private final ReceiveQueue receiveQueue = new ReceiveQueue();
    private final AckQueue ackQueue = new AckQueue();
    private final RetransmissionQueue retransmissionQueue = new RetransmissionQueue();
    private final RTT rtt = new RTT();
    private final Congestion congestion = new Congestion();

    private final Map<Long, Stream> streams = new HashMap<>();
    private final Deque<Frame.StreamData> dataQueue = new ArrayDeque<>();

    private long nextSequence = 1L;
    private long lastReceiveNanos;
    private boolean closed = false;

    private Consumer<Stream> streamAcceptor;
    private Runnable closeHandler;

    public Connection(long connectionId, InetSocketAddress remote, Transport transport, long nowNanos) {
        this.connectionId = connectionId;
        this.remote = remote;
        this.transport = transport;
        this.lastReceiveNanos = nowNanos;
    }

    public long connectionId() {
        return this.connectionId;
    }

    public InetSocketAddress remoteAddress() {
        return this.remote;
    }

    public void setStreamAcceptor(Consumer<Stream> acceptor) {
        this.streamAcceptor = acceptor;
    }

    public void setCloseHandler(Runnable handler) {
        this.closeHandler = handler;
    }

    int streamChunkSize() {
        return this.congestion.mss() - STREAM_DATA_OVERHEAD;
    }

    void queueStreamData(Frame.StreamData frame) {
        this.dataQueue.addLast(frame);
    }

    void onDatagram(ByteBuf buf, long nowNanos) {
        long sequenceId = Datagram.readSequenceId(buf);
        List<Frame> frames = Datagram.decodeFrames(buf);
        this.lastReceiveNanos = nowNanos;

        boolean process = true;
        if (sequenceId != Protocol.SEQUENCE_UNRELIABLE) {
            this.ackQueue.add(sequenceId, nowNanos);
            process = this.receiveQueue.receive(sequenceId);
        }
        if (!process) {
            return; // duplicate: acked, not processed
        }

        for (Frame frame : frames) {
            handleFrame(frame, nowNanos);
        }
    }

    private void handleFrame(Frame frame, long nowNanos) {
        if (frame instanceof Frame.Acknowledgement ack) {
            processAck(ack, nowNanos);
        } else if (frame instanceof Frame.ConnectionClose) {
            teardown();
        } else if (frame instanceof Frame.StreamRequest req) {
            handleStreamRequest(req.streamId());
        } else if (frame instanceof Frame.StreamData data) {
            Stream stream = this.streams.get(data.streamId());
            if (stream != null) {
                stream.onData(data.sequenceId(), data.payload());
            }
        } else if (frame instanceof Frame.StreamClose close) {
            Stream stream = this.streams.get(close.streamId());
            if (stream != null) {
                stream.internalClose(false);
            }
        } else if (frame instanceof Frame.MTURequest req) {
            sendUnreliable(List.of(new Frame.MTUResponse(req.mtu())));
        }
        // ConnectionRequest/ConnectionResponse/StreamResponse/MTUResponse: no server action
    }

    private void handleStreamRequest(long streamId) {
        if (this.streams.containsKey(streamId)) {
            sendReliableFrame(new Frame.StreamResponse(streamId, FrameIds.RESPONSE_FAILED));
            return;
        }
        Stream stream = new Stream(streamId, this);
        this.streams.put(streamId, stream);
        sendReliableFrame(new Frame.StreamResponse(streamId, FrameIds.RESPONSE_SUCCESS));
        if (this.streamAcceptor != null) {
            this.streamAcceptor.accept(stream);
        }
    }

    private void processAck(Frame.Acknowledgement ack, long nowNanos) {
        for (int i = 0; i < ack.rangeStarts().length; i++) {
            for (long seq = ack.rangeStarts()[i]; seq <= ack.rangeEnds()[i]; seq++) {
                RetransmissionQueue.Entry entry = this.retransmissionQueue.ack(seq);
                if (entry == null) {
                    continue;
                }
                this.congestion.onAck(entry.length());
                if (seq == ack.max()) {
                    this.rtt.add(nowNanos - entry.sentNanos(), ack.delayMicros() * 1000L);
                }
            }
        }
    }

    // ---- outgoing ----

    void sendReliableFrame(Frame frame) {
        if (this.closed) {
            return;
        }
        long seq = this.nextSequence++;
        ByteBuf buf = Unpooled.buffer();
        Datagram.writeHeader(buf, this.connectionId, seq);
        frame.write(buf);
        appendPiggybackAcks(buf, System.nanoTime());
        byte[] bytes = toBytes(buf);
        this.retransmissionQueue.add(seq, bytes, System.nanoTime());
        this.congestion.onSend(bytes.length);
        this.transport.send(this.remote, bytes);
    }

    private void sendUnreliable(List<Frame> frames) {
        ByteBuf buf = Unpooled.buffer();
        Datagram.writeHeader(buf, this.connectionId, Protocol.SEQUENCE_UNRELIABLE);
        for (Frame frame : frames) {
            frame.write(buf);
        }
        this.transport.send(this.remote, toBytes(buf));
    }

    private void appendPiggybackAcks(ByteBuf buf, long nowNanos) {
        if (!this.ackQueue.hasPending()) {
            return;
        }
        for (Frame.Acknowledgement ack : this.ackQueue.drain(nowNanos)) {
            ack.write(buf);
        }
    }

    private void flushData(long nowNanos) {
        while (!this.dataQueue.isEmpty()) {
            int mss = this.congestion.mss();
            if (!this.congestion.canSend(mss)) {
                break;
            }

            long seq = this.nextSequence++;
            ByteBuf buf = Unpooled.buffer();
            Datagram.writeHeader(buf, this.connectionId, seq);

            boolean wrote = false;
            while (!this.dataQueue.isEmpty()) {
                Frame.StreamData frame = this.dataQueue.peekFirst();
                int frameSize = STREAM_DATA_OVERHEAD + frame.payload().length;
                if (wrote && buf.readableBytes() + frameSize > mss) {
                    break;
                }
                frame.write(buf);
                this.dataQueue.pollFirst();
                wrote = true;
                if (buf.readableBytes() >= mss) {
                    break;
                }
            }

            appendPiggybackAcks(buf, nowNanos);
            byte[] bytes = toBytes(buf);
            this.retransmissionQueue.add(seq, bytes, nowNanos);
            this.congestion.onSend(bytes.length);
            this.transport.send(this.remote, bytes);
        }
    }

    private void flushStandaloneAcks(long nowNanos) {
        if (!this.ackQueue.shouldFlush(nowNanos)) {
            return;
        }
        List<Frame> acks = new ArrayList<>(this.ackQueue.drain(nowNanos));
        if (!acks.isEmpty()) {
            sendUnreliable(acks);
        }
    }

    // ---- tick ----

    void tick(long nowNanos) {
        if (this.closed) {
            return;
        }

        if (nowNanos - this.lastReceiveNanos >= Protocol.INACTIVITY_TIMEOUT_NANOS) {
            closeWithError(FrameIds.CLOSE_TIMEOUT, "network inactivity");
            return;
        }

        long rto = this.rtt.rto();
        RetransmissionQueue.Timeout timeout = this.retransmissionQueue.tick(nowNanos, rto);
        if (timeout != null) {
            if (timeout.dropped()) {
                this.congestion.releaseFlight(timeout.droppedLength());
            } else {
                this.transport.send(this.remote, timeout.resend());
            }
            this.congestion.onCongestionEvent(nowNanos, timeout.originalSentNanos());
        }

        flushData(nowNanos);
        flushStandaloneAcks(nowNanos);
    }

    public void closeWithError(int code, String message) {
        if (this.closed) {
            return;
        }
        // send close reliably-ish as a single unreliable datagram (peer closes on receipt)
        long seq = this.nextSequence++;
        ByteBuf buf = Unpooled.buffer();
        Datagram.writeHeader(buf, this.connectionId, seq);
        new Frame.ConnectionClose(code, message).write(buf);
        this.transport.send(this.remote, toBytes(buf));
        teardown();
    }

    private void teardown() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        for (Stream stream : new ArrayList<>(this.streams.values())) {
            stream.internalClose(false);
        }
        this.streams.clear();
        if (this.closeHandler != null) {
            this.closeHandler.run();
        }
    }

    void removeStream(long streamId) {
        this.streams.remove(streamId);
    }

    public boolean isClosed() {
        return this.closed;
    }

    private static byte[] toBytes(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        buf.release();
        return bytes;
    }

    @FunctionalInterface
    public interface Transport {
        void send(InetSocketAddress address, byte[] data);
    }
}
