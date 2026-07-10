package org.powernukkitx.spectrum.network;

/*
  MIT License - Copyright (c) 2024 cooldogedev. @auto-license
 */

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.cloudburstmc.protocol.bedrock.BedrockPeer;
import org.cloudburstmc.protocol.bedrock.BedrockServerSession;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodec;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodecHelper;
import org.cloudburstmc.protocol.bedrock.packet.BedrockPacket;
import org.cloudburstmc.protocol.bedrock.packet.BedrockPacketHandler;
import org.cloudburstmc.protocol.common.PacketSignal;
import org.cloudburstmc.protocol.common.util.VarInts;

import javax.crypto.SecretKey;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * A BedrockServerSession that talks to the Spectrum proxy instead of a RakNet peer.
 * Packets are serialized directly with the codec (no netty pipeline) and handed to the
 * outbound sink; inbound raw packets are decoded and dispatched to the packet handler.
 */
public final class ProxySession extends BedrockServerSession {

    @FunctionalInterface
    public interface OutboundSink {
        void send(byte[] rawPacket, int packetId);
    }

    private static final int PID_MASK = 0x3ff;

    private final InetSocketAddress address;
    private final OutboundSink sink;
    private BedrockCodec codec;
    private BedrockCodecHelper helper;
    private long ping;

    public ProxySession(InetSocketAddress address, BedrockCodec codec, OutboundSink sink) {
        super(new BedrockPeer(new EmbeddedChannel(), (peer, sub) -> null), 0);
        this.address = address;
        this.codec = codec;
        this.helper = codec.createHelper();
        this.sink = sink;
    }

    @Override
    public BedrockCodec getCodec() {
        return this.codec;
    }

    @Override
    public void setCodec(BedrockCodec codec) {
        this.codec = codec;
        this.helper = codec.createHelper();
    }

    @Override
    public void setCompression(org.cloudburstmc.protocol.bedrock.data.PacketCompressionAlgorithm algorithm) {
        // handled by the spectral stream (per-packet snappy), nothing to do here
    }

    @Override
    public void enableEncryption(SecretKey key) {
        // encryption is terminated at the proxy
    }

    @Override
    public SocketAddress getSocketAddress() {
        return this.address;
    }

    public long getPing() {
        return this.ping;
    }

    public void setPing(long ping) {
        this.ping = ping;
    }

    @Override
    public void sendPacket(BedrockPacket packet) {
        encodeAndSend(packet);
    }

    @Override
    public void sendPacketImmediately(BedrockPacket packet) {
        encodeAndSend(packet);
    }

    private void encodeAndSend(BedrockPacket packet) {
        int id = this.codec.getPacketDefinition(packet.getClass()).getId();
        ByteBuf buf = Unpooled.buffer();
        try {
            VarInts.writeUnsignedInt(buf, id & PID_MASK);
            this.codec.tryEncode(this.helper, buf, packet);
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            this.sink.send(bytes, id);
        } finally {
            buf.release();
        }
    }

    /**
     * Decodes a single raw bedrock packet forwarded by the proxy and dispatches it.
     */
    public void handleInbound(byte[] rawPacket) {
        ByteBuf buf = Unpooled.wrappedBuffer(rawPacket);
        try {
            int header = VarInts.readUnsignedInt(buf);
            int id = header & PID_MASK;
            BedrockPacket packet = this.codec.tryDecode(this.helper, buf, id);
            BedrockPacketHandler handler = this.getPacketHandler();
            if (handler != null && handler.handlePacket(packet) == PacketSignal.UNHANDLED) {
                // ignored - matches BedrockSession default behaviour
            }
        } finally {
            buf.release();
        }
    }
}
