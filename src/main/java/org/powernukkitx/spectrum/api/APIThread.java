package org.powernukkitx.spectrum.api;

/*
  MIT License

  Copyright (c) 2024 cooldogedev

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.

  @auto-license
 */

import cn.nukkit.Server;
import cn.nukkit.network.connection.util.HandleByteBuf;
import cn.nukkit.plugin.PluginLogger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.powernukkitx.spectrum.Spectrum;
import org.powernukkitx.spectrum.api.packet.ConnectionRequestPacket;
import org.powernukkitx.spectrum.api.packet.ConnectionResponsePacket;
import org.powernukkitx.spectrum.api.packet.SpectrumPacket;
import org.powernukkitx.spectrum.api.packet.PacketIds;
import org.powernukkitx.spectrum.event.SpectrumPacketSendEvent;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentLinkedDeque;

public class APIThread extends Thread {
    private static final int PACKET_LENGTH_SIZE = 4;

    private final PluginLogger logger;
    private final String token;
    private final String address;
    private final int port;

    private boolean running = false;
    private Socket socket;

    private final ConcurrentLinkedDeque<ByteBuf> buffer = new ConcurrentLinkedDeque<>();

    public APIThread(PluginLogger logger, String token, String address, int port) {
        this.logger = logger;
        this.token = token;
        this.address = address;
        this.port = port;
    }

    @Override
    public void start() {
        super.start();

        this.running = true;
    }

    @Override
    public void run() {
        this.running = true;
        this.socket = new Socket();

        try {
            this.socket.setSoTimeout(1); // Socket cannot be non-blocking, but we can set a very short timeout to achieve a similar effect
        } catch (SocketException e) {
            this.logger.error("Failed to set socket timeout: {}", e);
        }

        this.connect();

        ConnectionRequestPacket requestPacket = new ConnectionRequestPacket();
        requestPacket.setToken(token);

        this.sendPacketImmediately(requestPacket);

        ByteBuf connectionResponse = this.read();
        if (connectionResponse == null) {
            this.logger.error("Failed to receive connection response.");
            return;
        }

        int connectionResponseId = connectionResponse.readInt();
        if (connectionResponseId != PacketIds.CONNECTION_RESPONSE) {
            this.logger.error("Received invalid connection response packet with id: " + connectionResponseId);
            return;
        }

        ConnectionResponsePacket packet = new ConnectionResponsePacket();
        packet.decode0(HandleByteBuf.of(connectionResponse));

        if (packet.response != ConnectionResponsePacket.RESPONSE_SUCCESS) {
            this.logger.error("Connection request was rejected by the server with response code: " + packet.response);
            return;
        }

        this.logger.info("Successfully connected to the API server.");
        while (this.running) {
            synchronized (this) {
                if (this.running && this.buffer.isEmpty()) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            this.flush();
        }

        try {
            this.socket.close();
        } catch (IOException e) {
            // Ignore, we're shutting down anyway
        }
        this.logger.debug("Disconnected from API");
    }

    @Override
    public void interrupt() {
        synchronized (this) {
            this.running = false;
            this.notify();
        }
        super.interrupt();
    }

    private ByteBuf read() {
        ByteBuf lengthBytes = this.internalRead(APIThread.PACKET_LENGTH_SIZE);
        if (lengthBytes == null) {
            return null;
        }

        int length;
        try {
            length = lengthBytes.readInt();
        } catch (IndexOutOfBoundsException e) {
            return null; // equivalent to BinaryDataException - not enough bytes to read an int
        } finally {
            lengthBytes.release();
        }

        return this.internalRead(length);
    }

    private ByteBuf internalRead(int length) {
        try {
            InputStream in = this.socket.getInputStream();
            byte[] data = new byte[length];
            int bytes = in.read(data, 0, length);

            if (bytes == -1) {
                return null; // Socket closed
            }

            return Unpooled.wrappedBuffer(data, 0, bytes);
        } catch (IOException e) {
            return null;
        }
    }

    public void sendPacket(SpectrumPacket packet) {
        if (!this.running) {
            return;
        }

        SpectrumPacketSendEvent event = new SpectrumPacketSendEvent(Spectrum.get(), packet);

        Server.getInstance().getPluginManager().callEvent(event);

        if (event.isCancelled()) return;

        ByteBuf buf = Unpooled.buffer();
        packet.encode0(HandleByteBuf.of(buf));

        synchronized (this) {
            this.buffer.addLast(buf);
            this.notify();
        }
    }

    private void flush() {
        while (this.running) {
            ByteBuf payload = this.buffer.pollFirst();
            if (payload == null) break;

            try {
                ByteBuf buf = Unpooled.buffer();
                buf.writeInt(payload.readableBytes());
                buf.writeBytes(payload);

                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                this.socket.getOutputStream().write(data);

                buf.release();
                payload.release();
            } catch (IOException e) {
                this.buffer.addFirst(payload);
                this.connect();
                this.flush();
            }
        }
    }

    private void connect() {
        while (true) {
            try {
                this.socket.connect(new InetSocketAddress(this.address, this.port));
                this.logger.debug("Socket successfully connected");
                return;
            } catch (IOException e) {
                if (!this.running) {
                    return;
                }
                this.logger.debug("Socket failed to connect due to: {}, retrying again in 3 seconds...", e);
                try {
                    this.wait(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public void sendPacketImmediately(SpectrumPacket packet) {
        sendPacket(packet);
        flush();
    }
}
