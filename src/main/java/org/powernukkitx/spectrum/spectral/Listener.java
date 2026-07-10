package org.powernukkitx.spectrum.spectral;

import io.netty.buffer.Unpooled;
import org.powernukkitx.spectrum.spectral.protocol.Datagram;
import org.powernukkitx.spectrum.spectral.protocol.Frame;
import org.powernukkitx.spectrum.spectral.protocol.FrameIds;
import org.powernukkitx.spectrum.spectral.protocol.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public final class Listener implements Connection.Transport {
    private final DatagramChannel channel;
    private final Selector selector;
    private final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(Protocol.MAX_UDP_PAYLOAD_SIZE);

    private final Map<Long, Connection> byId = new HashMap<>();
    private final Map<InetSocketAddress, Connection> byAddress = new HashMap<>();
    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();

    private long nextConnectionId = 0L;
    private Consumer<Connection> connectionAcceptor;

    private Listener(DatagramChannel channel, Selector selector) {
        this.channel = channel;
        this.selector = selector;
    }

    public static Listener listen(String host, int port) throws IOException {
        DatagramChannel channel = DatagramChannel.open();
        channel.configureBlocking(false);
        channel.socket().setReceiveBufferSize(1024 * 1024 * 7);
        channel.socket().setSendBufferSize(1024 * 1024 * 7);
        channel.bind(new InetSocketAddress(host, port));
        Selector selector = Selector.open();
        channel.register(selector, SelectionKey.OP_READ);
        return new Listener(channel, selector);
    }

    public void setConnectionAcceptor(Consumer<Connection> acceptor) {
        this.connectionAcceptor = acceptor;
    }

    /**
     * Posts a task to run on the listener thread at the start of the next tick.
     */
    public void execute(Runnable task) {
        this.tasks.add(task);
        this.selector.wakeup();
    }

    @Override
    public void send(InetSocketAddress address, byte[] data) {
        try {
            this.channel.send(ByteBuffer.wrap(data), address);
        } catch (IOException ignored) {
            // probe loss / EMSGSIZE expected, swallow
        }
    }

    /**
     * One iteration: drains posted tasks, reads all pending datagrams, ticks every connection.
     * Blocks up to a few ms waiting for readable data.
     */
    public void tick() {
        Runnable task;
        while ((task = this.tasks.poll()) != null) {
            task.run();
        }

        try {
            this.selector.select(5);
            this.selector.selectedKeys().clear();
        } catch (IOException ignored) {
        }

        drainSocket();

        long now = System.nanoTime();
        Iterator<Connection> it = this.byId.values().iterator();
        List<Connection> toRemove = new ArrayList<>();
        while (it.hasNext()) {
            Connection conn = it.next();
            conn.tick(now);
            if (conn.isClosed()) {
                toRemove.add(conn);
            }
        }
        for (Connection conn : toRemove) {
            this.byId.remove(conn.connectionId());
            this.byAddress.remove(conn.remoteAddress());
        }
    }

    private void drainSocket() {
        while (true) {
            this.receiveBuffer.clear();
            InetSocketAddress remote;
            try {
                remote = (InetSocketAddress) this.channel.receive(this.receiveBuffer);
            } catch (IOException e) {
                break;
            }
            if (remote == null) {
                break;
            }
            this.receiveBuffer.flip();
            byte[] data = new byte[this.receiveBuffer.remaining()];
            this.receiveBuffer.get(data);
            route(remote, data);
        }
    }

    private void route(InetSocketAddress remote, byte[] data) {
        var buf = Unpooled.wrappedBuffer(data);
        Long connId = Datagram.peekConnectionId(buf);
        if (connId == null) {
            return;
        }

        Connection conn = this.byId.get(connId);
        if (conn == null && connId == Protocol.CLIENT_UNKNOWN_CONNECTION_ID) {
            conn = this.byAddress.get(remote);
        }

        long now = System.nanoTime();
        if (conn == null) {
            if (!hasConnectionRequest(buf)) {
                return;
            }
            conn = accept(remote, now);
        }

        conn.onDatagram(buf, now);
    }

    private boolean hasConnectionRequest(io.netty.buffer.ByteBuf buf) {
        buf.markReaderIndex();
        try {
            for (Frame frame : Datagram.decodeFrames(buf)) {
                if (frame instanceof Frame.ConnectionRequest) {
                    return true;
                }
            }
            return false;
        } finally {
            buf.resetReaderIndex();
        }
    }

    private Connection accept(InetSocketAddress remote, long now) {
        long id = this.nextConnectionId++;
        Connection conn = new Connection(id, remote, this, now);
        this.byId.put(id, conn);
        this.byAddress.put(remote, conn);
        conn.sendReliableFrame(new Frame.ConnectionResponse(id, FrameIds.RESPONSE_SUCCESS));
        if (this.connectionAcceptor != null) {
            this.connectionAcceptor.accept(conn);
        }
        return conn;
    }

    public void close() {
        for (Connection conn : new ArrayList<>(this.byId.values())) {
            conn.closeWithError(FrameIds.CLOSE_GRACEFUL, "server closing");
        }
        this.byId.clear();
        this.byAddress.clear();
        try {
            this.selector.close();
        } catch (IOException ignored) {
        }
        try {
            this.channel.close();
        } catch (IOException ignored) {
        }
    }
}
