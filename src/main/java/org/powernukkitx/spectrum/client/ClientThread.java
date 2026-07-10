package org.powernukkitx.spectrum.client;

import org.powernukkitx.plugin.PluginLogger;
import org.powernukkitx.spectrum.spectral.Listener;

import java.io.IOException;

/**
 * Hosts the spectral {@link Listener} on a dedicated thread. All spectral state is touched
 * only from this thread; the main server thread interacts via {@link #execute(Runnable)}.
 */
public final class ClientThread extends Thread {
    private final PluginLogger logger;
    private final Bridge bridge;
    private final Listener listener;
    private volatile boolean running = true;

    public ClientThread(PluginLogger logger, Bridge bridge, String host, int port) throws IOException {
        super("Spectrum Spectral IO");
        this.logger = logger;
        this.bridge = bridge;
        this.listener = Listener.listen(host, port);
        this.listener.setConnectionAcceptor(connection -> connection.setStreamAcceptor(stream -> {
            Client client = new Client(stream);
            client.setReader(packet -> this.bridge.onClientPacket(client, packet));
            client.setCloseHandler(() -> this.bridge.onClientClose(client));
            this.bridge.onClientOpen(client);
        }));
    }

    /**
     * Runs a task on the spectral thread (safe entry point from the main thread).
     */
    public void execute(Runnable task) {
        this.listener.execute(task);
    }

    @Override
    public void run() {
        while (this.running) {
            try {
                this.listener.tick();
            } catch (Throwable t) {
                this.logger.error("Error in spectral tick", t);
            }
        }
        this.listener.close();
    }

    public void shutdown() {
        this.running = false;
        this.listener.execute(() -> {
        });
    }
}
