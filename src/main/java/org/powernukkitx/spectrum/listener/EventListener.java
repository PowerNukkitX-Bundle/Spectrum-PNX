package org.powernukkitx.spectrum.listener;

import org.powernukkitx.Server;
import org.powernukkitx.event.EventHandler;
import org.powernukkitx.event.Listener;
import org.powernukkitx.event.network.NetworkRegisterEvent;
import org.powernukkitx.utils.ConfigSection;
import org.powernukkitx.spectrum.Spectrum;
import org.powernukkitx.spectrum.network.SpectrumInterface;

import java.io.IOException;

public class EventListener implements Listener {

    private final Spectrum plugin;

    public EventListener(Spectrum plugin) {
        this.plugin = plugin;
    }

    @EventHandler
    public void onNetworkRegister(NetworkRegisterEvent event) {
        if (!this.plugin.getConfig().exists("spectral")) {
            return;
        }
        ConfigSection section = this.plugin.getConfig().getSection("spectral");
        if (!section.getBoolean("enabled", false)) {
            return;
        }

        Server server = event.getNetworkInterface().getServer();
        String host = section.getString("host", "0.0.0.0");
        int port = section.getInt("port", 0);
        if (port <= 0) {
            port = server.getPort();
        }

        try {
            // the default RakNet interface has already bound the UDP port - free it first
            event.getNetworkInterface().shutdown();
            event.setNetworkInterface(new SpectrumInterface(server, this.plugin, host, port));
        } catch (IOException e) {
            this.plugin.getLogger().error("Failed to start spectral listener, keeping default network", e);
        }
    }
}
