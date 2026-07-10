package org.powernukkitx.spectrum.listener;

import org.powernukkitx.event.EventHandler;
import org.powernukkitx.event.Listener;
import org.powernukkitx.event.network.NetworkRegisterEvent;
import org.powernukkitx.spectrum.network.ProxyInterface;

public class EventListener implements Listener {

    @EventHandler
    public void onNetworkRegister(NetworkRegisterEvent event) {
        event.setNetworkInterface(new ProxyInterface(event.getNetworkInterface().getServer()));
    }
}
