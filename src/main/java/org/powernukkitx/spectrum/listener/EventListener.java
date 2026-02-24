package org.powernukkitx.spectrum.listener;

import cn.nukkit.event.EventHandler;
import cn.nukkit.event.Listener;
import cn.nukkit.event.network.NetworkRegisterEvent;
import org.powernukkitx.spectrum.network.ProxyInterface;

public class EventListener implements Listener {

    @EventHandler
    public void onNetworkRegister(NetworkRegisterEvent event) {
        event.setNetworkInterface(new ProxyInterface(event.getNetworkInterface().getServer()));
    }
}
