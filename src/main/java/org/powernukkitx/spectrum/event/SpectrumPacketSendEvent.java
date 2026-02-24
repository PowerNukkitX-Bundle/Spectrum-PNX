package org.powernukkitx.spectrum.event;

import cn.nukkit.event.Cancellable;
import cn.nukkit.event.plugin.PluginEvent;
import cn.nukkit.plugin.Plugin;
import lombok.Getter;
import lombok.Setter;
import org.powernukkitx.spectrum.api.packet.SpectrumPacket;

@Getter
@Setter
public class SpectrumPacketSendEvent extends PluginEvent implements Cancellable {

    private SpectrumPacket packet;

    public SpectrumPacketSendEvent(Plugin plugin, SpectrumPacket packet) {
        super(plugin);
        this.packet = packet;
    }
}
