package org.powernukkitx.spectrum;

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

import org.powernukkitx.plugin.PluginBase;
import org.powernukkitx.utils.ConfigSection;
import org.powernukkitx.spectrum.api.APIThread;
import org.powernukkitx.spectrum.api.packet.KickPacket;
import org.powernukkitx.spectrum.api.packet.TransferPacket;
import org.powernukkitx.spectrum.listener.EventListener;

import java.util.HashSet;
import java.util.Set;

public class Spectrum extends PluginBase {
    protected APIThread apiThread = null;

    private static Spectrum instance;

    // packet ids the proxy must decode; default is decode-all, entries here opt out
    private final Set<Integer> decodeDisabled = new HashSet<>();

    public static Spectrum get() {
        return instance;
    }

    public APIThread getApiThread() {
        return this.apiThread;
    }

    public boolean shouldPacketDecode(int packetId) {
        return !this.decodeDisabled.contains(packetId);
    }

    public void registerPacketDecode(int packetId, boolean decode) {
        if (decode) {
            this.decodeDisabled.remove(packetId);
        } else {
            this.decodeDisabled.add(packetId);
        }
    }

    @Override
    public void onEnable() {
        instance = this;
        this.saveDefaultConfig();
        if (this.getConfig().exists("api")) {
            ConfigSection section = this.getConfig().getSection("api");
            if (section.getBoolean("enabled", true)) {
                registerAPIThread(section);
            }
        }

        getServer().getPluginManager().registerEvents(new EventListener(this), this);
    }

    @Override
    public void onDisable() {
        if (this.apiThread != null) {
            this.apiThread.interrupt();
            this.apiThread = null;
        }
    }

    public boolean transfer(String username, String address) {
        if (this.apiThread == null) {
            return false;
        }

        TransferPacket packet = new TransferPacket();
        packet.setAddress(address);
        packet.setUsername(username);
        this.apiThread.sendPacket(packet);
        return true;
    }

    public boolean kick(String username, String reason) {
        if (this.apiThread == null) {
            return false;
        }

        KickPacket packet = new KickPacket();
        packet.setReason(reason);
        packet.setUsername(username);
        this.apiThread.sendPacket(packet);
        return true;
    }

    private void registerAPIThread(ConfigSection section) {
        this.apiThread = new APIThread(
                this.getLogger(),
                section.getString("token"),
                section.getString("address"),
                section.getInt("port")
        );
        this.apiThread.start();
    }
}
