package org.powernukkitx.spectrum.network;

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

import cn.nukkit.Player;
import cn.nukkit.Server;
import cn.nukkit.network.NetworkInterface;
import cn.nukkit.network.connection.BedrockPong;
import cn.nukkit.network.connection.BedrockSession;
import cn.nukkit.network.process.NetworkState;
import org.jetbrains.annotations.Nullable;
import oshi.hardware.NetworkIF;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

public final class ProxyInterface implements NetworkInterface {

    @Override
    public NetworkState getState() {
        return null;
    }

    @Override
    public void setState(NetworkState state) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public double getUpload() {
        return 0;
    }

    @Override
    public double getDownload() {
        return 0;
    }

    @Override
    public void resetStatistics() {

    }

    @Override
    public void process() {

    }

    @Override
    public void processInterfaces() {

    }

    @Override
    public Server getServer() {
        return null;
    }

    @Override
    public @Nullable List<NetworkIF> getHardWareNetworkInterfaces() {
        return List.of();
    }

    @Override
    public BedrockSession getSession(InetSocketAddress address) {
        return null;
    }

    @Override
    public void replaceSessionAddress(InetSocketAddress oldAddress, InetSocketAddress newAddress, BedrockSession newSession) {

    }

    @Override
    public void onSessionDisconnect(InetSocketAddress address) {

    }

    @Override
    public int getNetworkLatency(Player player) {
        return 0;
    }

    @Override
    public void blockAddress(InetAddress address) {

    }

    @Override
    public void blockAddress(InetAddress address, int timeout) {

    }

    @Override
    public void unblockAddress(InetAddress address) {

    }

    @Override
    public boolean isAddressBlocked(InetSocketAddress address) {
        return false;
    }

    @Override
    public BedrockPong getPong() {
        return null;
    }
}
