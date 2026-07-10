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

package org.powernukkitx.spectrum.network;

import org.powernukkitx.Player;
import org.powernukkitx.Server;
import org.powernukkitx.config.category.NetworkSettings;
import org.powernukkitx.event.server.ServerBotnetAttackEvent;
import org.powernukkitx.network.NetworkConstants;
import org.powernukkitx.network.NetworkInterface;
import org.powernukkitx.network.process.NetworkPacketHandler;
import org.powernukkitx.network.process.NetworkState;
import org.powernukkitx.network.process.PlayerSessionHolder;
import org.powernukkitx.network.query.codec.QueryPacketCodec;
import org.powernukkitx.network.query.handler.QueryPacketHandler;
import org.powernukkitx.network.security.BotnetDetector;
import org.powernukkitx.plugin.InternalPlugin;
import org.powernukkitx.utils.Utils;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudburstmc.netty.channel.raknet.RakChannelFactory;
import org.cloudburstmc.netty.channel.raknet.RakServerChannel;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import org.cloudburstmc.netty.channel.raknet.config.RakServerCookieMode;
import org.cloudburstmc.protocol.bedrock.BedrockPong;
import org.cloudburstmc.protocol.bedrock.BedrockServerSession;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodec;
import org.cloudburstmc.protocol.bedrock.netty.initializer.BedrockServerInitializer;
import org.jetbrains.annotations.Nullable;
import oshi.SystemInfo;
import oshi.hardware.NetworkIF;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author MagicDroidX (Nukkit Project)
 */
@Slf4j
public class ProxyInterface implements NetworkInterface {
    @Getter
    private final Server server;
    private final LinkedList<NetWorkStatisticData> netWorkStatisticDataList = new LinkedList<>();
    private final AtomicReference<List<NetworkIF>> hardWareNetworkInterfaces = new AtomicReference<>(null);
    private final Map<InetSocketAddress, BedrockServerSession> sessionMap = new ConcurrentHashMap<>();
    private final Map<InetAddress, LocalDateTime> blockIpMap = new ConcurrentHashMap<>();
    private final RakServerChannel channel;
    @Getter(onMethod_ = {@Override})
    private final BotnetDetector botnetDetector;
    @Getter
    private BedrockPong pong;
    @Getter @Setter
    private NetworkState state = NetworkState.STARTING;

    public ProxyInterface(Server server) {
        this(server, Runtime.getRuntime().availableProcessors(), new ThreadFactoryBuilder().setNameFormat("Netty Server IO #%d").build());
    }

    public ProxyInterface(Server server, int nettyThreadNumber, ThreadFactory threadFactory) {
        this.server = server;
        var bns = server.getSettings().networkSettings().botnetSettings();
        this.botnetDetector = bns.detectionEnabled()
                ? new BotnetDetector(bns.suspiciousThreshold(), bns.minSuspiciousIps(), bns.minScore())
                : null;
        server.getScheduler().scheduleTask(InternalPlugin.INSTANCE, () -> {
            List<NetworkIF> tmpIfs = null;
            try {
                tmpIfs = new SystemInfo().getHardware().getNetworkIFs();
            } catch (Throwable t) {
                log.warn(Server.getInstance().getLanguage().get("nukkit.start.hardwareMonitorDisabled"));
            }
            hardWareNetworkInterfaces.set(tmpIfs);
        }, true);

        // We only use NioDatagram and NioEventLoopGroup in ProxyInterface
        Class<? extends DatagramChannel> oclass = NioDatagramChannel.class;
        EventLoopGroup eventloopgroup = new NioEventLoopGroup(nettyThreadNumber, threadFactory);
        InetSocketAddress bindAddress = new InetSocketAddress(Strings.isNullOrEmpty(this.server.getIp()) ? "0.0.0.0" : this.server.getIp(), this.server.getPort());

        final BedrockCodec codec = NetworkConstants.CODEC;

        this.pong = new BedrockPong()
                .edition("MCPE")
                .motd(server.getMotd())
                .subMotd(server.getSubMotd())
                .playerCount(server.getOnlinePlayers().size())
                .maximumPlayerCount(server.getMaxPlayers())
                .serverId(UUID.randomUUID().getMostSignificantBits())
                .gameType(Server.getGamemodeString(server.getDefaultGamemode(), true))
                .nintendoLimited(false)
                .protocolVersion(codec.getProtocolVersion())
                .ipv4Port(server.getPort())
                .ipv6Port(server.getPort());

        final NetworkSettings net = server.getSettings().networkSettings();
        this.channel = (RakServerChannel) new ServerBootstrap()
                .channelFactory(RakChannelFactory.server(oclass))
                .option(RakChannelOption.RAK_ADVERTISEMENT, getAdvertisement())
                .option(RakChannelOption.RAK_SUPPORTED_PROTOCOLS, new int[]{codec.getRaknetProtocolVersion()})
                .option(RakChannelOption.RAK_PACKET_LIMIT, net.packetLimit())
                .option(RakChannelOption.RAK_SERVER_COOKIE_MODE, RakServerCookieMode.ACTIVE)
                .childOption(RakChannelOption.RAK_AUTO_FLUSH, net.autoFlush())
                .childOption(RakChannelOption.RAK_FLUSH_INTERVAL, net.flushInterval())
                .childOption(RakChannelOption.RAK_MAX_QUEUED_BYTES, net.maxQueuedBytes())
                .group(eventloopgroup)
                .childHandler(new BedrockServerInitializer() {
                    @Override
                    protected void postInitChannel(Channel channel) {
                        if (ProxyInterface.this.server.getSettings().networkSettings().enableQuery()) {
                            channel.pipeline().addLast("queryPacketCodec", new QueryPacketCodec())
                                    .addLast("queryPacketHandler", new QueryPacketHandler(address -> ProxyInterface.this.server.getQueryInformation()));
                        }
                    }

                    @Override
                    protected void initSession(BedrockServerSession session) {
                        final InetSocketAddress address = (InetSocketAddress) session.getSocketAddress();
                        if (ProxyInterface.this.getState() == NetworkState.STARTING || ProxyInterface.this.getState() == NetworkState.STOPPING) {
                            return;
                        }
                        if (isAddressBlocked(address)) {
                            session.close("Your IP address has been blocked by this server!");
                            onSessionDisconnect(address);
                        } else {
                            session.setCodec(NetworkConstants.CODEC);
                            session.setPacketHandler(new NetworkPacketHandler(server,
                                    new PlayerSessionHolder(
                                            session,
                                            ProxyInterface.this.server.getSettings().networkSettings().rateLimitSettings()
                                    )
                            ));
                            ProxyInterface.this.sessionMap.put(address, session);
                        }
                    }
                })
                .bind(bindAddress)
                .awaitUninterruptibly()
                .channel();
    }

    record NetWorkStatisticData(long upload, long download) {}

    public void shutdown() {
        this.channel.close();
        this.pong = null;
        this.sessionMap.clear();
        this.netWorkStatisticDataList.clear();
    }

    public double getUpload() {
        if (netWorkStatisticDataList.size() < 2) return 0;
        return netWorkStatisticDataList.get(1).upload - netWorkStatisticDataList.get(0).upload;
    }

    public double getDownload() {
        if (netWorkStatisticDataList.size() < 2) return 0;
        return netWorkStatisticDataList.get(1).download - netWorkStatisticDataList.get(0).download;
    }

    public void resetStatistics() {
        long upload = 0;
        long download = 0;
        if (netWorkStatisticDataList.size() > 1) {
            netWorkStatisticDataList.removeFirst();
        }
        if (this.getHardWareNetworkInterfaces() != null) {
            for (var networkIF : this.getHardWareNetworkInterfaces()) {
                networkIF.updateAttributes();
                upload += networkIF.getBytesSent();
                download += networkIF.getBytesRecv();
            }
        }
        netWorkStatisticDataList.add(new NetWorkStatisticData(upload, download));
    }

    /**
     * process tick for all network interfaces.
     */
    public void processInterfaces() {
        try {
            this.process();
        } catch (Exception e) {
            log.error(this.server.getLanguage().tr("nukkit.server.networkError", this.getClass().getName(), Utils.getExceptionMessage(e)), e);
        }
    }

    public @Nullable List<NetworkIF> getHardWareNetworkInterfaces() {
        return hardWareNetworkInterfaces.get();
    }

    /**
     * Get network latency for specific player.
     *
     * @param player the player
     * @return the network latency
     */
    public int getNetworkLatency(Player player) {
        return -1;
    }

    /**
     * Block an address forever.
     *
     * @param address the address
     */
    public void blockAddress(InetAddress address) {
        blockIpMap.put(address, LocalDateTime.of(9999, 1, 1, 0, 0));
    }

    /**
     * Block an address.
     *
     * @param address the address
     * @param timeout the timeout, unit millisecond
     */
    public void blockAddress(InetAddress address, int timeout) {
        blockIpMap.put(address, LocalDateTime.now().plus(timeout, ChronoUnit.MILLIS));
    }

    /**
     * Recover an address of banned.
     *
     * @param address the address
     */
    public void unblockAddress(InetAddress address) {
        blockIpMap.remove(address);
    }

    /**
     * Get a session of player.
     *
     * @param address the address of session
     * @return the session
     */
    public BedrockServerSession getSession(InetSocketAddress address) {
        return this.sessionMap.get(address);
    }

    /**
     * Replace session address.
     * <p>
     * handle a scenario that the player from proxy
     *
     * @param oldAddress the old address
     * @param newAddress the new address, usually the IP of the proxy
     * @param newSession original session
     */
    @Override
    public void replaceSessionAddress(InetSocketAddress oldAddress, InetSocketAddress newAddress, BedrockServerSession newSession) {
        if (!this.sessionMap.containsKey(oldAddress))
            return;

        if (isAddressBlocked(newAddress))
            return;

        onSessionDisconnect(oldAddress);
        this.sessionMap.put(newAddress, newSession);
    }

    /**
     * Whether the address is blocked
     */
    public boolean isAddressBlocked(InetSocketAddress address) {
        LocalDateTime until = this.blockIpMap.get(address.getAddress());
        return until != null && LocalDateTime.now().isBefore(until);
    }

    /**
     * A function of tick for network session
     */
    public void process() {
        if (botnetDetector != null) {
            var bns = server.getSettings().networkSettings().botnetSettings();
            botnetDetector.tick().ifPresent(report -> {
                ServerBotnetAttackEvent event = new ServerBotnetAttackEvent(
                        report, bns.autoBlock(), bns.autoBlockDurationSeconds());
                server.getPluginManager().callEvent(event);
                if (event.isAutoBlock()) {
                    int durationMs = event.getBlockDurationSeconds() * 1000;
                    for (var ip : event.getSuspiciousAddresses()) {
                        blockAddress(ip, durationMs);
                    }
                }
            });
        }
    }

    /**
     * Call on session disconnect.
     */
    public void onSessionDisconnect(InetSocketAddress address) {
        this.sessionMap.remove(address);
    }

    @Override
    public void updatePong(BedrockPong pong) {
        this.pong = pong;
        this.channel.config().setAdvertisement(this.pong.toByteBuf());
    }

    /**
     * Retrieves RakNet advertisement.
     * @return Byte buffer
     */
    private ByteBuf getAdvertisement() {
        if (state == NetworkState.STARTING || state == NetworkState.STOPPING) {
            return Unpooled.EMPTY_BUFFER;
        }
        return pong.toByteBuf();
    }
}
