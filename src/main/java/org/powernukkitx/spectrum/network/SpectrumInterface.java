package org.powernukkitx.spectrum.network;

/*
  MIT License - Copyright (c) 2024 cooldogedev. @auto-license

  Java port of Spectrum's network interface. PowerNukkitX becomes a downstream server that
  the Spectrum proxy dials into over the spectral transport. Each spectral stream is one
  player; login is synthesised from the identity the proxy already validated upstream.
 */

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudburstmc.protocol.bedrock.BedrockPong;
import org.cloudburstmc.protocol.bedrock.BedrockServerSession;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodec;
import org.cloudburstmc.protocol.bedrock.data.skin.SerializedSkin;
import org.cloudburstmc.protocol.bedrock.util.ChainValidationResult;
import org.cloudburstmc.protocol.common.util.VarInts;
import org.jose4j.jwt.JwtClaims;
import org.powernukkitx.Player;
import org.powernukkitx.Server;
import org.powernukkitx.network.NetworkConstants;
import org.powernukkitx.network.NetworkInterface;
import org.powernukkitx.network.process.NetworkPacketHandler;
import org.powernukkitx.network.process.NetworkState;
import org.powernukkitx.network.process.PlayerSessionHolder;
import org.powernukkitx.network.process.auth.ClientChainData;
import org.powernukkitx.network.process.auth.ClientSkinData;
import org.powernukkitx.network.security.BotnetDetector;
import org.powernukkitx.plugin.PluginLogger;
import org.powernukkitx.spectrum.Spectrum;
import org.powernukkitx.spectrum.client.Bridge;
import org.powernukkitx.spectrum.client.Client;
import org.powernukkitx.spectrum.client.ClientThread;
import org.powernukkitx.spectrum.client.packet.ConnectionRequestPacket;
import org.powernukkitx.spectrum.client.packet.ConnectionResponsePacket;
import org.powernukkitx.spectrum.client.packet.LatencyPacket;
import org.powernukkitx.spectrum.client.packet.ProxyPacket;
import org.powernukkitx.spectrum.client.packet.ProxyPacketIds;
import org.jetbrains.annotations.Nullable;
import oshi.hardware.NetworkIF;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class SpectrumInterface implements NetworkInterface, Bridge {
    private static final int PID_MASK = 0x3ff;

    @Getter
    private final Server server;
    private final Spectrum plugin;
    private final PluginLogger logger;
    private final ClientThread clientThread;
    private final BedrockCodec codec = NetworkConstants.CODEC;

    private final Map<Client, ProxySession> sessionsByClient = new ConcurrentHashMap<>();
    private final Map<InetSocketAddress, ProxySession> sessionsByAddress = new ConcurrentHashMap<>();
    private final Map<InetAddress, LocalDateTime> blockIpMap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Runnable> mainThreadQueue = new ConcurrentLinkedQueue<>();

    @Getter
    private BedrockPong pong;
    @Getter
    @Setter
    private NetworkState state = NetworkState.STARTING;

    public SpectrumInterface(Server server, Spectrum plugin, String host, int port) throws IOException {
        this.server = server;
        this.plugin = plugin;
        this.logger = plugin.getLogger();
        this.pong = buildPong(server);
        this.clientThread = new ClientThread(this.logger, this, host, port);
        this.clientThread.start();
        this.state = NetworkState.STARTED;
        this.logger.info("Spectral listener bound on " + host + ":" + port);
    }

    private static BedrockPong buildPong(Server server) {
        BedrockCodec codec = NetworkConstants.CODEC;
        return new BedrockPong()
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
    }

    // ---- Bridge (spectral thread): marshal everything to the main thread ----

    @Override
    public void onClientOpen(Client client) {
        // session is created lazily on the ConnectionRequest packet
    }

    @Override
    public void onClientPacket(Client client, byte[] rawPacket) {
        this.mainThreadQueue.add(() -> handlePacket(client, rawPacket));
    }

    @Override
    public void onClientClose(Client client) {
        this.mainThreadQueue.add(() -> teardown(client));
    }

    // ---- main thread handling ----

    private void handlePacket(Client client, byte[] rawPacket) {
        int id = peekId(rawPacket);
        try {
            if (id >= ProxyPacketIds.CONNECTION_REQUEST) {
                handleProxyPacket(client, id, rawPacket);
            } else {
                ProxySession session = this.sessionsByClient.get(client);
                if (session != null) {
                    session.handleInbound(rawPacket);
                }
            }
        } catch (Exception e) {
            this.logger.error("Error handling spectral packet (id=" + id + ")", e);
            teardown(client);
        }
    }

    private void handleProxyPacket(Client client, int id, byte[] rawPacket) throws Exception {
        ByteBuf buf = Unpooled.wrappedBuffer(rawPacket);
        try {
            switch (id) {
                case ProxyPacketIds.CONNECTION_REQUEST -> {
                    ConnectionRequestPacket pk = new ConnectionRequestPacket();
                    pk.decode(buf);
                    login(client, pk);
                }
                case ProxyPacketIds.LATENCY -> {
                    LatencyPacket pk = new LatencyPacket();
                    pk.decode(buf);
                    latency(client, pk);
                }
                case ProxyPacketIds.DISCONNECT -> teardown(client);
                default -> {
                    // TRANSFER / UPDATE_CACHE / LOGIN are outbound-only or internal
                }
            }
        } finally {
            buf.release();
        }
    }

    private void login(Client client, ConnectionRequestPacket pk) {
        String[] parts = pk.getAddress().split(":");
        InetSocketAddress address = new InetSocketAddress(parts[0], parts.length > 1 ? Integer.parseInt(parts[1]) : 0);

        ProxySession session = new ProxySession(address, this.codec, (bytes, packetId) ->
                this.clientThread.execute(() -> client.write(bytes, this.plugin.shouldPacketDecode(packetId))));

        PlayerSessionHolder holder = new PlayerSessionHolder(session,
                this.server.getSettings().networkSettings().rateLimitSettings());
        session.setPacketHandler(new NetworkPacketHandler(this.server, holder));

        Player.PlayerInfo info;
        try {
            info = buildPlayerInfo(pk);
        } catch (Exception e) {
            this.logger.error("Failed to build player info from proxy login", e);
            return;
        }

        holder.setPlayerInfo(info);
        this.sessionsByClient.put(client, session);
        this.sessionsByAddress.put(address, session);

        holder.doPlayerCreation();
        Player player = holder.getPlayer();
        if (player == null) {
            teardown(client);
            return;
        }

        long entityId = player.getId();
        ConnectionResponsePacket response = new ConnectionResponsePacket();
        response.setRuntimeId(entityId);
        response.setUniqueId(entityId);
        sendProxyPacket(client, response);
    }

    private Player.PlayerInfo buildPlayerInfo(ConnectionRequestPacket pk) throws Exception {
        // identityData is the raw identity-claims payload the proxy validated upstream
        ChainValidationResult identityResult = new ChainValidationResult(false, pk.getIdentityData());
        ChainValidationResult.IdentityClaims identityClaims = identityResult.identityClaims();

        // clientData is the client-data claims JSON (skin, device, input, etc.)
        JwtClaims clientClaims = JwtClaims.parse(pk.getClientData());
        ClientChainData clientChainData = ClientChainData.from(clientClaims);
        SerializedSkin skin = ClientSkinData.readSkin(clientClaims);

        return new Player.PlayerInfo(identityClaims, clientChainData, skin, false);
    }

    private void latency(Client client, LatencyPacket pk) {
        ProxySession session = this.sessionsByClient.get(client);
        if (session == null) {
            return;
        }
        long downstream = System.currentTimeMillis() - pk.getTimestamp();
        long total = downstream + pk.getLatency();
        session.setPing(total);

        LatencyPacket response = new LatencyPacket();
        response.setLatency(total);
        response.setTimestamp(pk.getTimestamp());
        sendProxyPacket(client, response);
    }

    private void sendProxyPacket(Client client, ProxyPacket packet) {
        ByteBuf buf = Unpooled.buffer();
        packet.encode(buf);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        buf.release();
        this.clientThread.execute(() -> client.write(bytes, true));
    }

    private void teardown(Client client) {
        ProxySession session = this.sessionsByClient.remove(client);
        if (session != null) {
            this.sessionsByAddress.remove((InetSocketAddress) session.getSocketAddress());
            try {
                session.close("Disconnected");
            } catch (Exception ignored) {
            }
        }
        this.clientThread.execute(client::close);
    }

    private int peekId(byte[] rawPacket) {
        ByteBuf buf = Unpooled.wrappedBuffer(rawPacket);
        try {
            return VarInts.readUnsignedInt(buf) & PID_MASK;
        } finally {
            buf.release();
        }
    }

    // ---- NetworkInterface ----

    @Override
    public void processInterfaces() {
        Runnable task;
        while ((task = this.mainThreadQueue.poll()) != null) {
            try {
                task.run();
            } catch (Exception e) {
                this.logger.error("Error in spectral main-thread task", e);
            }
        }
        process();
    }

    @Override
    public void process() {
        // proxy handles pings and botnet mitigation upstream
    }

    @Override
    public void shutdown() {
        this.state = NetworkState.STOPPING;
        this.clientThread.shutdown();
        this.sessionsByClient.clear();
        this.sessionsByAddress.clear();
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
    public @Nullable List<NetworkIF> getHardWareNetworkInterfaces() {
        return null;
    }

    @Override
    public BedrockServerSession getSession(InetSocketAddress address) {
        return this.sessionsByAddress.get(address);
    }

    @Override
    public void replaceSessionAddress(InetSocketAddress oldAddress, InetSocketAddress newAddress, BedrockServerSession newSession) {
        ProxySession session = this.sessionsByAddress.remove(oldAddress);
        if (session != null && newSession instanceof ProxySession proxySession) {
            this.sessionsByAddress.put(newAddress, proxySession);
        }
    }

    @Override
    public void onSessionDisconnect(InetSocketAddress address) {
        this.sessionsByAddress.remove(address);
    }

    @Override
    public int getNetworkLatency(Player player) {
        return -1;
    }

    @Override
    public void blockAddress(InetAddress address) {
        this.blockIpMap.put(address, LocalDateTime.of(9999, 1, 1, 0, 0));
    }

    @Override
    public void blockAddress(InetAddress address, int timeout) {
        this.blockIpMap.put(address, LocalDateTime.now().plus(timeout, ChronoUnit.MILLIS));
    }

    @Override
    public void unblockAddress(InetAddress address) {
        this.blockIpMap.remove(address);
    }

    @Override
    public boolean isAddressBlocked(InetSocketAddress address) {
        LocalDateTime until = this.blockIpMap.get(address.getAddress());
        return until != null && LocalDateTime.now().isBefore(until);
    }

    @Override
    public void updatePong(BedrockPong pong) {
        this.pong = pong;
    }

    @Override
    public BotnetDetector getBotnetDetector() {
        return null;
    }
}
