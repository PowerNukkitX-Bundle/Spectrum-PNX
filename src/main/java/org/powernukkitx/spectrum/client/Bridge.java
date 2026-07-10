package org.powernukkitx.spectrum.client;

/**
 * Callbacks fired on the spectral (client) thread. Implementations must marshal to the
 * main server thread themselves.
 */
public interface Bridge {
    void onClientOpen(Client client);

    void onClientPacket(Client client, byte[] rawPacket);

    void onClientClose(Client client);
}
