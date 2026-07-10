package org.powernukkitx.spectrum.spectral.protocol;

public final class Protocol {
    public static final byte[] MAGIC = {0x20, 0x24, 0x10, 0x01};

    public static final int PACKET_HEADER_SIZE = 16;
    public static final int MAX_UDP_PAYLOAD_SIZE = 1500;

    public static final int MIN_PACKET_SIZE = 1200;
    public static final int MAX_PACKET_SIZE = 1452;

    public static final long MAX_ACK_DELAY_NANOS = 25_000_000L;
    public static final long TIMER_GRANULARITY_NANOS = 2_000_000L;
    public static final int MAX_ACK_RANGES = 128;

    public static final long INACTIVITY_TIMEOUT_NANOS = 30_000_000_000L;

    // header sequenceID 0 marks an unreliable packet that must not be acked
    public static final long SEQUENCE_UNRELIABLE = 0L;
    public static final long CLIENT_UNKNOWN_CONNECTION_ID = -1L;

    private Protocol() {
    }
}
