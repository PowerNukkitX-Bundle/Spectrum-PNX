package org.powernukkitx.spectrum.spectral.protocol;

public final class FrameIds {
    public static final int ACKNOWLEDGEMENT = 0;
    public static final int CONNECTION_REQUEST = 1;
    public static final int CONNECTION_RESPONSE = 2;
    public static final int CONNECTION_CLOSE = 3;
    public static final int STREAM_REQUEST = 4;
    public static final int STREAM_RESPONSE = 5;
    public static final int STREAM_DATA = 6;
    public static final int STREAM_CLOSE = 7;
    public static final int MTU_REQUEST = 8;
    public static final int MTU_RESPONSE = 9;

    public static final int RESPONSE_SUCCESS = 0;
    public static final int RESPONSE_FAILED = 1;

    public static final int CLOSE_APPLICATION = 0;
    public static final int CLOSE_GRACEFUL = 1;
    public static final int CLOSE_TIMEOUT = 2;
    public static final int CLOSE_INTERNAL = 3;

    private FrameIds() {
    }
}
