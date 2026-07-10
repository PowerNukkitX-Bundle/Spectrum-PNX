package org.powernukkitx.spectrum.spectral.reliable;

import org.powernukkitx.spectrum.spectral.protocol.Protocol;

public final class RTT {
    private static final long INITIAL_RTT = 333_000_000L;
    private static final long MIN_RTO = 2_000_000L;

    private long smoothed = INITIAL_RTT;
    private long variation = INITIAL_RTT / 2;
    private long min = INITIAL_RTT;
    private boolean measured = false;

    public void add(long latest, long ackDelay) {
        if (latest < this.min) {
            this.min = latest;
        }

        if (!this.measured) {
            this.smoothed = latest;
            this.variation = latest / 2;
            this.min = latest;
            this.measured = true;
            return;
        }

        long adjusted = latest;
        long maxAckDelay = Math.min(ackDelay, Protocol.MAX_ACK_DELAY_NANOS);
        if (latest >= this.min + maxAckDelay) {
            adjusted -= ackDelay;
        }

        this.variation = (3 * this.variation + Math.abs(this.smoothed - adjusted)) / 4;
        this.smoothed = (7 * this.smoothed + adjusted) / 8;
    }

    public long smoothed() {
        return this.smoothed;
    }

    public long rto() {
        return this.smoothed + Math.max(4 * this.variation, MIN_RTO) + Protocol.MAX_ACK_DELAY_NANOS;
    }
}
