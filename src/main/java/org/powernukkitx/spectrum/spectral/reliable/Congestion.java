package org.powernukkitx.spectrum.spectral.reliable;

import org.powernukkitx.spectrum.spectral.protocol.Protocol;

public final class Congestion {
    private int mss = Protocol.MIN_PACKET_SIZE;
    private long window;
    private long minWindow;
    private long ssthresh = Long.MAX_VALUE;
    private long flight = 0L;
    private long bytesAcked = 0L;
    private long recoveryStartNanos = 0L;
    private boolean recoverySend = false;

    public Congestion() {
        recomputeWindows();
    }

    private void recomputeWindows() {
        this.minWindow = 2L * this.mss;
        long initial = Math.max(this.minWindow, Math.min(14720L, 10L * this.mss));
        this.window = Math.max(this.window, initial);
    }

    public void setMss(int mss) {
        this.mss = mss;
        recomputeWindows();
    }

    public int mss() {
        return this.mss;
    }

    public boolean canSend(int length) {
        if (this.recoverySend) {
            return true;
        }
        return this.flight + length <= this.window;
    }

    public void onSend(int length) {
        this.flight += length;
        this.recoverySend = false;
    }

    public void onAck(int length) {
        this.flight = Math.max(0L, this.flight - length);
        if (this.window < this.ssthresh) {
            this.window += Math.min(length, this.mss);
        } else {
            this.bytesAcked += length;
            if (this.bytesAcked >= this.window) {
                this.bytesAcked -= this.window;
                this.window += this.mss;
            }
        }
    }

    public void releaseFlight(int length) {
        this.flight = Math.max(0L, this.flight - length);
    }

    public void onCongestionEvent(long nowNanos, long sentNanos) {
        if (sentNanos <= this.recoveryStartNanos) {
            return;
        }
        this.window = Math.max(this.window / 2, this.minWindow);
        this.ssthresh = this.window;
        this.bytesAcked = 0L;
        this.recoveryStartNanos = nowNanos;
        this.recoverySend = true;
    }
}
