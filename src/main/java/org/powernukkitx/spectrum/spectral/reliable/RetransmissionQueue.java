package org.powernukkitx.spectrum.spectral.reliable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class RetransmissionQueue {
    private static final int MAX_ATTEMPTS = 3;

    public static final class Entry {
        final long sequenceId;
        final byte[] payload; // full datagram bytes, resent verbatim
        long sentNanos;
        int attempts;

        Entry(long sequenceId, byte[] payload, long sentNanos) {
            this.sequenceId = sequenceId;
            this.payload = payload;
            this.sentNanos = sentNanos;
            this.attempts = 1;
        }

        public long sentNanos() {
            return this.sentNanos;
        }

        public int length() {
            return this.payload.length;
        }
    }

    public record Timeout(byte[] resend, boolean dropped, int droppedLength, long originalSentNanos) {
    }

    private final LinkedHashMap<Long, Entry> entries = new LinkedHashMap<>();

    public void add(long sequenceId, byte[] payload, long nowNanos) {
        this.entries.put(sequenceId, new Entry(sequenceId, payload, nowNanos));
    }

    public Entry ack(long sequenceId) {
        return this.entries.remove(sequenceId);
    }

    public boolean isEmpty() {
        return this.entries.isEmpty();
    }

    private Entry oldest() {
        Iterator<Map.Entry<Long, Entry>> it = this.entries.entrySet().iterator();
        return it.hasNext() ? it.next().getValue() : null;
    }

    public Timeout tick(long nowNanos, long rto) {
        Entry entry = oldest();
        if (entry == null || nowNanos - entry.sentNanos < rto) {
            return null;
        }

        this.entries.remove(entry.sequenceId);
        long originalSent = entry.sentNanos;

        if (entry.attempts >= MAX_ATTEMPTS) {
            return new Timeout(null, true, entry.payload.length, originalSent);
        }

        entry.attempts++;
        entry.sentNanos = nowNanos;
        this.entries.put(entry.sequenceId, entry);
        return new Timeout(entry.payload, false, 0, originalSent);
    }

    public long nextTimeoutNanos(long rto) {
        Entry entry = oldest();
        return entry == null ? Long.MAX_VALUE : entry.sentNanos + rto;
    }
}
