package org.powernukkitx.spectrum.spectral.reliable;

import org.powernukkitx.spectrum.spectral.protocol.Frame;
import org.powernukkitx.spectrum.spectral.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class AckQueue {
    private final TreeMap<Long, Long> ranges = new TreeMap<>();
    private long max = 0L;
    private long maxArrivalNanos = 0L;
    private long nextAckNanos = 0L;
    private boolean armed = false;

    public void add(long sequenceId, long nowNanos) {
        if (sequenceId > this.max) {
            this.max = sequenceId;
            this.maxArrivalNanos = nowNanos;
        }

        // already covered?
        Map.Entry<Long, Long> floor = this.ranges.floorEntry(sequenceId);
        if (floor != null && sequenceId <= floor.getValue()) {
            arm(nowNanos);
            return;
        }

        this.ranges.put(sequenceId, sequenceId);
        merge();

        arm(nowNanos);
    }

    private void arm(long nowNanos) {
        if (!this.armed) {
            this.armed = true;
            this.nextAckNanos = nowNanos + Protocol.MAX_ACK_DELAY_NANOS - Protocol.TIMER_GRANULARITY_NANOS;
        }
    }

    private void merge() {
        Long key = this.ranges.firstKey();
        while (key != null) {
            Map.Entry<Long, Long> higher = this.ranges.higherEntry(key);
            if (higher == null) {
                break;
            }
            long end = this.ranges.get(key);
            if (end + 1 >= higher.getKey()) {
                long mergedEnd = Math.max(end, higher.getValue());
                this.ranges.put(key, mergedEnd);
                this.ranges.remove(higher.getKey());
            } else {
                key = higher.getKey();
            }
        }
    }

    public boolean hasPending() {
        return !this.ranges.isEmpty();
    }

    public boolean shouldFlush(long nowNanos) {
        return this.armed && !this.ranges.isEmpty() && nowNanos >= this.nextAckNanos;
    }

    public List<Frame.Acknowledgement> drain(long nowNanos) {
        List<Frame.Acknowledgement> acks = new ArrayList<>();
        if (this.ranges.isEmpty()) {
            return acks;
        }

        long delayMicros = Math.max(0L, (nowNanos - this.maxArrivalNanos) / 1000L);

        List<long[]> all = new ArrayList<>();
        for (Map.Entry<Long, Long> e : this.ranges.entrySet()) {
            all.add(new long[]{e.getKey(), e.getValue()});
        }

        for (int i = 0; i < all.size(); i += Protocol.MAX_ACK_RANGES) {
            int end = Math.min(i + Protocol.MAX_ACK_RANGES, all.size());
            int count = end - i;
            long[] starts = new long[count];
            long[] ends = new long[count];
            for (int j = 0; j < count; j++) {
                starts[j] = all.get(i + j)[0];
                ends[j] = all.get(i + j)[1];
            }
            acks.add(new Frame.Acknowledgement(delayMicros, this.max, starts, ends));
        }

        this.ranges.clear();
        this.armed = false;
        return acks;
    }
}
