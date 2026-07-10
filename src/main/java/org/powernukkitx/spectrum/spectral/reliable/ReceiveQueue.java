package org.powernukkitx.spectrum.spectral.reliable;

import java.util.HashSet;
import java.util.Set;

public final class ReceiveQueue {
    private long expected = 1L;
    private final Set<Long> outOfOrder = new HashSet<>();

    /**
     * @return true if this sequence is new (frames should be processed), false if duplicate.
     */
    public boolean receive(long sequenceId) {
        if (sequenceId < this.expected || this.outOfOrder.contains(sequenceId)) {
            return false;
        }

        if (sequenceId == this.expected) {
            this.expected++;
            while (this.outOfOrder.remove(this.expected)) {
                this.expected++;
            }
        } else {
            this.outOfOrder.add(sequenceId);
        }
        return true;
    }
}
