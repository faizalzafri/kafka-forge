package com.github.faizalzafri.kafkaapp;

import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetTracker {

    private final Map<TopicPartition, PartitionState> partitionStates = new ConcurrentHashMap<>();

    private static class PartitionState {
        // Keeps track of all offsets in-flight, sorted ascending
        private final TreeSet<Long> activeOffsets = new TreeSet<>();
        // Keeps track of completed offsets
        private final Set<Long> completedOffsets = new HashSet<>();
        // The highest offset that was part of a contiguous completed sequence
        private long highestContiguousOffset = -1;

        public synchronized void register(long offset) {
            activeOffsets.add(offset);
        }

        public synchronized void complete(long offset) {
            completedOffsets.add(offset);
        }

        /**
         * Finds and returns the next offset to commit (which is highestContiguousOffset + 1).
         * Returns null if no progress has been made since last call.
         */
        public synchronized Long getOffsetToCommit() {
            boolean progress = false;
            while (!activeOffsets.isEmpty()) {
                long smallestActive = activeOffsets.first();
                if (completedOffsets.contains(smallestActive)) {
                    activeOffsets.remove(smallestActive);
                    completedOffsets.remove(smallestActive);
                    highestContiguousOffset = smallestActive;
                    progress = true;
                } else {
                    break;
                }
            }
            return progress ? (highestContiguousOffset + 1) : null;
        }

        /**
         * Clears all tracking state (e.g. on partition revocation)
         */
        public synchronized void clear() {
            activeOffsets.clear();
            completedOffsets.clear();
            highestContiguousOffset = -1;
        }
    }

    public void register(TopicPartition partition, long offset) {
        partitionStates.computeIfAbsent(partition, p -> new PartitionState()).register(offset);
    }

    public void complete(TopicPartition partition, long offset) {
        PartitionState state = partitionStates.get(partition);
        if (state != null) {
            state.complete(offset);
        }
    }

    /**
     * Gathers the next offsets to commit for all tracked partitions.
     * Only partitions that have progressed will be included in the return map.
     */
    public Map<TopicPartition, Long> getOffsetsToCommit() {
        Map<TopicPartition, Long> offsetsToCommit = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionState> entry : partitionStates.entrySet()) {
            Long commitOffset = entry.getValue().getOffsetToCommit();
            if (commitOffset != null) {
                offsetsToCommit.put(entry.getKey(), commitOffset);
            }
        }
        return offsetsToCommit;
    }

    public void clear(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            PartitionState state = partitionStates.get(partition);
            if (state != null) {
                state.clear();
            }
        }
    }
}
