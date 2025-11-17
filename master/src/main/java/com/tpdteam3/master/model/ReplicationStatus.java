package com.tpdteam3.master.model;

/**
 * Modelo para almacenar estado de replicación de un archivo.
 */
public class ReplicationStatus {
    private final int totalChunks;
    private final int currentMinReplicas;
    private final int currentMaxReplicas;
    private final int totalReplicas;
    private final int chunksNeedingReplication;
    private static final int TARGET_REPLICATION_FACTOR = 3;

    public ReplicationStatus(int totalChunks, int currentMinReplicas, int currentMaxReplicas,
                          int totalReplicas, int chunksNeedingReplication) {
        this.totalChunks = totalChunks;
        this.currentMinReplicas = currentMinReplicas;
        this.currentMaxReplicas = currentMaxReplicas;
        this.totalReplicas = totalReplicas;
        this.chunksNeedingReplication = chunksNeedingReplication;
    }

    public boolean needsReplication() {
        return chunksNeedingReplication > 0;
    }

    public boolean hasExcessReplicas() {
        return currentMaxReplicas > TARGET_REPLICATION_FACTOR + 1;
    }

    public int getTotalChunks() {
        return totalChunks;
    }

    public int getCurrentMinReplicas() {
        return currentMinReplicas;
    }

    public int getCurrentMaxReplicas() {
        return currentMaxReplicas;
    }

    public int getTotalReplicas() {
        return totalReplicas;
    }

    public int getChunksNeedingReplication() {
        return chunksNeedingReplication;
    }
}