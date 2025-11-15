package com.tpdteam3.master.model;

import java.util.ArrayList;
import java.util.List;

public class FileMetadata {
    private String imagenId;
    private long size;
    private List<ChunkMetadata> chunks;
    private long timestamp;

    public FileMetadata() {
        this.chunks = new ArrayList<>();
        this.timestamp = System.currentTimeMillis();
    }

    public FileMetadata(String imagenId, long size) {
        this();
        this.imagenId = imagenId;
        this.size = size;
    }

    // Getters y Setters
    public String getImagenId() {
        return imagenId;
    }

    public void setImagenId(String imagenId) {
        this.imagenId = imagenId;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public List<ChunkMetadata> getChunks() {
        return chunks;
    }

    public void setChunks(List<ChunkMetadata> chunks) {
        this.chunks = chunks;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Metadata de un chunk con soporte para múltiples réplicas
     */
    public static class ChunkMetadata {
        private int chunkIndex;
        private String chunkserverId;
        private String chunkserverUrl;
        private int replicaIndex; // 0 = primaria, 1+ = réplicas

        public ChunkMetadata() {
        }

        public ChunkMetadata(int chunkIndex, String chunkserverId, String chunkserverUrl) {
            this.chunkIndex = chunkIndex;
            this.chunkserverId = chunkserverId;
            this.chunkserverUrl = chunkserverUrl;
            this.replicaIndex = 0; // Por defecto es primaria
        }

        public int getChunkIndex() {
            return chunkIndex;
        }

        public void setChunkIndex(int chunkIndex) {
            this.chunkIndex = chunkIndex;
        }

        public String getChunkserverId() {
            return chunkserverId;
        }

        public void setChunkserverId(String chunkserverId) {
            this.chunkserverId = chunkserverId;
        }

        public String getChunkserverUrl() {
            return chunkserverUrl;
        }

        public void setChunkserverUrl(String chunkserverUrl) {
            this.chunkserverUrl = chunkserverUrl;
        }

        public int getReplicaIndex() {
            return replicaIndex;
        }

        public void setReplicaIndex(int replicaIndex) {
            this.replicaIndex = replicaIndex;
        }

        @Override
        public String toString() {
            return "ChunkMetadata{" +
                   "chunkIndex=" + chunkIndex +
                   ", replicaIndex=" + replicaIndex +
                   ", chunkserverUrl='" + chunkserverUrl + '\'' +
                   '}';
        }
    }
}