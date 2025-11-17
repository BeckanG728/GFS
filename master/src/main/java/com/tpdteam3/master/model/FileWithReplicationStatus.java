package com.tpdteam3.master.model;

/**
 * Modelo para combinar archivo y su estado de replicación.
 */
public class FileWithReplicationStatus {
    private final FileMetadata file;
    private final ReplicationStatus status;

    public FileWithReplicationStatus(FileMetadata file, ReplicationStatus status) {
        this.file = file;
        this.status = status;
    }

    public FileMetadata getFile() {
        return file;
    }

    public ReplicationStatus getStatus() {
        return status;
    }
}