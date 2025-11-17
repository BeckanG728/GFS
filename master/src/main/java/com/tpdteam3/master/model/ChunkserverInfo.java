package com.tpdteam3.master.model;

/**
 * Modelo para información de chunkserver registrado.
 */
public class ChunkserverInfo {
    private final String url;
    private final String id;
    private final long registrationTime;

    public ChunkserverInfo(String url, String id) {
        this.url = url;
        this.id = id;
        this.registrationTime = System.currentTimeMillis();
    }

    public String getUrl() {
        return url;
    }

    public String getId() {
        return id;
    }

    public long getRegistrationTime() {
        return registrationTime;
    }
}