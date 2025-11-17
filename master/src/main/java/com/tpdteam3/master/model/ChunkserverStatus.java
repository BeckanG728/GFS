package com.tpdteam3.master.model;

import java.util.List;
import java.util.Map;

/**
 * Modelo que mantiene el estado de salud de un chunkserver individual.
 * Implementa lógica de detección de fallos con umbral de tolerancia.
 */
public class ChunkserverStatus {
    private final String url;
    private int consecutiveFailures = 0;
    private int consecutiveSuccesses = 0;
    private boolean healthy = true; // Optimista: asumimos UP al inicio
    private long lastCheckTime = 0;
    private long lastSuccessTime = 0;
    private long lastFailureTime = 0;
    private int totalChecks = 0;
    private int totalSuccesses = 0;
    private int totalFailures = 0;
    private Map<String, List<Integer>> lastInventory = null;

    public ChunkserverStatus(String url) {
        this.url = url;
    }

    public synchronized void recordCheck(boolean success, Map<String, List<Integer>> inventory) {
        totalChecks++;
        lastCheckTime = System.currentTimeMillis();

        if (success) {
            totalSuccesses++;
            lastSuccessTime = lastCheckTime;
            consecutiveSuccesses++;
            consecutiveFailures = 0;
            lastInventory = inventory;

            if (!healthy && consecutiveSuccesses >= 2) {
                healthy = true;
            }
        } else {
            totalFailures++;
            lastFailureTime = lastCheckTime;
            consecutiveFailures++;
            consecutiveSuccesses = 0;

            if (healthy && consecutiveFailures >= 3) {
                healthy = false;
            }
        }
    }

    public boolean isHealthy() {
        return healthy;
    }

    public String getUrl() {
        return url;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    public int getConsecutiveSuccesses() {
        return consecutiveSuccesses;
    }

    public long getLastCheckTime() {
        return lastCheckTime;
    }

    public long getLastSuccessTime() {
        return lastSuccessTime;
    }

    public long getLastFailureTime() {
        return lastFailureTime;
    }

    public int getTotalChecks() {
        return totalChecks;
    }

    public int getTotalSuccesses() {
        return totalSuccesses;
    }

    public int getTotalFailures() {
        return totalFailures;
    }

    public Map<String, List<Integer>> getLastInventory() {
        return lastInventory;
    }

    public double getUptimePercentage() {
        if (totalChecks == 0) return 100.0;
        return (totalSuccesses * 100.0) / totalChecks;
    }
}