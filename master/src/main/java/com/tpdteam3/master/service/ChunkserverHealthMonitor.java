package com.tpdteam3.master.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Servicio que monitorea continuamente la salud de los chunkservers
 * y mantiene una lista actualizada de servidores activos
 */
@Service
public class ChunkserverHealthMonitor {

    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Estructura para almacenar estado de cada chunkserver
    private final Map<String, ChunkserverStatus> chunkserverStatuses = new ConcurrentHashMap<>();

    // Configuración
    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 10; // Verificar cada 10 segundos
    private static final int HEALTH_CHECK_TIMEOUT_MS = 3000; // Timeout de 3 segundos
    private static final int MAX_CONSECUTIVE_FAILURES = 3; // Marcar como DOWN después de 3 fallos
    private static final int RECOVERY_THRESHOLD = 2; // Marcar como UP después de 2 éxitos

    public ChunkserverHealthMonitor() {
        // Configurar RestTemplate con timeouts
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(HEALTH_CHECK_TIMEOUT_MS);
        factory.setReadTimeout(HEALTH_CHECK_TIMEOUT_MS);
        this.restTemplate = new RestTemplate(factory);
    }

    @PostConstruct
    public void startMonitoring() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🏥 INICIANDO HEALTH MONITOR                          ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("⏱️  Intervalo de verificación: " + HEALTH_CHECK_INTERVAL_SECONDS + " segundos");
        System.out.println("⚠️  Fallos antes de marcar DOWN: " + MAX_CONSECUTIVE_FAILURES);
        System.out.println("✅ Éxitos para marcar UP: " + RECOVERY_THRESHOLD);
        System.out.println();

        // Iniciar monitoreo periódico
        scheduler.scheduleAtFixedRate(
                this::performHealthChecks,
                0, // Iniciar inmediatamente
                HEALTH_CHECK_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    @PreDestroy
    public void stopMonitoring() {
        System.out.println("🛑 Deteniendo Health Monitor...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Registra un nuevo chunkserver para monitoreo
     */
    public void registerChunkserver(String url) {
        if (!chunkserverStatuses.containsKey(url)) {
            chunkserverStatuses.put(url, new ChunkserverStatus(url));
            System.out.println("📋 Chunkserver registrado para monitoreo: " + url);

            // Hacer health check inmediato
            checkChunkserverHealth(url);
        }
    }

    /**
     * Remueve un chunkserver del monitoreo
     */
    public void unregisterChunkserver(String url) {
        chunkserverStatuses.remove(url);
        System.out.println("📋 Chunkserver removido del monitoreo: " + url);
    }

    /**
     * Obtiene lista de chunkservers ACTIVOS (UP)
     */
    public List<String> getHealthyChunkservers() {
        return chunkserverStatuses.values().stream()
                .filter(ChunkserverStatus::isHealthy)
                .map(ChunkserverStatus::getUrl)
                .collect(Collectors.toList());
    }

    /**
     * Obtiene lista de chunkservers INACTIVOS (DOWN)
     */
    public List<String> getUnhealthyChunkservers() {
        return chunkserverStatuses.values().stream()
                .filter(status -> !status.isHealthy())
                .map(ChunkserverStatus::getUrl)
                .collect(Collectors.toList());
    }

    /**
     * Verifica si un chunkserver específico está activo
     */
    public boolean isChunkserverHealthy(String url) {
        ChunkserverStatus status = chunkserverStatuses.get(url);
        return status != null && status.isHealthy();
    }

    /**
     * Obtiene el estado completo de todos los chunkservers
     */
    public Map<String, Map<String, Object>> getDetailedStatus() {
        Map<String, Map<String, Object>> detailedStatus = new HashMap<>();

        for (ChunkserverStatus status : chunkserverStatuses.values()) {
            Map<String, Object> info = new HashMap<>();
            info.put("healthy", status.isHealthy());
            info.put("consecutiveFailures", status.getConsecutiveFailures());
            info.put("consecutiveSuccesses", status.getConsecutiveSuccesses());
            info.put("lastCheckTime", status.getLastCheckTime());
            info.put("lastSuccessTime", status.getLastSuccessTime());
            info.put("lastFailureTime", status.getLastFailureTime());
            info.put("totalChecks", status.getTotalChecks());
            info.put("totalSuccesses", status.getTotalSuccesses());
            info.put("totalFailures", status.getTotalFailures());
            info.put("uptimePercentage", status.getUptimePercentage());

            detailedStatus.put(status.getUrl(), info);
        }

        return detailedStatus;
    }

    /**
     * Realiza health checks a TODOS los chunkservers registrados
     */
    private void performHealthChecks() {
        if (chunkserverStatuses.isEmpty()) {
            return;
        }

        System.out.println("🏥 Ejecutando health checks...");

        List<String> urls = new ArrayList<>(chunkserverStatuses.keySet());

        for (String url : urls) {
            checkChunkserverHealth(url);
        }

        // Mostrar resumen
        List<String> healthy = getHealthyChunkservers();
        List<String> unhealthy = getUnhealthyChunkservers();

        System.out.println("   ✅ Activos: " + healthy.size() + "/" + chunkserverStatuses.size());
        if (!unhealthy.isEmpty()) {
            System.out.println("   ❌ Inactivos: " + unhealthy);
        }
        System.out.println();
    }

    /**
     * Verifica la salud de un chunkserver específico
     */
    private void checkChunkserverHealth(String url) {
        ChunkserverStatus status = chunkserverStatuses.get(url);
        if (status == null) {
            return;
        }

        boolean isHealthy = false;

        try {
            String healthUrl = url + "/api/chunk/health";
            Map<String, String> response = restTemplate.getForObject(healthUrl, Map.class);

            // Verificar que la respuesta sea válida
            isHealthy = response != null && "UP".equals(response.get("status"));

        } catch (Exception e) {
            // Cualquier excepción = chunkserver no disponible
            isHealthy = false;
        }

        // Actualizar estado
        boolean wasHealthy = status.isHealthy();
        status.recordCheck(isHealthy);
        boolean isHealthyNow = status.isHealthy();

        // Detectar cambios de estado
        if (wasHealthy && !isHealthyNow) {
            System.out.println("⚠️  CHUNKSERVER DOWN: " + url);
            System.out.println("   └─ Fallos consecutivos: " + status.getConsecutiveFailures());
        } else if (!wasHealthy && isHealthyNow) {
            System.out.println("✅ CHUNKSERVER RECUPERADO: " + url);
            System.out.println("   └─ Éxitos consecutivos: " + status.getConsecutiveSuccesses());
        }
    }

    /**
     * Clase interna para mantener el estado de un chunkserver
     */
    private class ChunkserverStatus {
        private final String url;
        private int consecutiveFailures = 0;
        private int consecutiveSuccesses = 0;
        private boolean healthy = true; // Optimista: asumimos que está arriba al inicio
        private long lastCheckTime = 0;
        private long lastSuccessTime = 0;
        private long lastFailureTime = 0;
        private int totalChecks = 0;
        private int totalSuccesses = 0;
        private int totalFailures = 0;

        public ChunkserverStatus(String url) {
            this.url = url;
        }

        public synchronized void recordCheck(boolean success) {
            totalChecks++;
            lastCheckTime = System.currentTimeMillis();

            if (success) {
                totalSuccesses++;
                lastSuccessTime = lastCheckTime;
                consecutiveSuccesses++;
                consecutiveFailures = 0;

                // Marcar como healthy si alcanza el umbral de recuperación
                if (!healthy && consecutiveSuccesses >= RECOVERY_THRESHOLD) {
                    healthy = true;
                }
            } else {
                totalFailures++;
                lastFailureTime = lastCheckTime;
                consecutiveFailures++;
                consecutiveSuccesses = 0;

                // Marcar como unhealthy si alcanza el umbral de fallos
                if (healthy && consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
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

        public double getUptimePercentage() {
            if (totalChecks == 0) return 100.0;
            return (totalSuccesses * 100.0) / totalChecks;
        }
    }
}