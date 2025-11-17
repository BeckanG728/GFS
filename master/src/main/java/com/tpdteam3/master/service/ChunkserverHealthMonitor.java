package com.tpdteam3.master.service;

import com.tpdteam3.master.model.ChunkserverStatus;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Servicio que monitorea continuamente la salud e integridad de los chunkservers.
 * <p>
 * Responsabilidades:
 * 1. Verificar que cada chunkserver esté respondiendo (health check)
 * 2. Obtener inventario de chunks de cada servidor
 * 3. Mantener lista actualizada de servidores activos/inactivos
 * 4. Proveer información para que otros servicios puedan actuar
 */
@Service
public class ChunkserverHealthMonitor {

    @Autowired
    private IntegrityMonitorService integrityMonitor;

    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Almacena el estado de salud de cada chunkserver registrado
    private final Map<String, ChunkserverStatus> chunkserverStatuses = new ConcurrentHashMap<>();

    // Configuración de health checks
    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 10; // Verificar cada 10 segundos
    private static final int HEALTH_CHECK_TIMEOUT_MS = 3000;     // Timeout de 3 segundos
    private static final int MAX_CONSECUTIVE_FAILURES = 3;        // Marcar como DOWN después de 3 fallos
    private static final int RECOVERY_THRESHOLD = 2;              // Marcar como UP después de 2 éxitos

    /**
     * Constructor que configura el RestTemplate con timeouts apropiados.
     * Los timeouts evitan que health checks bloqueados afecten el sistema.
     */
    public ChunkserverHealthMonitor() {
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(HEALTH_CHECK_TIMEOUT_MS);
        factory.setReadTimeout(HEALTH_CHECK_TIMEOUT_MS);
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * Inicia el monitoreo periódico de salud al arrancar el servicio.
     * Se ejecuta automáticamente después de la construcción del bean.
     */
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

    /**
     * Detiene el monitoreo al apagar el servicio.
     * Limpia recursos del scheduler de forma ordenada.
     */
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
     * Registra un nuevo chunkserver para monitoreo continuo.
     * Se llama automáticamente cuando un chunkserver se registra en el Master.
     *
     * @param url URL base del chunkserver (ej: http://localhost:9001/chunkserver1)
     */
    public void registerChunkserver(String url) {
        if (!chunkserverStatuses.containsKey(url)) {
            chunkserverStatuses.put(url, new ChunkserverStatus(url));
            System.out.println("📋 Chunkserver registrado para monitoreo: " + url);

            // Hacer health check inmediato para obtener estado inicial
            checkChunkserverHealth(url);
        }
    }

    /**
     * Remueve un chunkserver del monitoreo.
     * Se llama cuando un chunkserver se desregistra del Master.
     *
     * @param url URL base del chunkserver a remover
     */
    public void unregisterChunkserver(String url) {
        chunkserverStatuses.remove(url);
        System.out.println("📋 Chunkserver removido del monitoreo: " + url);
    }

    /**
     * Obtiene lista de URLs de chunkservers que están ACTIVOS (UP).
     * Un servidor está activo si ha respondido exitosamente en los últimos checks.
     *
     * @return Lista de URLs de servidores activos
     */
    public List<String> getHealthyChunkservers() {
        return chunkserverStatuses.values().stream()
                .filter(ChunkserverStatus::isHealthy)
                .map(ChunkserverStatus::getUrl)
                .collect(Collectors.toList());
    }

    /**
     * Obtiene lista de URLs de chunkservers que están INACTIVOS (DOWN).
     * Un servidor está inactivo si ha fallado consecutivamente varios health checks.
     *
     * @return Lista de URLs de servidores inactivos
     */
    public List<String> getUnhealthyChunkservers() {
        return chunkserverStatuses.values().stream()
                .filter(status -> !status.isHealthy())
                .map(ChunkserverStatus::getUrl)
                .collect(Collectors.toList());
    }

    /**
     * Verifica si un chunkserver específico está activo.
     *
     * @param url URL del chunkserver a verificar
     * @return true si está activo, false si está caído o no registrado
     */
    public boolean isChunkserverHealthy(String url) {
        ChunkserverStatus status = chunkserverStatuses.get(url);
        return status != null && status.isHealthy();
    }

    /**
     * Obtiene el estado detallado de todos los chunkservers monitoreados.
     * Incluye métricas como uptime, fallos consecutivos, último check, etc.
     *
     * @return Mapa con URL como clave y métricas detalladas como valor
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
     * ✅ NUEVO: Obtiene el inventario de chunks más reciente de un chunkserver.
     * Este inventario es obtenido durante el último health check exitoso.
     *
     * @param url URL del chunkserver
     * @return Mapa con imagenId -> lista de índices de chunks, o mapa vacío si no disponible
     */
    public Map<String, List<Integer>> getChunkserverInventory(String url) {
        ChunkserverStatus status = chunkserverStatuses.get(url);
        if (status != null && status.getLastInventory() != null) {
            return status.getLastInventory();
        }
        return new HashMap<>();
    }

    /**
     * Ejecuta health checks en TODOS los chunkservers registrados.
     * Se ejecuta periódicamente cada HEALTH_CHECK_INTERVAL_SECONDS segundos.
     * <p>
     * Para cada servidor:
     * 1. Llama a /api/chunk/health
     * 2. Obtiene inventario de chunks
     * 3. Actualiza estado (UP/DOWN)
     * 4. Notifica cambios de estado
     * 5. ✅ Dispara verificación de integridad si detecta cambios
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
     * ✅ MEJORADO: Verifica la salud de un chunkserver específico Y obtiene su inventario.
     * <p>
     * Proceso:
     * 1. Llama a GET /api/chunk/health del chunkserver
     * 2. Verifica que responda con status: "UP"
     * 3. Extrae el inventario de chunks del response
     * 4. Actualiza el estado del servidor
     * 5. Si detecta cambios en inventario, notifica al IntegrityMonitor
     *
     * @param url URL del chunkserver a verificar
     */
    private void checkChunkserverHealth(String url) {
        ChunkserverStatus status = chunkserverStatuses.get(url);
        if (status == null) {
            return;
        }

        boolean isHealthy = false;
        Map<String, List<Integer>> currentInventory = null;

        try {
            String healthUrl = url + "/api/chunk/health";

            // Llamar al health endpoint que ahora incluye inventario
            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.getForObject(healthUrl, Map.class);

            // Verificar que la respuesta sea válida
            isHealthy = response != null && "UP".equals(response.get("status"));

            // ✅ NUEVO: Extraer inventario de chunks si está disponible
            if (isHealthy && response.containsKey("inventory")) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> rawInventory = (Map<String, Object>) response.get("inventory");

                    // Convertir inventario a formato esperado
                    currentInventory = new HashMap<>();
                    for (Map.Entry<String, Object> entry : rawInventory.entrySet()) {
                        String imagenId = entry.getKey();
                        @SuppressWarnings("unchecked")
                        List<Integer> indices = (List<Integer>) entry.getValue();
                        currentInventory.put(imagenId, new ArrayList<>(indices));
                    }
                } catch (Exception e) {
                    System.err.println("⚠️  Error parseando inventario de " + url + ": " + e.getMessage());
                }
            }

        } catch (Exception e) {
            // Cualquier excepción = chunkserver no disponible
            isHealthy = false;
        }

        // Actualizar estado del servidor
        boolean wasHealthy = status.isHealthy();
        Map<String, List<Integer>> previousInventory = status.getLastInventory();

        status.recordCheck(isHealthy, currentInventory);
        boolean isHealthyNow = status.isHealthy();

        // ✅ DETECTAR Y NOTIFICAR CAMBIOS DE ESTADO
        if (wasHealthy && !isHealthyNow) {
            System.out.println("⚠️  CHUNKSERVER DOWN: " + url);
            System.out.println("   └─ Fallos consecutivos: " + status.getConsecutiveFailures());

            // Servidor caído: el IntegrityMonitor manejará la re-replicación
            if (integrityMonitor != null) {
                integrityMonitor.onChunkserverDown(url);
            }

        } else if (!wasHealthy && isHealthyNow) {
            System.out.println("✅ CHUNKSERVER RECUPERADO: " + url);
            System.out.println("   └─ Éxitos consecutivos: " + status.getConsecutiveSuccesses());

            // Servidor recuperado: verificar si tiene los chunks esperados
            if (integrityMonitor != null) {
                integrityMonitor.onChunkserverRecovered(url, currentInventory);
            }
        }

        // ✅ NUEVO: DETECTAR CAMBIOS EN EL INVENTARIO (chunks eliminados manualmente)
        if (isHealthy && currentInventory != null && previousInventory != null) {
            if (!inventoriesMatch(previousInventory, currentInventory)) {
                System.out.println("🔍 CAMBIO EN INVENTARIO DETECTADO: " + url);

                // Analizar qué cambió
                Set<String> removedChunks = findRemovedChunks(previousInventory, currentInventory);
                Set<String> addedChunks = findAddedChunks(previousInventory, currentInventory);

                if (!removedChunks.isEmpty()) {
                    System.out.println("   ❌ Chunks eliminados: " + removedChunks.size());
                    removedChunks.stream().limit(5).forEach(chunk ->
                            System.out.println("      - " + chunk)
                    );
                }

                if (!addedChunks.isEmpty()) {
                    System.out.println("   ✅ Chunks nuevos: " + addedChunks.size());
                }

                // Notificar al IntegrityMonitor para que verifique y repare
                if (integrityMonitor != null && !removedChunks.isEmpty()) {
                    integrityMonitor.onInventoryChanged(url, currentInventory, removedChunks);
                }
            }
        }
    }

    /**
     * Compara dos inventarios para detectar si son idénticos.
     *
     * @param inventory1 Primer inventario
     * @param inventory2 Segundo inventario
     * @return true si son idénticos, false si hay diferencias
     */
    private boolean inventoriesMatch(Map<String, List<Integer>> inventory1,
                                     Map<String, List<Integer>> inventory2) {
        if (inventory1.size() != inventory2.size()) {
            return false;
        }

        for (Map.Entry<String, List<Integer>> entry : inventory1.entrySet()) {
            String imagenId = entry.getKey();
            List<Integer> chunks1 = entry.getValue();
            List<Integer> chunks2 = inventory2.get(imagenId);

            if (chunks2 == null || !new HashSet<>(chunks1).equals(new HashSet<>(chunks2))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Encuentra chunks que fueron removidos entre dos inventarios.
     *
     * @param oldInventory Inventario anterior
     * @param newInventory Inventario nuevo
     * @return Set de identificadores de chunks eliminados (formato: "imagenId_chunk_N")
     */
    private Set<String> findRemovedChunks(Map<String, List<Integer>> oldInventory,
                                          Map<String, List<Integer>> newInventory) {
        Set<String> removed = new HashSet<>();

        for (Map.Entry<String, List<Integer>> entry : oldInventory.entrySet()) {
            String imagenId = entry.getKey();
            List<Integer> oldChunks = entry.getValue();
            List<Integer> newChunks = newInventory.getOrDefault(imagenId, new ArrayList<>());

            for (Integer chunkIndex : oldChunks) {
                if (!newChunks.contains(chunkIndex)) {
                    removed.add(imagenId + "_chunk_" + chunkIndex);
                }
            }
        }

        return removed;
    }

    /**
     * Encuentra chunks que fueron agregados entre dos inventarios.
     *
     * @param oldInventory Inventario anterior
     * @param newInventory Inventario nuevo
     * @return Set de identificadores de chunks nuevos (formato: "imagenId_chunk_N")
     */
    private Set<String> findAddedChunks(Map<String, List<Integer>> oldInventory,
                                        Map<String, List<Integer>> newInventory) {
        Set<String> added = new HashSet<>();

        for (Map.Entry<String, List<Integer>> entry : newInventory.entrySet()) {
            String imagenId = entry.getKey();
            List<Integer> newChunks = entry.getValue();
            List<Integer> oldChunks = oldInventory.getOrDefault(imagenId, new ArrayList<>());

            for (Integer chunkIndex : newChunks) {
                if (!oldChunks.contains(chunkIndex)) {
                    added.add(imagenId + "_chunk_" + chunkIndex);
                }
            }
        }

        return added;
    }
}