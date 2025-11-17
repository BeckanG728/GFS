package com.tpdteam3.master.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ✅ DISEÑO CORRECTO: El Master RECIBE heartbeats de los Chunkservers
 * <p>
 * Este servicio:
 * 1. Recibe heartbeats activos de cada chunkserver
 * 2. Actualiza timestamp de último heartbeat
 * 3. Marca como DOWN si no recibe heartbeat en tiempo esperado
 * 4. Procesa inventario de chunks en cada heartbeat
 * 5. Detecta cambios y dispara acciones correctivas
 */
@Service
public class MasterHeartbeatHandler {

    @Autowired
    @Lazy
    private IntegrityMonitorService integrityMonitor;

    // Almacena información de cada chunkserver
    private final Map<String, ChunkserverHeartbeatInfo> chunkserverHeartbeats = new ConcurrentHashMap<>();

    // Configuración
    private static final int HEARTBEAT_TIMEOUT_SECONDS = 30; // 3x el intervalo normal
    private static final int CLEANUP_INTERVAL_SECONDS = 10;  // Verificar timeouts cada 10 segundos

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @PostConstruct
    public void init() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  💓 HEARTBEAT HANDLER - MODO RECEPTOR                 ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("✅ Esperando heartbeats de chunkservers...");
        System.out.println("⏱️  Timeout de heartbeat: " + HEARTBEAT_TIMEOUT_SECONDS + " segundos");
        System.out.println();

        // Programar verificación periódica de timeouts
        scheduler.scheduleAtFixedRate(
                this::checkHeartbeatTimeouts,
                CLEANUP_INTERVAL_SECONDS,
                CLEANUP_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    @PreDestroy
    public void shutdown() {
        System.out.println("🛑 Deteniendo Heartbeat Handler...");
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
     * ✅ MÉTODO PRINCIPAL: Procesa un heartbeat recibido de un chunkserver
     * Este método es llamado por el MasterController cuando llega un POST /api/master/heartbeat
     */
    public Map<String, Object> processHeartbeat(Map<String, Object> heartbeatData) {
        String chunkserverId = (String) heartbeatData.get("chunkserverId");
        String url = (String) heartbeatData.get("url");
        String status = (String) heartbeatData.get("status");
        Long timestamp = ((Number) heartbeatData.get("timestamp")).longValue();

        // Manejar shutdown graceful
        if ("SHUTDOWN".equals(status)) {
            handleChunkserverShutdown(url, chunkserverId);
            return createSuccessResponse("Shutdown acknowledged");
        }

        // Obtener o crear info del chunkserver
        ChunkserverHeartbeatInfo info = chunkserverHeartbeats.computeIfAbsent(
                url,
                k -> new ChunkserverHeartbeatInfo(url, chunkserverId)
        );

        boolean wasDown = !info.isAlive();

        // Actualizar información del heartbeat
        info.updateHeartbeat(timestamp);
        info.updateMetrics(heartbeatData);

        // Procesar inventario de chunks
        @SuppressWarnings("unchecked")
        Map<String, List<Integer>> currentInventory =
                (Map<String, List<Integer>>) heartbeatData.get("inventory");

        if (currentInventory != null) {
            Map<String, List<Integer>> previousInventory = info.getLastInventory();

            // Detectar cambios en el inventario
            if (previousInventory != null && !inventoriesMatch(previousInventory, currentInventory)) {
                handleInventoryChange(url, previousInventory, currentInventory);
            }

            info.updateInventory(currentInventory);
        }

        // Si el servidor estaba caído y ahora volvió
        if (wasDown) {
            handleChunkserverRecovery(url, chunkserverId, currentInventory);
        }

        // Respuesta al chunkserver (puede incluir comandos)
        Map<String, Object> response = createSuccessResponse("Heartbeat received");

        // Opcionalmente, el Master puede enviar comandos al chunkserver
        List<Map<String, Object>> commands = generateCommandsForChunkserver(url, info);
        if (!commands.isEmpty()) {
            response.put("commands", commands);
        }

        return response;
    }

    /**
     * Verifica periódicamente si algún chunkserver dejó de enviar heartbeats
     */
    private void checkHeartbeatTimeouts() {
        long now = System.currentTimeMillis();
        long timeoutMs = HEARTBEAT_TIMEOUT_SECONDS * 1000L;

        List<String> timedOutServers = new ArrayList<>();

        for (Map.Entry<String, ChunkserverHeartbeatInfo> entry : chunkserverHeartbeats.entrySet()) {
            String url = entry.getKey();
            ChunkserverHeartbeatInfo info = entry.getValue();

            long timeSinceLastHeartbeat = now - info.getLastHeartbeatTime();

            if (timeSinceLastHeartbeat > timeoutMs && info.isAlive()) {
                timedOutServers.add(url);
                info.markAsDead();

                System.out.println("╔════════════════════════════════════════════════════════╗");
                System.out.println("║  ⚠️  CHUNKSERVER TIMEOUT                              ║");
                System.out.println("╚════════════════════════════════════════════════════════╝");
                System.out.println("   URL: " + url);
                System.out.println("   Último heartbeat: " + (timeSinceLastHeartbeat / 1000) + " segundos atrás");
                System.out.println("   Uptime previo: " + info.getUptimePercentage() + "%");
                System.out.println();

                // Notificar al IntegrityMonitor
                if (integrityMonitor != null) {
                    integrityMonitor.onChunkserverDown(url);
                }
            }
        }
    }

    /**
     * Maneja cuando un chunkserver se recupera después de estar caído
     */
    private void handleChunkserverRecovery(String url, String chunkserverId,
                                           Map<String, List<Integer>> inventory) {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  ✅ CHUNKSERVER RECOVERED                             ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   URL: " + url);
        System.out.println("   ID: " + chunkserverId);

        if (inventory != null) {
            int totalChunks = inventory.values().stream().mapToInt(List::size).sum();
            System.out.println("   Chunks reportados: " + totalChunks);
        }

        System.out.println();

        // Notificar al IntegrityMonitor para verificar integridad
        if (integrityMonitor != null && inventory != null) {
            integrityMonitor.onChunkserverRecovered(url, inventory);
        }
    }

    /**
     * Maneja shutdown graceful de un chunkserver
     */
    private void handleChunkserverShutdown(String url, String chunkserverId) {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🛑 CHUNKSERVER SHUTDOWN (GRACEFUL)                   ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   URL: " + url);
        System.out.println("   ID: " + chunkserverId);
        System.out.println();

        ChunkserverHeartbeatInfo info = chunkserverHeartbeats.get(url);
        if (info != null) {
            info.markAsDead();
        }

        // No disparar re-replicación inmediata en shutdown graceful
        // El ReplicationMonitor lo manejará si el servidor no vuelve
    }

    /**
     * Maneja cambios en el inventario de chunks de un servidor
     */
    private void handleInventoryChange(String url,
                                       Map<String, List<Integer>> oldInventory,
                                       Map<String, List<Integer>> newInventory) {
        System.out.println("🔍 CAMBIO EN INVENTARIO DETECTADO: " + url);

        Set<String> removedChunks = findRemovedChunks(oldInventory, newInventory);

        if (!removedChunks.isEmpty()) {
            System.out.println("   ❌ Chunks eliminados: " + removedChunks.size());

            // Notificar al IntegrityMonitor
            if (integrityMonitor != null) {
                integrityMonitor.onInventoryChanged(url, newInventory, removedChunks);
            }
        }
    }

    /**
     * Genera comandos opcionales que el Master quiere que el chunkserver ejecute
     */
    private List<Map<String, Object>> generateCommandsForChunkserver(String url,
                                                                     ChunkserverHeartbeatInfo info) {
        List<Map<String, Object>> commands = new ArrayList<>();

        // Ejemplo: Pedir verificación de integridad si han pasado muchos chunks
        if (info.getTotalHeartbeats() % 100 == 0) {
            Map<String, Object> command = new HashMap<>();
            command.put("action", "verify_chunks");
            commands.add(command);
        }

        return commands;
    }

    // Métodos auxiliares

    private boolean inventoriesMatch(Map<String, List<Integer>> inv1,
                                     Map<String, List<Integer>> inv2) {
        if (inv1.size() != inv2.size()) return false;

        for (Map.Entry<String, List<Integer>> entry : inv1.entrySet()) {
            String imagenId = entry.getKey();
            List<Integer> chunks1 = entry.getValue();
            List<Integer> chunks2 = inv2.get(imagenId);

            if (chunks2 == null || !new HashSet<>(chunks1).equals(new HashSet<>(chunks2))) {
                return false;
            }
        }
        return true;
    }

    private Set<String> findRemovedChunks(Map<String, List<Integer>> oldInv,
                                          Map<String, List<Integer>> newInv) {
        Set<String> removed = new HashSet<>();

        for (Map.Entry<String, List<Integer>> entry : oldInv.entrySet()) {
            String imagenId = entry.getKey();
            List<Integer> oldChunks = entry.getValue();
            List<Integer> newChunks = newInv.getOrDefault(imagenId, new ArrayList<>());

            for (Integer chunkIndex : oldChunks) {
                if (!newChunks.contains(chunkIndex)) {
                    removed.add(imagenId + "_chunk_" + chunkIndex);
                }
            }
        }
        return removed;
    }

    private Map<String, Object> createSuccessResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", message);
        response.put("timestamp", System.currentTimeMillis());
        return response;
    }

    // API pública para otros servicios

    public List<String> getHealthyChunkservers() {
        return chunkserverHeartbeats.values().stream()
                .filter(ChunkserverHeartbeatInfo::isAlive)
                .map(ChunkserverHeartbeatInfo::getUrl)
                .collect(java.util.stream.Collectors.toList());
    }

    public List<String> getUnhealthyChunkservers() {
        return chunkserverHeartbeats.values().stream()
                .filter(info -> !info.isAlive())
                .map(ChunkserverHeartbeatInfo::getUrl)
                .collect(java.util.stream.Collectors.toList());
    }

    public boolean isChunkserverHealthy(String url) {
        ChunkserverHeartbeatInfo info = chunkserverHeartbeats.get(url);
        return info != null && info.isAlive();
    }

    public Map<String, List<Integer>> getChunkserverInventory(String url) {
        ChunkserverHeartbeatInfo info = chunkserverHeartbeats.get(url);
        return info != null ? info.getLastInventory() : new HashMap<>();
    }

    /**
     * Clase interna que mantiene el estado de heartbeats de un chunkserver
     */
    private static class ChunkserverHeartbeatInfo {
        private final String url;
        private final String chunkserverId;
        private long lastHeartbeatTime;
        private long firstHeartbeatTime;
        private boolean alive;
        private Map<String, List<Integer>> lastInventory;
        private long totalHeartbeats;
        private long totalDowntime;
        private long lastDowntimeStart;

        // Métricas reportadas por el chunkserver
        private Integer totalChunks;
        private Double storageUsedMB;
        private Long freeSpaceMB;
        private Boolean canWrite;

        public ChunkserverHeartbeatInfo(String url, String chunkserverId) {
            this.url = url;
            this.chunkserverId = chunkserverId;
            this.alive = true;
            this.firstHeartbeatTime = System.currentTimeMillis();
            this.lastHeartbeatTime = System.currentTimeMillis();
            this.totalHeartbeats = 0;
            this.totalDowntime = 0;
        }

        public void updateHeartbeat(long timestamp) {
            this.lastHeartbeatTime = timestamp;
            this.totalHeartbeats++;

            if (!alive) {
                alive = true;
                if (lastDowntimeStart > 0) {
                    totalDowntime += (timestamp - lastDowntimeStart);
                }
            }
        }

        public void updateInventory(Map<String, List<Integer>> inventory) {
            this.lastInventory = inventory;
        }

        public void updateMetrics(Map<String, Object> data) {
            if (data.containsKey("totalChunks")) {
                this.totalChunks = ((Number) data.get("totalChunks")).intValue();
            }
            if (data.containsKey("storageUsedMB")) {
                this.storageUsedMB = ((Number) data.get("storageUsedMB")).doubleValue();
            }
            if (data.containsKey("freeSpaceMB")) {
                this.freeSpaceMB = ((Number) data.get("freeSpaceMB")).longValue();
            }
            if (data.containsKey("canWrite")) {
                this.canWrite = (Boolean) data.get("canWrite");
            }
        }

        public void markAsDead() {
            if (alive) {
                alive = false;
                lastDowntimeStart = System.currentTimeMillis();
            }
        }

        public double getUptimePercentage() {
            long now = System.currentTimeMillis();
            long totalTime = now - firstHeartbeatTime;
            if (totalTime == 0) return 100.0;

            long currentDowntime = totalDowntime;
            if (!alive && lastDowntimeStart > 0) {
                currentDowntime += (now - lastDowntimeStart);
            }

            return ((totalTime - currentDowntime) * 100.0) / totalTime;
        }

        // Getters
        public String getUrl() {
            return url;
        }

        public String getChunkserverId() {
            return chunkserverId;
        }

        public long getLastHeartbeatTime() {
            return lastHeartbeatTime;
        }

        public boolean isAlive() {
            return alive;
        }

        public Map<String, List<Integer>> getLastInventory() {
            return lastInventory;
        }

        public long getTotalHeartbeats() {
            return totalHeartbeats;
        }
    }
}