package com.tpdteam3.master.service;

import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.model.FileMetadata.ChunkMetadata;
import com.tpdteam3.master.model.FileWithReplicationStatus;
import com.tpdteam3.master.model.ReplicationStatus;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Servicio que monitorea archivos con replicación degradada
 * y los re-replica automáticamente cuando hay servidores disponibles
 */
@Service
public class ReplicationMonitorService {

    @Autowired
    private MasterService masterService;

    @Autowired
    private HeartbeatHandler heartbeatHandler;

    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Configuración
    private static final int REPLICATION_CHECK_INTERVAL_SECONDS = 30; // Verificar cada 30 segundos
    private static final int TARGET_REPLICATION_FACTOR = 3;
    private static final int MAX_CONCURRENT_REREPLICATIONS = 2; // Máximo 2 archivos replicándose al mismo tiempo

    // ✅ NUEVO: Prevenir operaciones conflictivas
    private static final int MIN_REPLICATION_FACTOR = 2; // No eliminar si hay menos de esto
    private static final long COOLDOWN_AFTER_REPAIR_MS = 60000; // 60 segundos de espera después de reparar

    // Estado
    private final Set<String> currentlyReplicating = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> lastRepairTime = new ConcurrentHashMap<>(); // ✅ NUEVO: Track de última reparación
    private long totalReplicationsMade = 0;
    private long totalReplicationAttempts = 0;
    private long totalCleanupOperations = 0; // ✅ NUEVO: Contador de limpiezas

    public ReplicationMonitorService() {
        // Configurar RestTemplate con timeouts
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);  // 5 segundos
        factory.setReadTimeout(10000);    // 10 segundos
        this.restTemplate = new RestTemplate(factory);
    }

    @PostConstruct
    public void startMonitoring() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🔄 INICIANDO RE-REPLICATION MONITOR                  ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("⏱️  Intervalo de verificación: " + REPLICATION_CHECK_INTERVAL_SECONDS + " segundos");
        System.out.println("🎯 Factor de replicación objetivo: " + TARGET_REPLICATION_FACTOR);
        System.out.println("🔄 Re-replicaciones concurrentes máximas: " + MAX_CONCURRENT_REREPLICATIONS);
        System.out.println("⏰ Cooldown después de reparar: " + (COOLDOWN_AFTER_REPAIR_MS / 1000) + " segundos");
        System.out.println();

        // Iniciar monitoreo periódico
        scheduler.scheduleAtFixedRate(
                this::checkAndRereplicate,
                REPLICATION_CHECK_INTERVAL_SECONDS, // Esperar antes del primer check
                REPLICATION_CHECK_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    @PreDestroy
    public void stopMonitoring() {
        System.out.println("🛑 Deteniendo Re-replication Monitor...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Verifica todos los archivos y re-replica los que necesiten más réplicas
     * o elimina réplicas excedentes
     */
    private void checkAndRereplicate() {
        try {
            List<String> healthyServers = heartbeatHandler.getHealthyChunkservers();

            // Solo proceder si hay suficientes servidores para mejorar la replicación
            if (healthyServers.size() < 2) {
                return; // No tiene sentido revisar si no hay servidores disponibles
            }

            System.out.println("🔍 Verificando estado de replicación...");
            System.out.println("   Servidores activos: " + healthyServers.size());

            Collection<FileMetadata> allFiles = masterService.listFiles();
            List<FileWithReplicationStatus> degradedFiles = new ArrayList<>();
            List<FileWithReplicationStatus> overReplicatedFiles = new ArrayList<>();

            // Identificar archivos que necesitan ajuste de réplicas
            for (FileMetadata file : allFiles) {
                ReplicationStatus status = analyzeReplication(file, healthyServers);

                if (status.needsReplication()) {
                    degradedFiles.add(new FileWithReplicationStatus(file, status));
                } else if (status.hasExcessReplicas()) {
                    // ✅ NUEVO: Solo considerar limpieza si no se reparó recientemente
                    Long lastRepair = lastRepairTime.get(file.getImagenId());
                    if (lastRepair == null ||
                        System.currentTimeMillis() - lastRepair > COOLDOWN_AFTER_REPAIR_MS) {
                        overReplicatedFiles.add(new FileWithReplicationStatus(file, status));
                    }
                }
            }

            // Informar estado
            if (degradedFiles.isEmpty() && overReplicatedFiles.isEmpty()) {
                System.out.println("   ✅ Todos los archivos tienen replicación óptima");
                return;
            }

            if (!degradedFiles.isEmpty()) {
                System.out.println("   ⚠️  Archivos con replicación degradada: " + degradedFiles.size());
            }

            if (!overReplicatedFiles.isEmpty()) {
                System.out.println("   📊 Archivos con sobre-replicación: " + overReplicatedFiles.size());
            }

            // 1. PRIORIDAD: Re-replicar archivos degradados
            degradedFiles.sort(Comparator.comparingInt(f -> f.getStatus().getCurrentMinReplicas()));

            int replicationsStarted = 0;
            for (FileWithReplicationStatus degradedFile : degradedFiles) {
                if (currentlyReplicating.size() >= MAX_CONCURRENT_REREPLICATIONS) {
                    System.out.println("   ⏸️  Límite de re-replicaciones concurrentes alcanzado");
                    break;
                }

                if (currentlyReplicating.contains(degradedFile.getFile().getImagenId())) {
                    continue;
                }

                replicateFile(degradedFile.getFile(), degradedFile.getStatus(), healthyServers);
                replicationsStarted++;
            }

            if (replicationsStarted > 0) {
                System.out.println("   🔄 Re-replicaciones iniciadas: " + replicationsStarted);
            }

            // 2. SEGUNDA PRIORIDAD: Limpiar réplicas excedentes (CON CUIDADO)
            int cleanupStarted = 0;
            for (FileWithReplicationStatus overReplicatedFile : overReplicatedFiles) {
                if (currentlyReplicating.contains(overReplicatedFile.getFile().getImagenId())) {
                    continue; // Ya está siendo procesado
                }

                // ✅ NUEVO: Solo limpiar si realmente hay exceso significativo
                if (overReplicatedFile.getStatus().getCurrentMinReplicas() > TARGET_REPLICATION_FACTOR) {
                    cleanupExcessReplicas(overReplicatedFile.getFile(), overReplicatedFile.getStatus(), healthyServers);
                    cleanupStarted++;
                }
            }

            if (cleanupStarted > 0) {
                System.out.println("   🧹 Limpiezas de réplicas iniciadas: " + cleanupStarted);
            }

        } catch (Exception e) {
            System.err.println("❌ Error en Re-replication Monitor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Analiza el estado de replicación de un archivo
     */
    private ReplicationStatus analyzeReplication(FileMetadata file, List<String> healthyServers) {
        // Agrupar chunks por índice
        Map<Integer, List<ChunkMetadata>> chunksByIndex = file.getChunks().stream()
                .collect(Collectors.groupingBy(ChunkMetadata::getChunkIndex));

        int totalChunks = chunksByIndex.size();
        int minReplicas = Integer.MAX_VALUE;
        int maxReplicas = 0;
        int totalReplicas = 0;
        int chunksNeedingReplication = 0;

        for (Map.Entry<Integer, List<ChunkMetadata>> entry : chunksByIndex.entrySet()) {
            // Contar solo réplicas en servidores ACTIVOS
            long activeReplicas = entry.getValue().stream()
                    .filter(chunk -> healthyServers.contains(chunk.getChunkserverUrl()))
                    .count();

            int replicas = (int) activeReplicas;
            totalReplicas += replicas;
            minReplicas = Math.min(minReplicas, replicas);
            maxReplicas = Math.max(maxReplicas, replicas);

            if (replicas < TARGET_REPLICATION_FACTOR) {
                chunksNeedingReplication++;
            }
        }

        return new ReplicationStatus(
                totalChunks,
                minReplicas == Integer.MAX_VALUE ? 0 : minReplicas,
                maxReplicas,
                totalReplicas,
                chunksNeedingReplication
        );
    }

    /**
     * Re-replica un archivo que necesita más réplicas
     */
    private void replicateFile(FileMetadata file, ReplicationStatus status, List<String> healthyServers) {
        String imagenId = file.getImagenId();

        if (!currentlyReplicating.add(imagenId)) {
            return; // Ya se está replicando
        }

        totalReplicationAttempts++;

        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🔄 INICIANDO RE-REPLICACIÓN                         ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);
        System.out.println("   Réplicas actuales: " + status.getCurrentMinReplicas());
        System.out.println("   Réplicas objetivo: " + TARGET_REPLICATION_FACTOR);
        System.out.println("   Chunks a replicar: " + status.getChunksNeedingReplication());
        System.out.println();

        // Ejecutar en thread separado para no bloquear el monitoreo
        CompletableFuture.runAsync(() -> {
            try {
                doReplication(file, healthyServers);
                totalReplicationsMade++;
                lastRepairTime.put(imagenId, System.currentTimeMillis()); // ✅ NUEVO: Registrar tiempo de reparación
                System.out.println("✅ Re-replicación completada: " + imagenId);
            } catch (Exception e) {
                System.err.println("❌ Error en re-replicación de " + imagenId + ": " + e.getMessage());
            } finally {
                currentlyReplicating.remove(imagenId);
            }
        });
    }

    /**
     * Realiza la re-replicación efectiva
     */
    private void doReplication(FileMetadata file, List<String> healthyServers) throws Exception {
        // Agrupar chunks por índice
        Map<Integer, List<ChunkMetadata>> chunksByIndex = file.getChunks().stream()
                .collect(Collectors.groupingBy(ChunkMetadata::getChunkIndex));

        int replicasCreated = 0;
        int replicasFailed = 0;

        for (Map.Entry<Integer, List<ChunkMetadata>> entry : chunksByIndex.entrySet()) {
            int chunkIndex = entry.getKey();
            List<ChunkMetadata> existingReplicas = entry.getValue();

            // Contar réplicas activas
            List<ChunkMetadata> activeReplicas = existingReplicas.stream()
                    .filter(chunk -> healthyServers.contains(chunk.getChunkserverUrl()))
                    .collect(Collectors.toList());

            int currentReplicas = activeReplicas.size();
            int neededReplicas = TARGET_REPLICATION_FACTOR - currentReplicas;

            if (neededReplicas <= 0) {
                continue; // Este chunk ya tiene suficientes réplicas
            }

            // Seleccionar servidores que NO tienen este chunk
            Set<String> serversWithChunk = activeReplicas.stream()
                    .map(ChunkMetadata::getChunkserverUrl)
                    .collect(Collectors.toSet());

            List<String> availableServers = healthyServers.stream()
                    .filter(server -> !serversWithChunk.contains(server))
                    .collect(Collectors.toList());

            if (availableServers.isEmpty()) {
                System.out.println("   ⚠️  Chunk " + chunkIndex + ": No hay servidores disponibles para replicar");
                continue;
            }

            // Limitar a los servidores necesarios
            int serversToUse = Math.min(neededReplicas, availableServers.size());

            System.out.println("   📦 Chunk " + chunkIndex + ": Creando " + serversToUse + " réplicas adicionales");

            // Leer chunk desde una réplica existente
            String sourceServer = activeReplicas.get(0).getChunkserverUrl();
            byte[] chunkData = readChunkFromServer(file.getImagenId(), chunkIndex, sourceServer);
            String base64Data = Base64.getEncoder().encodeToString(chunkData);

            // Escribir en nuevos servidores
            for (int i = 0; i < serversToUse; i++) {
                String targetServer = availableServers.get(i);
                try {
                    writeChunkToServer(file.getImagenId(), chunkIndex, base64Data, targetServer);

                    // Actualizar metadatos en memoria
                    ChunkMetadata newChunk = new ChunkMetadata(chunkIndex, targetServer, targetServer);
                    newChunk.setReplicaIndex(currentReplicas + i);
                    file.getChunks().add(newChunk);

                    System.out.println("      ✅ Réplica creada en: " + targetServer);
                    replicasCreated++;
                } catch (Exception e) {
                    System.err.println("      ❌ Error creando réplica en " + targetServer + ": " + e.getMessage());
                    replicasFailed++;
                }
            }
        }

        // Persistir cambios en metadatos
        if (replicasCreated > 0) {
            masterService.updateFileMetadata(file);
            System.out.println();
            System.out.println("📊 Resultado re-replicación:");
            System.out.println("   ✅ Réplicas creadas: " + replicasCreated);
            if (replicasFailed > 0) {
                System.out.println("   ❌ Réplicas fallidas: " + replicasFailed);
            }
        }
    }

    /**
     * ✅ MEJORADO: Elimina réplicas excedentes de un archivo CON CUIDADO
     */
    private void cleanupExcessReplicas(FileMetadata file, ReplicationStatus status, List<String> healthyServers) {
        String imagenId = file.getImagenId();

        if (!currentlyReplicating.add(imagenId)) {
            return; // Ya se está procesando
        }

        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🧹 LIMPIANDO RÉPLICAS EXCEDENTES                     ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);
        System.out.println("   Réplicas máximas actuales: " + status.getCurrentMaxReplicas());
        System.out.println("   Réplicas objetivo: " + TARGET_REPLICATION_FACTOR);
        System.out.println();

        CompletableFuture.runAsync(() -> {
            try {
                doCleanup(file, healthyServers);
                totalCleanupOperations++;
                System.out.println("✅ Limpieza completada: " + imagenId);
            } catch (Exception e) {
                System.err.println("❌ Error en limpieza de " + imagenId + ": " + e.getMessage());
            } finally {
                currentlyReplicating.remove(imagenId);
            }
        });
    }

    /**
     * ✅ MEJORADO: Realiza la limpieza efectiva de réplicas excedentes
     */
    private void doCleanup(FileMetadata file, List<String> healthyServers) throws Exception {
        Map<Integer, List<ChunkMetadata>> chunksByIndex = file.getChunks().stream()
                .collect(Collectors.groupingBy(ChunkMetadata::getChunkIndex));

        int replicasDeleted = 0;
        List<ChunkMetadata> chunksToRemove = new ArrayList<>();

        for (Map.Entry<Integer, List<ChunkMetadata>> entry : chunksByIndex.entrySet()) {
            int chunkIndex = entry.getKey();
            List<ChunkMetadata> allReplicas = entry.getValue();

            // Filtrar solo réplicas activas
            List<ChunkMetadata> activeReplicas = allReplicas.stream()
                    .filter(chunk -> healthyServers.contains(chunk.getChunkserverUrl()))
                    .sorted(Comparator.comparingInt(ChunkMetadata::getReplicaIndex))
                    .collect(Collectors.toList());

            int currentReplicas = activeReplicas.size();

            // ✅ NUEVO: Solo eliminar si hay significativamente más réplicas de lo necesario
            // Y asegurar que nunca bajemos del mínimo seguro
            int excessReplicas = currentReplicas - TARGET_REPLICATION_FACTOR;

            if (excessReplicas <= 0 || currentReplicas <= MIN_REPLICATION_FACTOR) {
                continue; // No eliminar si no hay exceso real o estamos en el mínimo
            }

            System.out.println("   📦 Chunk " + chunkIndex + ": Eliminando " + excessReplicas + " réplicas excedentes");
            System.out.println("      Réplicas actuales: " + currentReplicas + " → Objetivo: " + TARGET_REPLICATION_FACTOR);

            // Mantener solo las primeras TARGET_REPLICATION_FACTOR réplicas
            List<ChunkMetadata> replicasToDelete = activeReplicas.stream()
                    .skip(TARGET_REPLICATION_FACTOR)
                    .collect(Collectors.toList());

            for (ChunkMetadata chunk : replicasToDelete) {
                try {
                    String deleteUrl = chunk.getChunkserverUrl() + "/api/chunk/delete?imagenId=" +
                                       file.getImagenId() + "&chunkIndex=" + chunkIndex;
                    restTemplate.delete(deleteUrl);

                    chunksToRemove.add(chunk);
                    System.out.println("      ✅ Réplica eliminada de: " + chunk.getChunkserverUrl());
                    replicasDeleted++;
                } catch (Exception e) {
                    System.err.println("      ❌ Error eliminando réplica de " +
                                       chunk.getChunkserverUrl() + ": " + e.getMessage());
                }
            }
        }

        // Actualizar metadatos si se eliminaron réplicas
        if (replicasDeleted > 0) {
            file.getChunks().removeAll(chunksToRemove);
            masterService.updateFileMetadata(file);

            System.out.println();
            System.out.println("📊 Resultado limpieza:");
            System.out.println("   🗑️ Réplicas eliminadas: " + replicasDeleted);
        }
    }

    /**
     * Lee un chunk desde un chunkserver
     */
    private byte[] readChunkFromServer(String imagenId, int chunkIndex, String chunkserverUrl) throws Exception {
        String readUrl = chunkserverUrl + "/api/chunk/read?imagenId=" + imagenId + "&chunkIndex=" + chunkIndex;
        ResponseEntity<Map> response = restTemplate.getForEntity(readUrl, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error leyendo chunk");
        }

        Map<String, Object> responseBody = response.getBody();
        if (responseBody == null) {
            throw new RuntimeException("Response body es null");
        }

        String base64Data = (String) responseBody.get("data");
        if (base64Data == null) {
            throw new RuntimeException("Data es null en la respuesta");
        }

        return Base64.getDecoder().decode(base64Data);
    }

    /**
     * Escribe un chunk a un chunkserver
     */
    private void writeChunkToServer(String imagenId, int chunkIndex, String base64Data, String chunkserverUrl)
            throws Exception {
        String writeUrl = chunkserverUrl + "/api/chunk/write";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> request = new HashMap<>();
        request.put("imagenId", imagenId);
        request.put("chunkIndex", chunkIndex);
        request.put("data", base64Data);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        restTemplate.postForEntity(writeUrl, entity, String.class);
    }

    /**
     * Obtiene estadísticas del servicio de re-replicación
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("currentlyReplicating", currentlyReplicating.size());
        stats.put("replicatingFiles", new ArrayList<>(currentlyReplicating));
        stats.put("totalReplicationAttempts", totalReplicationAttempts);
        stats.put("totalReplicationsMade", totalReplicationsMade);
        stats.put("totalCleanupOperations", totalCleanupOperations);
        stats.put("successRate", totalReplicationAttempts > 0
                ? (totalReplicationsMade * 100.0 / totalReplicationAttempts)
                : 100.0);
        return stats;
    }


}