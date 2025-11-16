package com.tpdteam3.master.service;

import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.model.FileMetadata.ChunkMetadata;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class MasterService {

    @Autowired
    private MetadataPersistenceService persistenceService;

    @Autowired
    private ChunkserverHealthMonitor healthMonitor;

    // Almacena metadatos de archivos en memoria (cargados desde disco)
    private Map<String, FileMetadata> fileMetadataStore;

    // Lista COMPLETA de chunkservers (incluye inactivos)
    private final List<String> allChunkservers = new ArrayList<>();
    private int nextChunkserverIndex = 0;

    // ✅ CONFIGURACIÓN DE REPLICACIÓN
    private static final int REPLICATION_FACTOR = 3;
    private static final int CHUNK_SIZE = 32 * 1024; // 32KB

    // 🔒 POLÍTICA DE REPLICACIÓN
    // true = Permitir replicación degradada (crea menos réplicas si no hay suficientes servidores)
    // false = Rechazar upload si no hay suficientes servidores (más seguro)
    private static final boolean ALLOW_DEGRADED_REPLICATION = true;

    // Mínimo de réplicas requeridas (solo aplica si ALLOW_DEGRADED_REPLICATION = true)
    private static final int MIN_REPLICAS_REQUIRED = 1;

    @PostConstruct
    public void init() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║         🚀 MASTER SERVICE CON HEALTH CHECKS           ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        // 1. CARGAR METADATOS DESDE DISCO
        fileMetadataStore = persistenceService.loadMetadata();

        // 2. Registrar chunkservers
        allChunkservers.add("http://localhost:9001/chunkserver1");
        allChunkservers.add("http://localhost:9002/chunkserver2");
        allChunkservers.add("http://localhost:9003/chunkserver3");
        allChunkservers.add("http://localhost:9004/chunkserver4");

        // 3. Registrar en Health Monitor
        for (String chunkserver : allChunkservers) {
            healthMonitor.registerChunkserver(chunkserver);
        }

        System.out.println("📊 Configuración:");
        System.out.println("   ├─ Metadatos recuperados: " + fileMetadataStore.size() + " archivos");
        System.out.println("   ├─ Chunkservers registrados: " + allChunkservers.size());
        allChunkservers.forEach(cs -> System.out.println("   │  └─ " + cs));
        System.out.println("   ├─ Factor de replicación: " + REPLICATION_FACTOR + "x");
        System.out.println("   └─ Tamaño de fragmento: " + (CHUNK_SIZE / 1024) + " KB");
        System.out.println();
        System.out.println("⏳ Esperando primer health check...");
        System.out.println();
    }

    /**
     * Planifica upload - SOLO USA CHUNKSERVERS ACTIVOS
     */
    public FileMetadata planUpload(String imagenId, long fileSize) {
        // ✅ OBTENER SOLO CHUNKSERVERS ACTIVOS
        List<String> healthyChunkservers = healthMonitor.getHealthyChunkservers();

        if (healthyChunkservers.isEmpty()) {
            throw new RuntimeException("No hay chunkservers disponibles para almacenar el archivo");
        }

        // 🔒 APLICAR POLÍTICA DE REPLICACIÓN
        int availableReplicas = Math.min(REPLICATION_FACTOR, healthyChunkservers.size());

        if (!ALLOW_DEGRADED_REPLICATION && availableReplicas < REPLICATION_FACTOR) {
            // MODO ESTRICTO: Rechazar si no hay suficientes servidores
            throw new RuntimeException(
                    "Insuficientes chunkservers para mantener factor de replicación. " +
                    "Disponibles: " + healthyChunkservers.size() + ", " +
                    "Requeridos: " + REPLICATION_FACTOR + ". " +
                    "Operación rechazada por política de seguridad."
            );
        }

        if (ALLOW_DEGRADED_REPLICATION && availableReplicas < MIN_REPLICAS_REQUIRED) {
            // Incluso en modo degradado, necesitamos al menos MIN_REPLICAS_REQUIRED
            throw new RuntimeException(
                    "Insuficientes chunkservers incluso para modo degradado. " +
                    "Disponibles: " + healthyChunkservers.size() + ", " +
                    "Mínimo requerido: " + MIN_REPLICAS_REQUIRED
            );
        }

        if (availableReplicas < REPLICATION_FACTOR) {
            System.out.println("⚠️  ADVERTENCIA: Modo de replicación degradado");
            System.out.println("   ├─ Servidores disponibles: " + availableReplicas);
            System.out.println("   ├─ Factor de replicación normal: " + REPLICATION_FACTOR);
            System.out.println("   └─ Se crearán " + availableReplicas + " réplicas (riesgo de pérdida de datos)");
        }

        FileMetadata metadata = new FileMetadata(imagenId, fileSize);
        int numChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📋 PLANIFICANDO UPLOAD (SOLO SERVIDORES ACTIVOS)    ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);
        System.out.println("   Tamaño: " + fileSize + " bytes (" + (fileSize / 1024) + " KB)");
        System.out.println("   Fragmentos: " + numChunks);
        System.out.println("   Servidores activos: " + healthyChunkservers.size() + "/" + allChunkservers.size());
        System.out.println("   Réplicas por fragmento: " + availableReplicas +
                           (availableReplicas < REPLICATION_FACTOR ? " ⚠️ DEGRADADO" : " ✅"));
        System.out.println();

        // Asignar fragmentos SOLO a servidores activos
        for (int i = 0; i < numChunks; i++) {
            List<String> replicaLocations = selectHealthyChunkserversForReplicas(
                    availableReplicas,
                    healthyChunkservers
            );

            System.out.println("   Fragmento " + i + ":");
            for (int r = 0; r < replicaLocations.size(); r++) {
                String chunkserver = replicaLocations.get(r);
                ChunkMetadata chunk = new ChunkMetadata(i, chunkserver, chunkserver);
                chunk.setReplicaIndex(r);
                metadata.getChunks().add(chunk);

                String replicaType = r == 0 ? "PRIMARIA" : "RÉPLICA " + r;
                System.out.println("      └─ [" + replicaType + "] → " + chunkserver);
            }
        }

        // Guardar metadatos
        fileMetadataStore.put(imagenId, metadata);
        persistenceService.saveFileMetadata(fileMetadataStore);

        System.out.println();
        System.out.println("✅ Plan de replicación creado (solo servidores activos)");
        System.out.println("   Total de escrituras: " + metadata.getChunks().size());
        System.out.println();

        return metadata;
    }

    /**
     * Selecciona N chunkservers ACTIVOS diferentes para réplicas
     */
    private List<String> selectHealthyChunkserversForReplicas(int numReplicas, List<String> healthyChunkservers) {
        List<String> selected = new ArrayList<>();
        List<String> available = new ArrayList<>(healthyChunkservers);

        int actualReplicas = Math.min(numReplicas, available.size());

        // Round-robin sobre servidores ACTIVOS
        for (int i = 0; i < actualReplicas; i++) {
            String chunkserver = available.get(nextChunkserverIndex % available.size());
            selected.add(chunkserver);
            nextChunkserverIndex++;
        }

        return selected;
    }

    /**
     * Obtiene metadatos - FILTRA RÉPLICAS EN SERVIDORES CAÍDOS
     */
    public FileMetadata getMetadata(String imagenId) {
        FileMetadata metadata = fileMetadataStore.get(imagenId);
        if (metadata == null) {
            throw new RuntimeException("Archivo no encontrado: " + imagenId);
        }

        // ✅ CREAR COPIA CON SOLO RÉPLICAS EN SERVIDORES ACTIVOS
        FileMetadata filteredMetadata = new FileMetadata(metadata.getImagenId(), metadata.getSize());
        filteredMetadata.setTimestamp(metadata.getTimestamp());

        List<String> healthyServers = healthMonitor.getHealthyChunkservers();

        for (ChunkMetadata chunk : metadata.getChunks()) {
            // Solo incluir chunks en servidores activos
            if (healthyServers.contains(chunk.getChunkserverUrl())) {
                filteredMetadata.getChunks().add(chunk);
            }
        }

        int totalReplicas = metadata.getChunks().size();
        int availableReplicas = filteredMetadata.getChunks().size();

        System.out.println("📥 Metadatos para: " + imagenId);
        System.out.println("   Total réplicas: " + totalReplicas);
        System.out.println("   Réplicas disponibles: " + availableReplicas);

        if (availableReplicas < totalReplicas) {
            System.out.println("   ⚠️  " + (totalReplicas - availableReplicas) + " réplicas en servidores caídos (filtradas)");
        }

        // Verificar si hay al menos una réplica por fragmento
        Map<Integer, Long> replicasPerChunk = filteredMetadata.getChunks().stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        ChunkMetadata::getChunkIndex,
                        java.util.stream.Collectors.counting()
                ));

        int numChunks = (int) Math.ceil((double) metadata.getSize() / CHUNK_SIZE);
        for (int i = 0; i < numChunks; i++) {
            if (!replicasPerChunk.containsKey(i) || replicasPerChunk.get(i) == 0) {
                throw new RuntimeException("Fragmento " + i + " no disponible - todas sus réplicas están en servidores caídos");
            }
        }

        return filteredMetadata;
    }

    /**
     * Elimina archivo Y réplicas
     */
    public void deleteFile(String imagenId) {
        FileMetadata metadata = fileMetadataStore.remove(imagenId);
        if (metadata != null) {
            persistenceService.deleteFileMetadata(imagenId, fileMetadataStore);
            System.out.println("🗑️ Metadatos eliminados: " + imagenId);
        }
    }

    /**
     * Actualiza los metadatos de un archivo (usado por re-replicación)
     */
    public void updateFileMetadata(FileMetadata metadata) {
        fileMetadataStore.put(metadata.getImagenId(), metadata);
        persistenceService.saveFileMetadata(fileMetadataStore);
        System.out.println("💾 Metadatos actualizados: " + metadata.getImagenId());
    }

    /**
     * Lista todos los archivos
     */
    public Collection<FileMetadata> listFiles() {
        return fileMetadataStore.values();
    }

    /**
     * Registra nuevo chunkserver
     */
    public void registerChunkserver(String url) {
        if (!allChunkservers.contains(url)) {
            allChunkservers.add(url);
            healthMonitor.registerChunkserver(url);
            System.out.println("✅ Chunkserver registrado: " + url);
        }
    }

    /**
     * Remueve chunkserver
     */
    public void unregisterChunkserver(String url) {
        if (allChunkservers.remove(url)) {
            healthMonitor.unregisterChunkserver(url);
            System.out.println("⚠️ Chunkserver removido: " + url);
        }
    }

    /**
     * Estado de salud del sistema
     */
    public Map<String, Object> getHealthStatus() {
        List<String> healthy = healthMonitor.getHealthyChunkservers();
        List<String> unhealthy = healthMonitor.getUnhealthyChunkservers();

        Map<String, Object> health = new HashMap<>();
        health.put("status", healthy.size() >= REPLICATION_FACTOR ? "HEALTHY" : "DEGRADED");
        health.put("totalChunkservers", allChunkservers.size());
        health.put("healthyChunkservers", healthy.size());
        health.put("unhealthyChunkservers", unhealthy.size());
        health.put("healthyServers", healthy);
        health.put("unhealthyServers", unhealthy);
        health.put("requiredForReplication", REPLICATION_FACTOR);
        health.put("canMaintainReplication", healthy.size() >= REPLICATION_FACTOR);
        health.put("filesInMemory", fileMetadataStore.size());
        health.putAll(persistenceService.getStorageStats());

        // Agregar estadísticas detalladas de cada chunkserver
        health.put("chunkserverDetails", healthMonitor.getDetailedStatus());

        return health;
    }

    /**
     * Estadísticas del sistema
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();

        List<String> healthy = healthMonitor.getHealthyChunkservers();

        stats.put("totalFiles", fileMetadataStore.size());
        stats.put("totalChunkservers", allChunkservers.size());
        stats.put("healthyChunkservers", healthy.size());
        stats.put("unhealthyChunkservers", allChunkservers.size() - healthy.size());
        stats.put("allChunkservers", allChunkservers);
        stats.put("healthyServers", healthy);
        stats.put("chunkSizeKB", CHUNK_SIZE / 1024);
        stats.put("replicationFactor", REPLICATION_FACTOR);

        // Calcular totales
        long totalSize = 0;
        long totalChunks = 0;
        long totalReplicas = 0;

        for (FileMetadata metadata : fileMetadataStore.values()) {
            totalSize += metadata.getSize();
            Set<Integer> uniqueChunks = new HashSet<>();
            for (ChunkMetadata chunk : metadata.getChunks()) {
                uniqueChunks.add(chunk.getChunkIndex());
                totalReplicas++;
            }
            totalChunks += uniqueChunks.size();
        }

        stats.put("totalStorageUsed", totalSize);
        stats.put("totalStorageUsedKB", totalSize / 1024);
        stats.put("totalUniqueChunks", totalChunks);
        stats.put("totalReplicas", totalReplicas);
        stats.put("replicationEfficiency", totalChunks > 0 ? (double) totalReplicas / totalChunks : 0);
        stats.put("healthStatus", getHealthStatus());
        stats.put("persistenceStats", persistenceService.getStorageStats());

        return stats;
    }
}