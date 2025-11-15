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

    // Almacena metadatos de archivos en memoria (cargados desde disco)
    private Map<String, FileMetadata> fileMetadataStore;

    // Lista de chunkservers disponibles CON context-path
    private final List<String> chunkservers = new ArrayList<>();
    private int nextChunkserverIndex = 0;

    // ✅ CONFIGURACIÓN DE REPLICACIÓN
    private static final int REPLICATION_FACTOR = 3; // Número de réplicas por chunk
    private static final int CHUNK_SIZE = 32 * 1024; // 32KB por fragmento

    @PostConstruct
    public void init() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║         🚀 MASTER SERVICE CON REPLICACIÓN              ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        // 1. CARGAR METADATOS DESDE DISCO
        fileMetadataStore = persistenceService.loadMetadata();

        // 2. Registrar chunkservers CON el context-path
        chunkservers.add("http://localhost:9001/chunkserver1");
        chunkservers.add("http://localhost:9002/chunkserver2");
        chunkservers.add("http://localhost:9003/chunkserver3");

        System.out.println("📊 Configuración:");
        System.out.println("   ├─ Metadatos recuperados: " + fileMetadataStore.size() + " archivos");
        System.out.println("   ├─ Chunkservers disponibles: " + chunkservers.size());
        chunkservers.forEach(cs -> System.out.println("   │  └─ " + cs));
        System.out.println("   ├─ Factor de replicación: " + REPLICATION_FACTOR + "x");
        System.out.println("   └─ Tamaño de fragmento: " + (CHUNK_SIZE / 1024) + " KB");
        System.out.println();
    }

    /**
     * Planifica dónde se almacenarán los fragmentos de un archivo CON REPLICACIÓN
     */
    public FileMetadata planUpload(String imagenId, long fileSize) {
        FileMetadata metadata = new FileMetadata(imagenId, fileSize);

        // Calcular número de fragmentos necesarios
        int numChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📋 PLANIFICANDO UPLOAD CON REPLICACIÓN               ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);
        System.out.println("   Tamaño: " + fileSize + " bytes (" + (fileSize / 1024) + " KB)");
        System.out.println("   Fragmentos: " + numChunks);
        System.out.println("   Réplicas por fragmento: " + REPLICATION_FACTOR);
        System.out.println();

        // Asignar cada fragmento a MÚLTIPLES chunkservers (replicación)
        for (int i = 0; i < numChunks; i++) {
            List<String> replicaLocations = selectChunkserversForReplicas(REPLICATION_FACTOR);

            System.out.println("   Fragmento " + i + ":");
            for (int r = 0; r < replicaLocations.size(); r++) {
                String chunkserver = replicaLocations.get(r);
                ChunkMetadata chunk = new ChunkMetadata(i, chunkserver, chunkserver);
                chunk.setReplicaIndex(r); // Índice de réplica
                metadata.getChunks().add(chunk);

                String replicaType = r == 0 ? "PRIMARIA" : "RÉPLICA " + r;
                System.out.println("      └─ [" + replicaType + "] → " + chunkserver);
            }
        }

        // Guardar metadatos EN MEMORIA Y DISCO
        fileMetadataStore.put(imagenId, metadata);
        persistenceService.saveFileMetadata(fileMetadataStore);

        System.out.println();
        System.out.println("✅ Plan de replicación creado y persistido");
        System.out.println("   Total de escrituras: " + metadata.getChunks().size());
        System.out.println();

        return metadata;
    }

    /**
     * Selecciona N chunkservers diferentes para almacenar réplicas
     */
    private List<String> selectChunkserversForReplicas(int numReplicas) {
        List<String> selected = new ArrayList<>();
        List<String> available = new ArrayList<>(chunkservers);

        // No podemos tener más réplicas que chunkservers disponibles
        int actualReplicas = Math.min(numReplicas, available.size());

        // Seleccionar N chunkservers diferentes usando round-robin
        for (int i = 0; i < actualReplicas; i++) {
            String chunkserver = available.get(nextChunkserverIndex % available.size());
            selected.add(chunkserver);
            nextChunkserverIndex++;
        }

        return selected;
    }

    /**
     * Obtiene metadatos de un archivo DESDE MEMORIA (cargado desde disco al inicio)
     */
    public FileMetadata getMetadata(String imagenId) {
        FileMetadata metadata = fileMetadataStore.get(imagenId);
        if (metadata == null) {
            throw new RuntimeException("Archivo no encontrado: " + imagenId);
        }

        System.out.println("📥 Recuperando metadatos para: " + imagenId);
        System.out.println("   Total de réplicas almacenadas: " + metadata.getChunks().size());

        return metadata;
    }

    /**
     * Elimina metadatos de un archivo DE MEMORIA Y DISCO
     */
    public void deleteFile(String imagenId) {
        FileMetadata metadata = fileMetadataStore.remove(imagenId);
        if (metadata != null) {
            persistenceService.deleteFileMetadata(imagenId, fileMetadataStore);
            System.out.println("🗑️ Metadatos eliminados de memoria y disco: " + imagenId);
            System.out.println("   Réplicas eliminadas: " + metadata.getChunks().size());
        }
    }

    /**
     * Lista todos los archivos registrados
     */
    public Collection<FileMetadata> listFiles() {
        return fileMetadataStore.values();
    }

    /**
     * Registra un nuevo chunkserver (para extensibilidad)
     */
    public void registerChunkserver(String url) {
        if (!chunkservers.contains(url)) {
            chunkservers.add(url);
            System.out.println("✅ Nuevo chunkserver registrado: " + url);
            System.out.println("   Total de chunkservers: " + chunkservers.size());
        }
    }

    /**
     * Remueve un chunkserver (para mantenimiento)
     */
    public void unregisterChunkserver(String url) {
        if (chunkservers.remove(url)) {
            System.out.println("⚠️ Chunkserver removido: " + url);
            System.out.println("   Chunkservers restantes: " + chunkservers.size());
        }
    }

    /**
     * Obtiene el estado de salud del sistema
     */
    public Map<String, Object> getHealthStatus() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", chunkservers.size() >= REPLICATION_FACTOR ? "HEALTHY" : "DEGRADED");
        health.put("availableChunkservers", chunkservers.size());
        health.put("requiredForReplication", REPLICATION_FACTOR);
        health.put("canMaintainReplication", chunkservers.size() >= REPLICATION_FACTOR);
        health.put("filesInMemory", fileMetadataStore.size());

        // Agregar estadísticas de persistencia
        health.putAll(persistenceService.getStorageStats());

        return health;
    }

    /**
     * Obtiene estadísticas detalladas del sistema
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();

        // Estadísticas básicas
        stats.put("totalFiles", fileMetadataStore.size());
        stats.put("totalChunkservers", chunkservers.size());
        stats.put("chunkservers", chunkservers);
        stats.put("chunkSizeKB", CHUNK_SIZE / 1024);
        stats.put("replicationFactor", REPLICATION_FACTOR);

        // Calcular totales
        long totalSize = 0;
        long totalChunks = 0;
        long totalReplicas = 0;

        for (FileMetadata metadata : fileMetadataStore.values()) {
            totalSize += metadata.getSize();

            // Contar chunks únicos y réplicas
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

        // Estado de salud
        stats.put("healthStatus", getHealthStatus());

        // Estadísticas de persistencia
        stats.put("persistenceStats", persistenceService.getStorageStats());

        return stats;
    }
}