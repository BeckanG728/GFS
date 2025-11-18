package com.tpdteam3.master.service;

import com.tpdteam3.master.model.ChunkserverInfo;
import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.model.FileMetadata.ChunkMetadata;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MasterService {

    @Autowired
    private MetadataPersistenceService persistenceService;

    @Autowired
    private HeartbeatHandler heartbeatHandler;

    @Autowired
    @Lazy
    private IntegrityMonitor integrityMonitor;

    // Almacena metadatos de archivos en memoria
    private Map<String, FileMetadata> fileMetadataStore;

    // ✅ SIMPLIFICADO: Estado de chunkservers directamente aquí
    private final Map<String, ChunkserverInfo> registeredChunkservers = new ConcurrentHashMap<>();

    // Configuración
    private static final int REPLICATION_FACTOR = 3;
    private static final int CHUNK_SIZE = 32 * 1024; // 32KB
    private static final boolean ALLOW_DEGRADED_REPLICATION = true;
    private static final int MIN_REPLICAS_REQUIRED = 1;

    @PostConstruct
    public void init() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║         🚀 MASTER SERVICE - INICIALIZADO              ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        // Cargar metadatos desde disco
        fileMetadataStore = persistenceService.loadMetadata();

        System.out.println("📊 Configuración:");
        System.out.println("   ├─ Metadatos recuperados: " + fileMetadataStore.size() + " archivos");
        System.out.println("   ├─ Factor de replicación: " + REPLICATION_FACTOR + "x");
        System.out.println("   └─ Tamaño de fragmento: " + (CHUNK_SIZE / 1024) + " KB");
        System.out.println();
        System.out.println("⏳ Esperando registro de chunkservers...");
        System.out.println();
    }


    // ═══════════════════════════════════════════════════════════════
    // GESTIÓN DE CHUNKSERVERS (consolidado desde Registry)
    // ═══════════════════════════════════════════════════════════════

    /**
     * Registra un chunkserver con ID opcional y verifica su integridad
     */
    public synchronized void registerChunkserver(String url, String id) {
        boolean isNewRegistration = !registeredChunkservers.containsKey(url);

        String chunkserverId = (id != null && !id.isEmpty())
                ? id
                : generateChunkserverId(url);

        ChunkserverInfo info = new ChunkserverInfo(url, chunkserverId);
        registeredChunkservers.put(url, info);

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  ✅ CHUNKSERVER " + (isNewRegistration ? "REGISTRADO" : "RE-REGISTRADO") + "                      ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   URL: " + url);
        System.out.println("   ID: " + chunkserverId);
        System.out.println("   Total registrados: " + registeredChunkservers.size());

        // ✅ Verificar integridad al registrar/re-registrar
        // Esto detecta chunks eliminados mientras el Master estaba caído
        if (!isNewRegistration || fileMetadataStore.size() > 0) {
            System.out.println("   🔍 Verificando integridad de chunks...");

            // Pequeña pausa para que el chunkserver esté listo
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (integrityMonitor != null) {
                integrityMonitor.onChunkserverRegistered(url);
            }
        }

        System.out.println();
    }

    /**
     * Desregistra un chunkserver del sistema
     */
    public synchronized void unregisterChunkserver(String url) {
        ChunkserverInfo removed = registeredChunkservers.remove(url);

        if (removed != null) {
            System.out.println("╔════════════════════════════════════════════════════════╗");
            System.out.println("║  ⚠️  CHUNKSERVER DESREGISTRADO                        ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.out.println("   URL: " + url);
            System.out.println("   ID: " + removed.getId());
            System.out.println("   Total registrados: " + registeredChunkservers.size());
            System.out.println();
        }
    }

    /**
     * Obtiene la lista de todos los chunkservers registrados
     */
    public List<String> getAllChunkservers() {
        return new ArrayList<>(registeredChunkservers.keySet());
    }

    /**
     * Verifica si un chunkserver está registrado
     */
    public boolean isChunkserverRegistered(String url) {
        return registeredChunkservers.containsKey(url);
    }

    /**
     * Obtiene información de un chunkserver específico
     */
    public ChunkserverInfo getChunkserverInfo(String url) {
        return registeredChunkservers.get(url);
    }

    /**
     * Genera un ID único para el chunkserver basado en su URL
     */
    private String generateChunkserverId(String url) {
        String[] parts = url.split("[:/]");
        String portPart = "";
        for (String part : parts) {
            if (part.matches("\\d+")) {
                portPart = part;
                break;
            }
        }
        return "chunkserver-" + portPart + "-" + System.currentTimeMillis();
    }


    // ═══════════════════════════════════════════════════════════════
    // GESTIÓN DE ARCHIVOS (sin cambios)
    // ═══════════════════════════════════════════════════════════════

    /**
     * Planifica upload - SOLO USA CHUNKSERVERS ACTIVOS
     */
    public FileMetadata planUpload(String imagenId, long fileSize) {
        if (registeredChunkservers.isEmpty()) {
            throw new RuntimeException(
                    "No hay chunkservers registrados en el sistema. " +
                    "Espere a que al menos un chunkserver se registre."
            );
        }

        List<String> healthyChunkservers = heartbeatHandler.getHealthyChunkservers();

        if (healthyChunkservers.isEmpty()) {
            throw new RuntimeException(
                    "No hay chunkservers disponibles para almacenar el archivo. " +
                    "Chunkservers registrados: " + registeredChunkservers.size() + ", " +
                    "pero ninguno está respondiendo a health checks."
            );
        }

        int availableReplicas = Math.min(REPLICATION_FACTOR, healthyChunkservers.size());

        if (!ALLOW_DEGRADED_REPLICATION && availableReplicas < REPLICATION_FACTOR) {
            throw new RuntimeException(
                    "Insuficientes chunkservers para mantener factor de replicación. " +
                    "Disponibles: " + healthyChunkservers.size() + ", " +
                    "Requeridos: " + REPLICATION_FACTOR
            );
        }

        if (ALLOW_DEGRADED_REPLICATION && availableReplicas < MIN_REPLICAS_REQUIRED) {
            throw new RuntimeException(
                    "Insuficientes chunkservers incluso para modo degradado. " +
                    "Disponibles: " + healthyChunkservers.size() + ", " +
                    "Mínimo requerido: " + MIN_REPLICAS_REQUIRED
            );
        }

        FileMetadata metadata = new FileMetadata(imagenId, fileSize);
        int numChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📋 PLANIFICANDO UPLOAD                               ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);
        System.out.println("   Tamaño: " + fileSize + " bytes");
        System.out.println("   Fragmentos: " + numChunks);
        System.out.println("   Réplicas por fragmento: " + availableReplicas);
        System.out.println();

        // Asignar fragmentos
        for (int i = 0; i < numChunks; i++) {
            List<String> replicaLocations = selectHealthyChunkserversForReplicas(
                    availableReplicas,
                    healthyChunkservers
            );

            for (int r = 0; r < replicaLocations.size(); r++) {
                String chunkserver = replicaLocations.get(r);
                ChunkMetadata chunk = new ChunkMetadata(i, chunkserver, chunkserver);
                chunk.setReplicaIndex(r);
                metadata.getChunks().add(chunk);
            }
        }

        fileMetadataStore.put(imagenId, metadata);
        persistenceService.saveFileMetadata(fileMetadataStore);

        return metadata;
    }

    /**
     * Selecciona chunkservers para réplicas usando distribución aleatoria
     */
    private List<String> selectHealthyChunkserversForReplicas(
            int numReplicas,
            List<String> healthyChunkservers) {

        List<String> available = new ArrayList<>(healthyChunkservers);
        Collections.shuffle(available);

        int actualReplicas = Math.min(numReplicas, available.size());
        return available.subList(0, actualReplicas);
    }

    /**
     * Obtiene metadatos - FILTRA RÉPLICAS EN SERVIDORES CAÍDOS
     */
    public FileMetadata getMetadata(String imagenId) {
        FileMetadata metadata = fileMetadataStore.get(imagenId);
        if (metadata == null) {
            throw new RuntimeException("Archivo no encontrado: " + imagenId);
        }

        FileMetadata filteredMetadata = new FileMetadata(
                metadata.getImagenId(),
                metadata.getSize()
        );
        filteredMetadata.setTimestamp(metadata.getTimestamp());

        List<String> healthyServers = heartbeatHandler.getHealthyChunkservers();

        for (ChunkMetadata chunk : metadata.getChunks()) {
            if (healthyServers.contains(chunk.getChunkserverUrl())) {
                filteredMetadata.getChunks().add(chunk);
            }
        }

        // Verificar que hay al menos una réplica por fragmento
        Map<Integer, Long> replicasPerChunk = filteredMetadata.getChunks().stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        ChunkMetadata::getChunkIndex,
                        java.util.stream.Collectors.counting()
                ));

        int numChunks = (int) Math.ceil((double) metadata.getSize() / CHUNK_SIZE);
        for (int i = 0; i < numChunks; i++) {
            if (!replicasPerChunk.containsKey(i) || replicasPerChunk.get(i) == 0) {
                throw new RuntimeException(
                        "Fragmento " + i + " no disponible - " +
                        "todas sus réplicas están en servidores caídos"
                );
            }
        }

        return filteredMetadata;
    }

    /**
     * Elimina archivo Y metadatos
     */
    public void deleteFile(String imagenId) {
        FileMetadata metadata = fileMetadataStore.remove(imagenId);
        if (metadata != null) {
            persistenceService.deleteFileMetadata(imagenId, fileMetadataStore);
            System.out.println("🗑️ Metadatos eliminados: " + imagenId);
        }
    }

    /**
     * Actualiza los metadatos de un archivo
     */
    public void updateFileMetadata(FileMetadata metadata) {
        fileMetadataStore.put(metadata.getImagenId(), metadata);
        persistenceService.saveFileMetadata(fileMetadataStore);
    }

    /**
     * Lista todos los archivos
     */
    public Collection<FileMetadata> listFiles() {
        return fileMetadataStore.values();
    }


    // ═══════════════════════════════════════════════════════════════
    // ESTADO Y ESTADÍSTICAS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Estado de salud del sistema
     */
    public Map<String, Object> getHealthStatus() {
        List<String> healthy = heartbeatHandler.getHealthyChunkservers();
        List<String> unhealthy = heartbeatHandler.getUnhealthyChunkservers();

        Map<String, Object> health = new HashMap<>();
        health.put("status", healthy.size() >= REPLICATION_FACTOR ? "HEALTHY" : "DEGRADED");
        health.put("totalChunkservers", registeredChunkservers.size());
        health.put("healthyChunkservers", healthy.size());
        health.put("unhealthyChunkservers", unhealthy.size());
        health.put("healthyServers", healthy);
        health.put("unhealthyServers", unhealthy);
        health.put("requiredForReplication", REPLICATION_FACTOR);
        health.put("canMaintainReplication", healthy.size() >= REPLICATION_FACTOR);
        health.put("filesInMemory", fileMetadataStore.size());
        health.putAll(persistenceService.getStorageStats());
        health.put("chunkserverDetails", heartbeatHandler.getDetailedStatus());

        return health;
    }

    /**
     * Estadísticas del sistema
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();

        List<String> healthy = heartbeatHandler.getHealthyChunkservers();

        stats.put("totalFiles", fileMetadataStore.size());
        stats.put("totalChunkservers", registeredChunkservers.size());
        stats.put("healthyChunkservers", healthy.size());
        stats.put("unhealthyChunkservers", registeredChunkservers.size() - healthy.size());
        stats.put("allChunkservers", getAllChunkservers());
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
        stats.put("replicationEfficiency",
                totalChunks > 0 ? (double) totalReplicas / totalChunks : 0);
        stats.put("healthStatus", getHealthStatus());
        stats.put("persistenceStats", persistenceService.getStorageStats());

        return stats;
    }
}