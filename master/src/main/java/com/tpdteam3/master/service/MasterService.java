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
    private ChunkserverRegistry chunkserverRegistry;

    @Autowired
    private MasterHeartbeatHandler heartbeatHandler;


    // Inyectar IntegrityMonitor con @Lazy para evitar ciclos
    @Autowired
    @Lazy
    private IntegrityMonitorService integrityMonitor;

    // Almacena metadatos de archivos en memoria (cargados desde disco)
    private Map<String, FileMetadata> fileMetadataStore;

    // ✅ CAMBIO: Ahora los chunkservers se registran dinámicamente
    private final Map<String, ChunkserverInfo> registeredChunkservers = new ConcurrentHashMap<>();
    private int nextChunkserverIndex = 0;

    // ✅ CONFIGURACIÓN DE REPLICACIÓN
    private static final int REPLICATION_FACTOR = 3;
    private static final int CHUNK_SIZE = 32 * 1024; // 32KB

    // 🔒 POLÍTICA DE REPLICACIÓN
    private static final boolean ALLOW_DEGRADED_REPLICATION = true;
    private static final int MIN_REPLICAS_REQUIRED = 1;

    @PostConstruct
    public void init() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║         🚀 MASTER SERVICE CON REGISTRO DINÁMICO       ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        // 1. CARGAR METADATOS DESDE DISCO
        fileMetadataStore = persistenceService.loadMetadata();

        System.out.println("📊 Configuración:");
        System.out.println("   ├─ Metadatos recuperados: " + fileMetadataStore.size() + " archivos");
        System.out.println("   ├─ Modo de registro: DINÁMICO (los chunkservers se auto-registran)");
        System.out.println("   ├─ Factor de replicación: " + REPLICATION_FACTOR + "x");
        System.out.println("   └─ Tamaño de fragmento: " + (CHUNK_SIZE / 1024) + " KB");
        System.out.println();
        System.out.println("⏳ Esperando registro de chunkservers...");
        System.out.println();
    }

    /**
     * ✅ NUEVO: Registra un chunkserver dinámicamente
     */
    public void registerChunkserver(String url) {
        registerChunkserver(url, null);
    }

    /**
     * ✅ MEJORADO: Registra un chunkserver con ID opcional
     * Y verifica su integridad al registrarse
     */
    public synchronized void registerChunkserver(String url, String id) {
        boolean isNewRegistration = !registeredChunkservers.containsKey(url);

        if (!isNewRegistration) {
            System.out.println("⚠️  Chunkserver ya registrado: " + url);
            // Aunque ya esté registrado, verificar integridad por si acaso
        }

        String chunkserverId = (id != null && !id.isEmpty()) ? id : generateChunkserverId(url);
        ChunkserverInfo info = new ChunkserverInfo(url, chunkserverId);

        registeredChunkservers.put(url, info);
        chunkserverRegistry.registerChunkserver(url);

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  ✅ CHUNKSERVER " + (isNewRegistration ? "REGISTRADO" : "RE-REGISTRADO") + "                      ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   URL: " + url);
        System.out.println("   ID: " + chunkserverId);
        System.out.println("   Total registrados: " + registeredChunkservers.size());

        // ✅ NUEVO: Verificar integridad al registrar/re-registrar
        // Esto detecta chunks eliminados mientras el Master estaba caído
        if (!isNewRegistration || fileMetadataStore.size() > 0) {
            System.out.println("   🔍 Verificando integridad de chunks...");

            // Esperar un momento para que el chunkserver esté completamente listo
            try {
                Thread.sleep(1000); // 1 segundo
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Disparar verificación de integridad
            if (integrityMonitor != null) {
                integrityMonitor.onChunkserverRegistered(url);
            }
        }

        System.out.println();
    }

    /**
     * ✅ NUEVO: Desregistra un chunkserver
     */
    public synchronized void unregisterChunkserver(String url) {
        ChunkserverInfo removed = registeredChunkservers.remove(url);
        if (removed != null) {
            chunkserverRegistry.unregisterChunkserver(url);
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

    /**
     * Obtiene la lista de todos los chunkservers registrados
     */
    public List<String> getAllChunkservers() {
        return new ArrayList<>(registeredChunkservers.keySet());
    }

    /**
     * Planifica upload - SOLO USA CHUNKSERVERS ACTIVOS
     */
    public FileMetadata planUpload(String imagenId, long fileSize) {
        // ✅ Verificar que hay chunkservers registrados
        if (registeredChunkservers.isEmpty()) {
            throw new RuntimeException(
                    "No hay chunkservers registrados en el sistema. " +
                    "Espere a que al menos un chunkserver se registre."
            );
        }

        // ✅ OBTENER SOLO CHUNKSERVERS ACTIVOS
        List<String> healthyChunkservers = heartbeatHandler.getHealthyChunkservers();

        if (healthyChunkservers.isEmpty()) {
            throw new RuntimeException(
                    "No hay chunkservers disponibles para almacenar el archivo. " +
                    "Chunkservers registrados: " + registeredChunkservers.size() + ", " +
                    "pero ninguno está respondiendo a health checks."
            );
        }

        // 🔒 APLICAR POLÍTICA DE REPLICACIÓN
        int availableReplicas = Math.min(REPLICATION_FACTOR, healthyChunkservers.size());

        if (!ALLOW_DEGRADED_REPLICATION && availableReplicas < REPLICATION_FACTOR) {
            throw new RuntimeException(
                    "Insuficientes chunkservers para mantener factor de replicación. " +
                    "Disponibles: " + healthyChunkservers.size() + ", " +
                    "Requeridos: " + REPLICATION_FACTOR + ". " +
                    "Operación rechazada por política de seguridad."
            );
        }

        if (ALLOW_DEGRADED_REPLICATION && availableReplicas < MIN_REPLICAS_REQUIRED) {
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
        System.out.println("║  📋 PLANIFICANDO UPLOAD                               ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);
        System.out.println("   Tamaño: " + fileSize + " bytes (" + (fileSize / 1024) + " KB)");
        System.out.println("   Fragmentos: " + numChunks);
        System.out.println("   Chunkservers registrados: " + registeredChunkservers.size());
        System.out.println("   Chunkservers activos: " + healthyChunkservers.size());
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
        System.out.println("✅ Plan de replicación creado");
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

        List<String> healthyServers = heartbeatHandler.getHealthyChunkservers();

        for (ChunkMetadata chunk : metadata.getChunks()) {
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
            System.out.println("   ⚠️  " + (totalReplicas - availableReplicas) +
                               " réplicas en servidores caídos (filtradas)");
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
                        "Fragmento " + i + " no disponible - todas sus réplicas están en servidores caídos"
                );
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
     * Actualiza los metadatos de un archivo
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
        stats.put("replicationEfficiency", totalChunks > 0 ? (double) totalReplicas / totalChunks : 0);
        stats.put("healthStatus", getHealthStatus());
        stats.put("persistenceStats", persistenceService.getStorageStats());

        return stats;
    }
}