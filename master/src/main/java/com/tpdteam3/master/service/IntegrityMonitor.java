package com.tpdteam3.master.service;

import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.model.FileMetadata.ChunkMetadata;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 🔍 SERVICIO DE INTEGRIDAD Y REPARACIÓN AUTOMÁTICA
 * <p>
 * Este servicio es el cerebro que detecta y repara problemas de replicación automáticamente.
 * <p>
 * Funcionalidades principales:
 * 1. Escucha notificaciones del HealthMonitor sobre cambios en inventarios
 * 2. Detecta cuando alguien eliminó chunks manualmente de disco
 * 3. Repara automáticamente la replicación copiando desde otras réplicas
 * 4. Maneja caídas y recuperaciones de chunkservers
 * <p>
 * FLUJO DE DETECCIÓN Y REPARACIÓN:
 * 1. HealthMonitor detecta cambio en inventario → notifica a IntegrityMonitor
 * 2. IntegrityMonitor compara inventario real vs metadatos del Master
 * 3. Si faltan chunks → busca otra réplica disponible
 * 4. Copia chunk desde réplica fuente a servidor afectado
 * 5. Actualiza metadatos del Master
 * 6. Todo automático, sin intervención manual
 */
@Service
public class IntegrityMonitor {

    // @Lazy para romper el ciclo de dependencias
    @Autowired
    @Lazy
    private MasterService masterService;

    @Autowired
    @Lazy
    private HeartbeatHandler heartbeatHandler;

    private final RestTemplate restTemplate;

    // Estadísticas de operaciones
    private long totalMissingChunksDetected = 0;
    private long totalChunksRepaired = 0;
    private long totalRepairAttempts = 0;
    private long totalRepairFailures = 0;

    // Evitar reparaciones concurrentes del mismo chunk
    private final Set<String> currentlyRepairing = ConcurrentHashMap.newKeySet();

    /**
     * Constructor que configura el RestTemplate con timeouts.
     */
    public IntegrityMonitor() {
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);  // 5 segundos
        factory.setReadTimeout(15000);    // 15 segundos (las copias pueden tardar)
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * Se ejecuta al iniciar el servicio.
     */
    @PostConstruct
    public void init() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🔧 INTEGRITY MONITOR - REPARACIÓN AUTOMÁTICA          ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("✅ Modo: Reparación automática activada");
        System.out.println("🎯 Detecta eliminaciones manuales de chunks");
        System.out.println("🔧 Repara replicación automáticamente");
        System.out.println();
    }

    /**
     * ✅ HANDLER: Llamado cuando el HealthMonitor detecta cambios en el inventario de un servidor.
     * Este es el punto de entrada principal para la detección de eliminaciones manuales.
     * <p>
     * Escenario típico:
     * - Usuario borra archivo "imagen-uuid_chunk_2.bin" del disco
     * - HealthMonitor lo detecta en el próximo health check (10 segundos)
     * - Llama a este método con los chunks removidos
     * - Este método repara automáticamente la réplica perdida
     *
     * @param chunkserverUrl   URL del servidor con cambios
     * @param currentInventory Inventario actual del servidor
     * @param removedChunks    Set de chunks que fueron eliminados
     */
    public synchronized void onInventoryChanged(String chunkserverUrl,
                                                Map<String, List<Integer>> currentInventory,
                                                Set<String> removedChunks) {

        if (removedChunks.isEmpty()) {
            return;
        }

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🚨 CHUNKS ELIMINADOS DETECTADOS                     ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   Servidor: " + chunkserverUrl);
        System.out.println("   Chunks eliminados: " + removedChunks.size());
        System.out.println();

        totalMissingChunksDetected += removedChunks.size();

        // Procesar cada chunk eliminado
        for (String chunkId : removedChunks) {
            try {
                // Parsear chunkId: "imagen-uuid_chunk_5" → imagenId="imagen-uuid", chunkIndex=5
                String[] parts = chunkId.split("_chunk_");
                if (parts.length != 2) {
                    System.err.println("⚠️  Formato de chunkId inválido: " + chunkId);
                    continue;
                }

                String imagenId = parts[0];
                int chunkIndex = Integer.parseInt(parts[1]);

                // Intentar reparar esta réplica perdida
                repairMissingChunk(imagenId, chunkIndex, chunkserverUrl);

            } catch (Exception e) {
                System.err.println("❌ Error procesando chunk eliminado " + chunkId + ": " + e.getMessage());
            }
        }

        System.out.println();
    }

    /**
     * ✅ HANDLER: Llamado cuando un servidor se cae.
     * No hacemos nada inmediatamente porque el servidor puede recuperarse.
     * El ReplicationMonitorService manejará la re-replicación si el servidor no vuelve.
     *
     * @param chunkserverUrl URL del servidor que cayó
     */
    public void onChunkserverDown(String chunkserverUrl) {
        System.out.println("ℹ️  Servidor caído detectado: " + chunkserverUrl);
        System.out.println("   El ReplicationMonitor manejará re-replicación si no se recupera");
    }

    /**
     * ✅ HANDLER: Llamado cuando un servidor se recupera después de estar caído.
     * Verificamos que tenga todos los chunks que debería tener según el Master.
     *
     * @param chunkserverUrl URL del servidor recuperado
     * @param inventory      Inventario actual del servidor
     */
    public synchronized void onChunkserverRecovered(String chunkserverUrl,
                                                    Map<String, List<Integer>> inventory) {
        System.out.println("🔍 Verificando integridad de servidor recuperado: " + chunkserverUrl);

        // Obtener chunks que este servidor DEBERÍA tener según el Master
        Map<String, Set<Integer>> expectedChunks = buildExpectedChunksForServer(chunkserverUrl);

        // Comparar con lo que realmente tiene
        Set<String> missingChunks = findMissingChunks(expectedChunks, inventory);

        if (missingChunks.isEmpty()) {
            System.out.println("   ✅ Servidor tiene todos los chunks esperados");
            return;
        }

        System.out.println("   ⚠️  Faltan " + missingChunks.size() + " chunks");
        System.out.println("   🔧 Reparando...");

        // Reparar chunks faltantes
        for (String chunkId : missingChunks) {
            try {
                String[] parts = chunkId.split("_chunk_");
                String imagenId = parts[0];
                int chunkIndex = Integer.parseInt(parts[1]);

                repairMissingChunk(imagenId, chunkIndex, chunkserverUrl);
            } catch (Exception e) {
                System.err.println("   ❌ Error reparando " + chunkId + ": " + e.getMessage());
            }
        }
    }

    /**
     * ✅ NUEVO: Llamado cuando un chunkserver se registra o re-registra.
     * Verifica que el servidor tenga todos los chunks que debería tener.
     * Esto detecta eliminaciones que ocurrieron mientras el Master estaba caído.
     *
     * @param chunkserverUrl URL del servidor registrado
     */
    public synchronized void onChunkserverRegistered(String chunkserverUrl) {
        System.out.println("🔍 Verificando integridad de servidor registrado: " + chunkserverUrl);

        try {
            // Obtener inventario actual del servidor
            Map<String, List<Integer>> currentInventory =
                    heartbeatHandler.getChunkserverInventory(chunkserverUrl);

            if (currentInventory == null || currentInventory.isEmpty()) {
                System.out.println("   ⚠️  No se pudo obtener inventario del servidor");
                return;
            }

            // Obtener chunks que este servidor DEBERÍA tener según el Master
            Map<String, Set<Integer>> expectedChunks = buildExpectedChunksForServer(chunkserverUrl);

            if (expectedChunks.isEmpty()) {
                System.out.println("   ℹ️  No hay chunks esperados para este servidor");
                return;
            }

            // Comparar y detectar diferencias
            Set<String> missingChunks = findMissingChunks(expectedChunks, currentInventory);

            if (missingChunks.isEmpty()) {
                System.out.println("   ✅ Servidor tiene todos los chunks esperados");
                return;
            }

            System.out.println("   🚨 CHUNKS FALTANTES DETECTADOS: " + missingChunks.size());
            System.out.println("      (Probablemente eliminados mientras Master estaba caído)");

            // Mostrar algunos ejemplos
            missingChunks.stream().limit(5).forEach(chunk ->
                    System.out.println("      - " + chunk)
            );

            if (missingChunks.size() > 5) {
                System.out.println("      ... y " + (missingChunks.size() - 5) + " más");
            }

            System.out.println("   🔧 Iniciando reparación automática...");
            System.out.println();

            // Reparar chunks faltantes
            int repaired = 0;
            int failed = 0;

            for (String chunkId : missingChunks) {
                try {
                    String[] parts = chunkId.split("_chunk_");
                    if (parts.length != 2) continue;

                    String imagenId = parts[0];
                    int chunkIndex = Integer.parseInt(parts[1]);

                    repairMissingChunk(imagenId, chunkIndex, chunkserverUrl);
                    repaired++;

                } catch (Exception e) {
                    System.err.println("      ❌ Error reparando " + chunkId + ": " + e.getMessage());
                    failed++;
                }
            }

            System.out.println();
            System.out.println("   📊 Resultado de verificación al registro:");
            System.out.println("      ✅ Chunks reparados: " + repaired);
            if (failed > 0) {
                System.out.println("      ❌ Fallos: " + failed);
            }
            System.out.println();

        } catch (Exception e) {
            System.err.println("   ❌ Error verificando integridad: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 🔧 MÉTODO PRINCIPAL DE REPARACIÓN
     * <p>
     * Repara un chunk específico que falta en un servidor:
     * 1. Verifica que el Master conozca este chunk
     * 2. Busca otra réplica disponible del mismo chunk
     * 3. Copia los datos desde la réplica fuente al servidor destino
     * 4. Actualiza metadatos del Master si fue una nueva réplica
     *
     * @param imagenId        ID de la imagen
     * @param chunkIndex      Índice del chunk faltante
     * @param targetServerUrl URL del servidor donde falta el chunk
     */
    private void repairMissingChunk(String imagenId, int chunkIndex, String targetServerUrl) {
        String repairKey = imagenId + "_" + chunkIndex + "_" + targetServerUrl;

        // Evitar reparaciones concurrentes del mismo chunk
        if (!currentlyRepairing.add(repairKey)) {
            System.out.println("   ⏭️  Ya se está reparando: " + repairKey);
            return;
        }

        totalRepairAttempts++;

        try {
            System.out.println("   🔧 Reparando: " + imagenId + " chunk " + chunkIndex + " en " + targetServerUrl);

            // 1. Obtener metadatos del archivo
            FileMetadata metadata;
            try {
                metadata = masterService.getMetadata(imagenId);
            } catch (RuntimeException e) {
                System.err.println("      ❌ Archivo no encontrado en Master: " + imagenId);
                totalRepairFailures++;
                return;
            }

            // 2. Buscar réplicas existentes de este chunk
            List<ChunkMetadata> replicas = metadata.getChunks().stream()
                    .filter(chunk -> chunk.getChunkIndex() == chunkIndex)
                    .collect(Collectors.toList());

            if (replicas.isEmpty()) {
                System.err.println("      ❌ No hay réplicas registradas para este chunk en el Master");
                totalRepairFailures++;
                return;
            }

            // 3. Buscar una réplica DISPONIBLE (en servidor activo y diferente al target)
            List<String> healthyServers = heartbeatHandler.getHealthyChunkservers();

            ChunkMetadata sourceReplica = null;
            for (ChunkMetadata replica : replicas) {
                String replicaServer = replica.getChunkserverUrl();

                // Debe estar en servidor activo y no ser el servidor destino
                if (healthyServers.contains(replicaServer) && !replicaServer.equals(targetServerUrl)) {
                    // Verificar que el chunk realmente existe en ese servidor
                    if (verifyChunkExists(imagenId, chunkIndex, replicaServer)) {
                        sourceReplica = replica;
                        break;
                    }
                }
            }

            if (sourceReplica == null) {
                System.err.println("      ❌ No hay réplicas disponibles para copiar");
                System.err.println("         Réplicas registradas: " + replicas.size());
                System.err.println("         Servidores activos: " + healthyServers.size());
                totalRepairFailures++;
                return;
            }

            String sourceServerUrl = sourceReplica.getChunkserverUrl();
            System.out.println("      📥 Copiando desde: " + sourceServerUrl);

            // 4. COPIAR CHUNK: Leer desde fuente y escribir en destino
            byte[] chunkData = readChunkFromServer(imagenId, chunkIndex, sourceServerUrl);
            String base64Data = Base64.getEncoder().encodeToString(chunkData);

            writeChunkToServer(imagenId, chunkIndex, base64Data, targetServerUrl);

            System.out.println("      ✅ Chunk reparado exitosamente (" + chunkData.length + " bytes)");

            // 5. ACTUALIZAR METADATOS: Agregar nueva réplica si no existía
            boolean replicaExisted = replicas.stream()
                    .anyMatch(r -> r.getChunkserverUrl().equals(targetServerUrl));

            if (!replicaExisted) {
                // Crear nueva entrada de réplica en metadatos
                int nextReplicaIndex = replicas.stream()
                                               .mapToInt(ChunkMetadata::getReplicaIndex)
                                               .max()
                                               .orElse(-1) + 1;

                ChunkMetadata newReplica = new ChunkMetadata(chunkIndex, targetServerUrl, targetServerUrl);
                newReplica.setReplicaIndex(nextReplicaIndex);
                metadata.getChunks().add(newReplica);

                masterService.updateFileMetadata(metadata);
                System.out.println("      💾 Metadatos actualizados - nueva réplica registrada");
            } else {
                System.out.println("      ℹ️  Réplica ya existía en metadatos (fue eliminada manualmente)");
            }

            totalChunksRepaired++;

        } catch (Exception e) {
            System.err.println("      ❌ Error reparando chunk: " + e.getMessage());
            e.printStackTrace();
            totalRepairFailures++;
        } finally {
            currentlyRepairing.remove(repairKey);
        }
    }

    /**
     * Construye un mapa de chunks que un servidor específico DEBERÍA tener según el Master.
     *
     * @param serverUrl URL del servidor
     * @return Mapa con imagenId -> Set de índices de chunks esperados
     */
    private Map<String, Set<Integer>> buildExpectedChunksForServer(String serverUrl) {
        Map<String, Set<Integer>> expectedChunks = new HashMap<>();

        Collection<FileMetadata> allFiles = masterService.listFiles();

        for (FileMetadata file : allFiles) {
            for (ChunkMetadata chunk : file.getChunks()) {
                if (chunk.getChunkserverUrl().equals(serverUrl)) {
                    expectedChunks
                            .computeIfAbsent(file.getImagenId(), k -> new HashSet<>())
                            .add(chunk.getChunkIndex());
                }
            }
        }

        return expectedChunks;
    }

    /**
     * Encuentra chunks que faltan comparando lo esperado vs lo que realmente hay.
     *
     * @param expected Chunks esperados según el Master
     * @param actual   Inventario real del servidor
     * @return Set de identificadores de chunks faltantes
     */
    private Set<String> findMissingChunks(Map<String, Set<Integer>> expected,
                                          Map<String, List<Integer>> actual) {
        Set<String> missing = new HashSet<>();

        for (Map.Entry<String, Set<Integer>> entry : expected.entrySet()) {
            String imagenId = entry.getKey();
            Set<Integer> expectedIndices = entry.getValue();
            Set<Integer> actualIndices = new HashSet<>(
                    actual.getOrDefault(imagenId, new ArrayList<>())
            );

            for (Integer index : expectedIndices) {
                if (!actualIndices.contains(index)) {
                    missing.add(imagenId + "_chunk_" + index);
                }
            }
        }

        return missing;
    }

    /**
     * Verifica que un chunk específico realmente existe en un servidor.
     * Hace una llamada HTTP al endpoint /api/chunk/exists del chunkserver.
     *
     * @param imagenId   ID de la imagen
     * @param chunkIndex Índice del chunk
     * @param serverUrl  URL del servidor
     * @return true si el chunk existe, false si no
     */
    private boolean verifyChunkExists(String imagenId, int chunkIndex, String serverUrl) {
        try {
            String url = serverUrl + "/api/chunk/exists?imagenId=" + imagenId +
                         "&chunkIndex=" + chunkIndex;

            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);

            return response != null && Boolean.TRUE.equals(response.get("exists"));
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Lee un chunk desde un chunkserver.
     * Llama al endpoint GET /api/chunk/read del servidor.
     *
     * @param imagenId   ID de la imagen
     * @param chunkIndex Índice del chunk
     * @param serverUrl  URL del servidor fuente
     * @return Bytes del chunk leído
     * @throws Exception si hay error leyendo o el servidor no responde
     */
    private byte[] readChunkFromServer(String imagenId, int chunkIndex, String serverUrl)
            throws Exception {
        String readUrl = serverUrl + "/api/chunk/read?imagenId=" + imagenId +
                         "&chunkIndex=" + chunkIndex;

        ResponseEntity<Map> response = restTemplate.getForEntity(readUrl, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error leyendo chunk: HTTP " + response.getStatusCode());
        }

        Map<String, Object> responseBody = response.getBody();
        if (responseBody == null || !responseBody.containsKey("data")) {
            throw new RuntimeException("Respuesta inválida del chunkserver");
        }

        String base64Data = (String) responseBody.get("data");
        return Base64.getDecoder().decode(base64Data);
    }

    /**
     * Escribe un chunk a un chunkserver.
     * Llama al endpoint POST /api/chunk/write del servidor.
     *
     * @param imagenId   ID de la imagen
     * @param chunkIndex Índice del chunk
     * @param base64Data Datos del chunk en Base64
     * @param serverUrl  URL del servidor destino
     * @throws Exception si hay error escribiendo o el servidor no responde
     */
    private void writeChunkToServer(String imagenId, int chunkIndex,
                                    String base64Data, String serverUrl) throws Exception {
        String writeUrl = serverUrl + "/api/chunk/write";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> request = new HashMap<>();
        request.put("imagenId", imagenId);
        request.put("chunkIndex", chunkIndex);
        request.put("data", base64Data);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(writeUrl, entity, String.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error escribiendo chunk: HTTP " + response.getStatusCode());
        }
    }

    /**
     * Obtiene estadísticas del servicio de integridad.
     *
     * @return Mapa con métricas del servicio
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalMissingChunksDetected", totalMissingChunksDetected);
        stats.put("totalChunksRepaired", totalChunksRepaired);
        stats.put("totalRepairAttempts", totalRepairAttempts);
        stats.put("totalRepairFailures", totalRepairFailures);
        stats.put("currentlyRepairing", currentlyRepairing.size());
        stats.put("successRate", totalRepairAttempts > 0
                ? (totalChunksRepaired * 100.0 / totalRepairAttempts)
                : 100.0);
        return stats;
    }
}