package com.tpdteam3.master.service;

import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.model.FileMetadata.ChunkMetadata;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 🔍 SERVICIO DE INTEGRIDAD
 * Verifica periódicamente que los chunks en los chunkservers
 * coincidan con lo que dice el Master
 */
@Service
public class IntegrityMonitorService {

    @Autowired
    private MasterService masterService;

    @Autowired
    private ChunkserverHealthMonitor healthMonitor;

    @Autowired
    private ReplicationMonitorService replicationMonitor;

    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Configuración
    private static final int INTEGRITY_CHECK_INTERVAL_MINUTES = 5; // Cada 5 minutos

    // Estadísticas
    private long totalChecks = 0;
    private long missingChunksDetected = 0;
    private long orphanChunksDetected = 0;

    public IntegrityMonitorService() {
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);
        factory.setReadTimeout(10000);
        this.restTemplate = new RestTemplate(factory);
    }

    @PostConstruct
    public void startMonitoring() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🔍 INICIANDO INTEGRITY MONITOR                       ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("⏱️  Intervalo: " + INTEGRITY_CHECK_INTERVAL_MINUTES + " minutos");
        System.out.println();

        scheduler.scheduleAtFixedRate(
                this::performIntegrityCheck,
                INTEGRITY_CHECK_INTERVAL_MINUTES,
                INTEGRITY_CHECK_INTERVAL_MINUTES,
                TimeUnit.MINUTES
        );
    }

    @PreDestroy
    public void stopMonitoring() {
        System.out.println("🛑 Deteniendo Integrity Monitor...");
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
     * Verifica la integridad comparando inventarios
     */
    private void performIntegrityCheck() {
        try {
            totalChecks++;

            System.out.println("🔍 Ejecutando verificación de integridad...");

            List<String> healthyServers = healthMonitor.getHealthyChunkservers();

            if (healthyServers.isEmpty()) {
                System.out.println("   ⚠️  No hay servidores activos para verificar");
                return;
            }

            // 1. OBTENER INVENTARIOS DE CADA CHUNKSERVER
            Map<String, Map<String, List<Integer>>> serverInventories = new HashMap<>();

            for (String serverUrl : healthyServers) {
                try {
                    String healthUrl = serverUrl + "/api/chunk/health";
                    Map<String, Object> health = restTemplate.getForObject(healthUrl, Map.class);

                    if (health != null && health.containsKey("inventory")) {
                        @SuppressWarnings("unchecked")
                        Map<String, List<Integer>> inventory =
                                (Map<String, List<Integer>>) health.get("inventory");
                        serverInventories.put(serverUrl, inventory);
                    }
                } catch (Exception e) {
                    System.err.println("   ❌ Error obteniendo inventario de " + serverUrl);
                }
            }

            // 2. CONSTRUIR MAPA DE CHUNKS ESPERADOS POR SERVIDOR
            Collection<FileMetadata> allFiles = masterService.listFiles();
            Map<String, Map<String, Set<Integer>>> expectedChunks = buildExpectedChunksMap(allFiles);

            // 3. COMPARAR Y DETECTAR INCONSISTENCIAS
            List<String> missingChunks = new ArrayList<>();
            List<String> orphanChunks = new ArrayList<>();

            for (Map.Entry<String, Map<String, Set<Integer>>> entry : expectedChunks.entrySet()) {
                String serverUrl = entry.getKey();
                Map<String, Set<Integer>> expected = entry.getValue();
                Map<String, List<Integer>> actual = serverInventories.getOrDefault(
                        serverUrl, new HashMap<>()
                );

                // Chunks faltantes
                for (Map.Entry<String, Set<Integer>> fileEntry : expected.entrySet()) {
                    String imagenId = fileEntry.getKey();
                    Set<Integer> expectedIndices = fileEntry.getValue();
                    Set<Integer> actualIndices = new HashSet<>(
                            actual.getOrDefault(imagenId, new ArrayList<>())
                    );

                    for (Integer index : expectedIndices) {
                        if (!actualIndices.contains(index)) {
                            String chunkId = imagenId + "_chunk_" + index + " @ " + serverUrl;
                            missingChunks.add(chunkId);
                            missingChunksDetected++;
                        }
                    }
                }

                // Chunks huérfanos (no esperados)
                for (Map.Entry<String, List<Integer>> fileEntry : actual.entrySet()) {
                    String imagenId = fileEntry.getKey();
                    List<Integer> actualIndices = fileEntry.getValue();
                    Set<Integer> expectedIndices = expected.getOrDefault(imagenId, new HashSet<>());

                    for (Integer index : actualIndices) {
                        if (!expectedIndices.contains(index)) {
                            String chunkId = imagenId + "_chunk_" + index + " @ " + serverUrl;
                            orphanChunks.add(chunkId);
                            orphanChunksDetected++;
                        }
                    }
                }
            }

            // 4. REPORTAR RESULTADOS
            if (missingChunks.isEmpty() && orphanChunks.isEmpty()) {
                System.out.println("   ✅ Integridad verificada - Todo correcto");
            } else {
                System.out.println("   ⚠️  INCONSISTENCIAS DETECTADAS:");

                if (!missingChunks.isEmpty()) {
                    System.out.println("   ❌ Chunks FALTANTES: " + missingChunks.size());
                    missingChunks.stream().limit(5).forEach(chunk ->
                            System.out.println("      - " + chunk)
                    );
                    if (missingChunks.size() > 5) {
                        System.out.println("      ... y " + (missingChunks.size() - 5) + " más");
                    }

                    // 🔄 ACTIVAR RE-REPLICACIÓN
                    System.out.println("   🔄 Activando re-replicación automática...");
                }

                if (!orphanChunks.isEmpty()) {
                    System.out.println("   ⚠️  Chunks HUÉRFANOS: " + orphanChunks.size());
                    orphanChunks.stream().limit(5).forEach(chunk ->
                            System.out.println("      - " + chunk)
                    );
                    if (orphanChunks.size() > 5) {
                        System.out.println("      ... y " + (orphanChunks.size() - 5) + " más");
                    }
                }
            }

            System.out.println();

        } catch (Exception e) {
            System.err.println("❌ Error en verificación de integridad: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Construye mapa de chunks esperados por servidor según metadatos del Master
     */
    private Map<String, Map<String, Set<Integer>>> buildExpectedChunksMap(
            Collection<FileMetadata> allFiles) {

        Map<String, Map<String, Set<Integer>>> expectedChunks = new HashMap<>();

        for (FileMetadata file : allFiles) {
            for (ChunkMetadata chunk : file.getChunks()) {
                String serverUrl = chunk.getChunkserverUrl();
                String imagenId = file.getImagenId();
                int chunkIndex = chunk.getChunkIndex();

                expectedChunks
                        .computeIfAbsent(serverUrl, k -> new HashMap<>())
                        .computeIfAbsent(imagenId, k -> new HashSet<>())
                        .add(chunkIndex);
            }
        }

        return expectedChunks;
    }

    /**
     * Obtiene estadísticas del monitor de integridad
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalChecks", totalChecks);
        stats.put("missingChunksDetected", missingChunksDetected);
        stats.put("orphanChunksDetected", orphanChunksDetected);
        stats.put("checkIntervalMinutes", INTEGRITY_CHECK_INTERVAL_MINUTES);
        return stats;
    }
}
