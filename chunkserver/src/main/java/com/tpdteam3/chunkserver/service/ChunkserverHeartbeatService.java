package com.tpdteam3.chunkserver.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ✅ DISEÑO CORRECTO: Heartbeats ACTIVOS desde Chunkserver → Master
 * <p>
 * El Chunkserver envía heartbeats periódicos al Master para:
 * 1. Informar que está vivo y disponible
 * 2. Enviar inventario actualizado de chunks
 * 3. Reportar métricas de salud (espacio disponible, carga, etc)
 * <p>
 * Ventajas sobre el polling del Master:
 * - Escalabilidad: El Master no tiene que consultar a cada chunkserver
 * - Detección de fallos más rápida: El Master detecta inmediatamente cuando no llega un heartbeat
 * - Menor carga en el Master: Solo recibe datos, no hace requests
 * - Mejor para network overhead: Conexiones unidireccionales
 */
@Service
public class ChunkserverHeartbeatService {

    @Value("${server.port}")
    private int serverPort;

    @Value("${server.servlet.context-path:/}")
    private String contextPath;

    @Value("${chunkserver.id}")
    private String chunkserverId;

    @Value("${chunkserver.master.url:http://localhost:9000/master}")
    private String masterUrl;

    @Value("${chunkserver.hostname:localhost}")
    private String hostname;

    @Value("${chunkserver.heartbeat.interval:10}")
    private int heartbeatIntervalSeconds;

    @Autowired
    private ChunkStorageService storageService;

    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private String chunkserverUrl;
    private int consecutiveFailures = 0;
    private static final int MAX_FAILURES_BEFORE_ALERT = 3;

    public ChunkserverHeartbeatService() {
        this.restTemplate = new RestTemplate();
    }

    @PostConstruct
    public void init() {
        // Construir URL del chunkserver
        String cleanContextPath = contextPath.equals("/") ? "" : contextPath;
        chunkserverUrl = String.format("http://%s:%d%s", hostname, serverPort, cleanContextPath);

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  💓 INICIANDO HEARTBEAT SERVICE                       ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   Chunkserver ID: " + chunkserverId);
        System.out.println("   URL: " + chunkserverUrl);
        System.out.println("   Master URL: " + masterUrl);
        System.out.println("   Intervalo heartbeat: " + heartbeatIntervalSeconds + " segundos");
        System.out.println();

        // Enviar primer heartbeat inmediatamente (registro inicial)
        sendHeartbeat();

        // Programar heartbeats periódicos
        scheduler.scheduleAtFixedRate(
                this::sendHeartbeat,
                heartbeatIntervalSeconds,
                heartbeatIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    @PreDestroy
    public void shutdown() {
        System.out.println("🛑 Deteniendo Heartbeat Service...");

        // Enviar heartbeat final indicando shutdown graceful
        try {
            sendShutdownNotification();
        } catch (Exception e) {
            System.err.println("⚠️  Error enviando notificación de shutdown: " + e.getMessage());
        }

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
     * ✅ HEARTBEAT ACTIVO: El Chunkserver envía su estado al Master
     */
    private void sendHeartbeat() {
        try {
            String heartbeatUrl = masterUrl + "/api/master/heartbeat";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            // Construir payload del heartbeat con información completa
            Map<String, Object> heartbeatData = new HashMap<>();
            heartbeatData.put("chunkserverId", chunkserverId);
            heartbeatData.put("url", chunkserverUrl);
            heartbeatData.put("status", "UP");
            heartbeatData.put("timestamp", System.currentTimeMillis());

            // ✅ INCLUIR INVENTARIO DE CHUNKS (lo que realmente tiene el servidor)
            Map<String, List<Integer>> inventory = storageService.getChunkInventory();
            heartbeatData.put("inventory", inventory);

            // ✅ INCLUIR MÉTRICAS DE SALUD
            Map<String, Object> stats = storageService.getStats();
            heartbeatData.put("totalChunks", stats.get("totalChunks"));
            heartbeatData.put("storageUsedMB", stats.get("storageUsedMB"));
            heartbeatData.put("freeSpaceMB", stats.get("freeSpaceMB"));
            heartbeatData.put("canWrite", stats.get("canWrite"));

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(heartbeatData, headers);

            // Enviar heartbeat al Master
            ResponseEntity<Map> response = restTemplate.postForEntity(heartbeatUrl, entity, Map.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                if (consecutiveFailures > 0) {
                    System.out.println("✅ Conexión con Master restaurada");
                }
                consecutiveFailures = 0;

                // El Master puede enviar comandos en la respuesta (opcional)
                Map<String, Object> responseBody = response.getBody();
                if (responseBody != null) {
                    handleMasterCommands(responseBody);
                }
            }

        } catch (Exception e) {
            consecutiveFailures++;

            if (consecutiveFailures == 1) {
                System.err.println("⚠️  Error enviando heartbeat al Master: " + e.getMessage());
            }

            if (consecutiveFailures >= MAX_FAILURES_BEFORE_ALERT) {
                System.err.println("❌ ALERTA: No se puede contactar al Master (" +
                                   consecutiveFailures + " intentos fallidos)");
                System.err.println("   El chunkserver continuará operando pero no será visible para el Master");
            }
        }
    }

    /**
     * Procesa comandos que el Master puede enviar en la respuesta del heartbeat
     * Por ejemplo: "delete_chunk", "verify_integrity", etc.
     */
    private void handleMasterCommands(Map<String, Object> responseBody) {
        Object commandsObj = responseBody.get("commands");
        if (commandsObj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> commands = (List<Map<String, Object>>) commandsObj;

            for (Map<String, Object> command : commands) {
                String action = (String) command.get("action");
                System.out.println("📨 Comando del Master: " + action);

                // Implementar handlers para diferentes comandos
                switch (action) {
                    case "verify_chunks":
                        // Verificar integridad de chunks específicos
                        break;
                    case "delete_chunk":
                        // Eliminar chunk específico
                        break;
                    case "report_detailed_stats":
                        // Enviar estadísticas más detalladas
                        break;
                }
            }
        }
    }

    /**
     * Notifica al Master que este chunkserver se está apagando de forma ordenada
     */
    private void sendShutdownNotification() {
        String shutdownUrl = masterUrl + "/api/master/heartbeat";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> shutdownData = new HashMap<>();
        shutdownData.put("chunkserverId", chunkserverId);
        shutdownData.put("url", chunkserverUrl);
        shutdownData.put("status", "SHUTDOWN");
        shutdownData.put("timestamp", System.currentTimeMillis());

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(shutdownData, headers);

        try {
            restTemplate.postForEntity(shutdownUrl, entity, Map.class);
            System.out.println("✅ Notificación de shutdown enviada al Master");
        } catch (Exception e) {
            // Silencioso - el Master detectará la ausencia de heartbeats
        }
    }
}