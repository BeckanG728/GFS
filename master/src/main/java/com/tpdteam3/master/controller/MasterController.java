package com.tpdteam3.master.controller;

import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.service.IntegrityMonitorService;
import com.tpdteam3.master.service.MasterHeartbeatHandler;
import com.tpdteam3.master.service.MasterService;
import com.tpdteam3.master.service.ReplicationMonitorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Controlador REST del Master Service.
 * Expone endpoints para gestión de archivos, chunkservers y monitoreo del sistema.
 */
@RestController
@RequestMapping("/api/master")
@CrossOrigin(origins = "*")
public class MasterController {

    @Autowired
    private MasterService masterService;

    @Autowired
    private ReplicationMonitorService replicationMonitor;

    @Autowired
    private IntegrityMonitorService integrityMonitor;

    private final RestTemplate restTemplate = new RestTemplate();


    @Autowired
    private MasterHeartbeatHandler heartbeatHandler;

    /**
     * ✅ NUEVO ENDPOINT: Recibe heartbeats de chunkservers
     * <p>
     * Este endpoint reemplaza el sistema de polling del Master.
     * Los chunkservers llaman este endpoint cada 10 segundos para:
     * 1. Informar que están vivos
     * 2. Enviar inventario actualizado de chunks
     * 3. Reportar métricas de salud
     * <p>
     * El Master puede responder con comandos opcionales que el chunkserver debe ejecutar.
     *
     * @param heartbeatData Datos del heartbeat enviados por el chunkserver
     * @return Respuesta con acknowledgment y comandos opcionales
     */
    @PostMapping("/heartbeat")
    public ResponseEntity<Map<String, Object>> receiveHeartbeat(
            @RequestBody Map<String, Object> heartbeatData) {
        try {
            // Validar datos requeridos
            if (!heartbeatData.containsKey("url") || !heartbeatData.containsKey("chunkserverId")) {
                Map<String, Object> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "Missing required fields: url and chunkserverId");
                return ResponseEntity.badRequest().body(error);
            }

            // Procesar heartbeat
            Map<String, Object> response = heartbeatHandler.processHeartbeat(heartbeatData);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            System.err.println("❌ Error procesando heartbeat: " + e.getMessage());
            e.printStackTrace();

            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error processing heartbeat: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * ✅ MODIFICADO: Ahora usa el heartbeat handler en lugar del health monitor
     */
    @GetMapping("/chunkservers")
    public ResponseEntity<Map<String, Object>> listChunkservers() {
        List<String> allServers = masterService.getAllChunkservers();
        List<String> healthyServers = heartbeatHandler.getHealthyChunkservers();
        List<String> unhealthyServers = heartbeatHandler.getUnhealthyChunkservers();

        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("total", allServers.size());
        response.put("healthy", healthyServers.size());
        response.put("unhealthy", unhealthyServers.size());
        response.put("allServers", allServers);
        response.put("healthyServers", healthyServers);
        response.put("unhealthyServers", unhealthyServers);

        return ResponseEntity.ok(response);
    }


    /**
     * Planifica la subida de un archivo con replicación.
     * Retorna plan de escritura indicando a qué chunkservers escribir cada fragmento.
     *
     * @param request Mapa con imagenId y size del archivo
     * @return Plan de escritura con lista de chunks y sus ubicaciones
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> planUpload(@RequestBody Map<String, Object> request) {
        try {
            String imagenId = (String) request.get("imagenId");
            Number sizeNumber = (Number) request.get("size");

            if (imagenId == null || imagenId.trim().isEmpty()) {
                throw new IllegalArgumentException("imagenId es requerido y no puede estar vacío");
            }
            if (sizeNumber == null || sizeNumber.longValue() <= 0) {
                throw new IllegalArgumentException("size debe ser mayor a 0");
            }

            long size = sizeNumber.longValue();
            FileMetadata metadata = masterService.planUpload(imagenId, size);

            int uniqueChunks = (int) metadata.getChunks().stream()
                    .mapToInt(c -> c.getChunkIndex())
                    .distinct()
                    .count();

            int replicationFactor = uniqueChunks > 0
                    ? metadata.getChunks().size() / uniqueChunks
                    : 0;

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("imagenId", metadata.getImagenId());
            response.put("chunks", metadata.getChunks());
            response.put("replicationFactor", replicationFactor);

            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Obtiene metadatos de un archivo (ubicación de fragmentos y réplicas).
     * Filtra automáticamente réplicas en servidores caídos.
     *
     * @param imagenId ID único de la imagen
     * @return Metadatos del archivo con ubicaciones de chunks
     */
    @GetMapping("/metadata")
    public ResponseEntity<Map<String, Object>> getMetadata(@RequestParam String imagenId) {
        try {
            if (imagenId == null || imagenId.trim().isEmpty()) {
                throw new IllegalArgumentException("imagenId es requerido");
            }

            FileMetadata metadata = masterService.getMetadata(imagenId);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("imagenId", metadata.getImagenId());
            response.put("size", metadata.getSize());
            response.put("chunks", metadata.getChunks());
            response.put("timestamp", metadata.getTimestamp());

            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
        } catch (RuntimeException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        }
    }

    /**
     * Elimina un archivo y todas sus réplicas de todos los chunkservers.
     *
     * @param imagenId ID único de la imagen a eliminar
     * @return Resultado de la eliminación con contadores de éxito/fallo
     */
    @DeleteMapping("/delete")
    public ResponseEntity<Map<String, String>> deleteFile(@RequestParam String imagenId) {
        try {
            if (imagenId == null || imagenId.trim().isEmpty()) {
                throw new IllegalArgumentException("imagenId es requerido");
            }

            FileMetadata metadata = masterService.getMetadata(imagenId);

            System.out.println("╔════════════════════════════════════════════════════════╗");
            System.out.println("║  🗑️  ELIMINANDO ARCHIVO Y TODAS SUS RÉPLICAS          ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.out.println("   ImagenId: " + imagenId);
            System.out.println("   Total de réplicas: " + metadata.getChunks().size());
            System.out.println();

            int deletedCount = 0;
            int failedCount = 0;

            for (FileMetadata.ChunkMetadata chunk : metadata.getChunks()) {
                String chunkserverUrl = chunk.getChunkserverUrl();
                int chunkIndex = chunk.getChunkIndex();
                int replicaIndex = chunk.getReplicaIndex();

                try {
                    String deleteUrl = chunkserverUrl + "/api/chunk/delete?imagenId=" +
                                       imagenId + "&chunkIndex=" + chunkIndex;

                    restTemplate.delete(deleteUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("   ✅ [" + replicaType + "] Chunk " + chunkIndex +
                                       " eliminado de " + chunkserverUrl);
                    deletedCount++;
                } catch (Exception e) {
                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.err.println("   ❌ [" + replicaType + "] Error eliminando chunk " +
                                       chunkIndex + " de " + chunkserverUrl + ": " + e.getMessage());
                    failedCount++;
                }
            }

            masterService.deleteFile(imagenId);

            System.out.println();
            System.out.println("📊 Resultado de eliminación:");
            System.out.println("   ✅ Réplicas eliminadas: " + deletedCount);
            System.out.println("   ❌ Fallos: " + failedCount);
            System.out.println();

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Archivo y réplicas eliminados");
            response.put("replicasDeleted", String.valueOf(deletedCount));
            response.put("replicasFailed", String.valueOf(failedCount));

            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Lista todos los archivos almacenados en el sistema.
     *
     * @return Colección de metadatos de todos los archivos
     */
    @GetMapping("/files")
    public ResponseEntity<Collection<FileMetadata>> listFiles() {
        return ResponseEntity.ok(masterService.listFiles());
    }

    /**
     * Obtiene estadísticas generales del sistema distribuido.
     *
     * @return Mapa con estadísticas: archivos totales, espacio usado, etc.
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        Map<String, Object> stats = masterService.getStats();

        // ✅ NUEVO: Incluir estadísticas de integridad
        stats.put("integrityStats", integrityMonitor.getStats());

        return ResponseEntity.ok(stats);
    }

    /**
     * Verifica el estado de salud del Master Service.
     *
     * @return Estado del servicio y de todos los chunkservers
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Master Service");
        response.putAll(masterService.getHealthStatus());
        return ResponseEntity.ok(response);
    }

    /**
     * Obtiene estado de salud detallado de todos los chunkservers.
     *
     * @return Métricas detalladas de cada chunkserver
     */
    @GetMapping("/health/detailed")
    public ResponseEntity<Map<String, Object>> getDetailedHealth() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("healthStatus", masterService.getHealthStatus());
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
    }

    /**
     * Fuerza un health check inmediato (informativo).
     * Los health checks se ejecutan automáticamente cada 10 segundos.
     *
     * @return Estado actual del sistema
     */
    @PostMapping("/health/check")
    public ResponseEntity<Map<String, Object>> forceHealthCheck() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Health checks se ejecutan automáticamente cada 10 segundos");
        response.put("currentStatus", masterService.getHealthStatus());
        return ResponseEntity.ok(response);
    }

    /**
     * Obtiene estadísticas de re-replicación automática.
     *
     * @return Métricas de operaciones de re-replicación
     */
    @GetMapping("/replication/stats")
    public ResponseEntity<Map<String, Object>> getReplicationStats() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("replicationStats", replicationMonitor.getStats());
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
    }

    /**
     * ✅ NUEVO: Obtiene estadísticas del sistema de integridad y reparación automática.
     *
     * @return Métricas de chunks detectados y reparados
     */
    @GetMapping("/integrity/stats")
    public ResponseEntity<Map<String, Object>> getIntegrityStats() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("integrityStats", integrityMonitor.getStats());
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
    }

    /**
     * Registra un nuevo chunkserver en el sistema.
     * Los chunkservers llaman este endpoint automáticamente al arrancar.
     *
     * @param request Mapa con url (requerida) e id (opcional) del chunkserver
     * @return Confirmación de registro
     */
    @PostMapping("/register")
    public ResponseEntity<Map<String, String>> registerChunkserver(@RequestBody Map<String, String> request) {
        try {
            String url = request.get("url");
            String id = request.get("id");

            if (url == null || url.trim().isEmpty()) {
                throw new IllegalArgumentException("url es requerida");
            }

            if (!url.startsWith("http://") && !url.startsWith("https://")) {
                throw new IllegalArgumentException("URL debe comenzar con http:// o https://");
            }

            masterService.registerChunkserver(url, id);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Chunkserver registrado exitosamente");
            response.put("url", url);

            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Desregistra un chunkserver del sistema.
     * Los chunkservers llaman este endpoint al hacer shutdown graceful.
     *
     * @param request Mapa con url del chunkserver a desregistrar
     * @return Confirmación de desregistro
     */
    @PostMapping("/unregister")
    public ResponseEntity<Map<String, String>> unregisterChunkserver(@RequestBody Map<String, String> request) {
        try {
            String url = request.get("url");

            if (url == null || url.trim().isEmpty()) {
                throw new IllegalArgumentException("url es requerida");
            }

            masterService.unregisterChunkserver(url);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Chunkserver desregistrado exitosamente");

            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}