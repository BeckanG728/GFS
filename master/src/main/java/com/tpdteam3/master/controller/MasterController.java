package com.tpdteam3.master.controller;

import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.service.MasterService;
import com.tpdteam3.master.service.ReplicationMonitorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/master")
@CrossOrigin(origins = "*")
public class MasterController {

    @Autowired
    private MasterService masterService;

    @Autowired
    private ReplicationMonitorService replicationMonitor;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Endpoint para planificar la subida de un archivo CON REPLICACIÓN
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> planUpload(@RequestBody Map<String, Object> request) {
        try {
            String imagenId = (String) request.get("imagenId");
            Number sizeNumber = (Number) request.get("size");
            long size = sizeNumber.longValue();

            FileMetadata metadata = masterService.planUpload(imagenId, size);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("imagenId", metadata.getImagenId());
            response.put("chunks", metadata.getChunks());
            response.put("replicationFactor", metadata.getChunks().size() /
                                              (metadata.getChunks().stream()
                                                       .mapToInt(c -> c.getChunkIndex())
                                                       .max()
                                                       .orElse(0) + 1));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para obtener metadatos de un archivo
     */
    @GetMapping("/metadata")
    public ResponseEntity<Map<String, Object>> getMetadata(@RequestParam String imagenId) {
        try {
            FileMetadata metadata = masterService.getMetadata(imagenId);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("imagenId", metadata.getImagenId());
            response.put("size", metadata.getSize());
            response.put("chunks", metadata.getChunks());
            response.put("timestamp", metadata.getTimestamp());

            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        }
    }

    /**
     * Endpoint para eliminar un archivo Y TODAS SUS RÉPLICAS
     */
    @DeleteMapping("/delete")
    public ResponseEntity<Map<String, String>> deleteFile(@RequestParam String imagenId) {
        try {
            // 1. Obtener metadatos antes de eliminar
            FileMetadata metadata = masterService.getMetadata(imagenId);

            System.out.println("╔════════════════════════════════════════════════════════╗");
            System.out.println("║  🗑️  ELIMINANDO ARCHIVO Y TODAS SUS RÉPLICAS          ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.out.println("   ImagenId: " + imagenId);
            System.out.println("   Total de réplicas: " + metadata.getChunks().size());
            System.out.println();

            int deletedCount = 0;
            int failedCount = 0;

            // 2. Eliminar cada réplica de cada fragmento
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

            // 3. Eliminar metadatos del Master
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
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para listar todos los archivos
     */
    @GetMapping("/files")
    public ResponseEntity<Collection<FileMetadata>> listFiles() {
        return ResponseEntity.ok(masterService.listFiles());
    }

    /**
     * Endpoint para obtener estadísticas del sistema
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        return ResponseEntity.ok(masterService.getStats());
    }

    /**
     * Endpoint para verificar el estado de salud
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
     * Endpoint para registrar un nuevo chunkserver
     */
    @PostMapping("/register")
    public ResponseEntity<Map<String, String>> registerChunkserver(@RequestBody Map<String, String> request) {
        try {
            String url = request.get("url");
            masterService.registerChunkserver(url);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Chunkserver registrado");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para dar de baja un chunkserver
     */
    @PostMapping("/unregister")
    public ResponseEntity<Map<String, String>> unregisterChunkserver(@RequestBody Map<String, String> request) {
        try {
            String url = request.get("url");
            masterService.unregisterChunkserver(url);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Chunkserver dado de baja");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para obtener el estado detallado de salud de todos los chunkservers
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
     * Endpoint para forzar un health check inmediato
     */
    @PostMapping("/health/check")
    public ResponseEntity<Map<String, Object>> forceHealthCheck() {
        // El health monitor se ejecuta automáticamente,
        // pero podemos devolver el estado actual
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Health checks se ejecutan automáticamente cada 10 segundos");
        response.put("currentStatus", masterService.getHealthStatus());
        return ResponseEntity.ok(response);
    }

    /**
     * Endpoint para obtener estadísticas de re-replicación
     */
    @GetMapping("/replication/stats")
    public ResponseEntity<Map<String, Object>> getReplicationStats() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("replicationStats", replicationMonitor.getStats());
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
    }
}