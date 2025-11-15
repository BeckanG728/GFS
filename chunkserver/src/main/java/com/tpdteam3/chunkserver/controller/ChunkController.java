package com.tpdteam3.chunkserver.controller;

import com.tpdteam3.chunkserver.service.ChunkStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/chunk")
@CrossOrigin(origins = "*")
public class ChunkController {

    @Autowired
    private ChunkStorageService storageService;

    /**
     * Endpoint para escribir un fragmento
     */
    @PostMapping("/write")
    public ResponseEntity<Map<String, String>> writeChunk(@RequestBody Map<String, Object> request) {
        try {
            String imagenId = (String) request.get("imagenId");
            Integer chunkIndex = (Integer) request.get("chunkIndex");
            String data = (String) request.get("data");

            if (imagenId == null || chunkIndex == null || data == null) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "Parámetros inválidos");
                return ResponseEntity.badRequest().body(error);
            }

            storageService.writeChunk(imagenId, chunkIndex, data);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Fragmento almacenado correctamente");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error al escribir fragmento: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para leer un fragmento
     */
    @GetMapping("/read")
    public ResponseEntity<Map<String, Object>> readChunk(
            @RequestParam String imagenId,
            @RequestParam int chunkIndex) {
        try {
            byte[] data = storageService.readChunk(imagenId, chunkIndex);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("imagenId", imagenId);
            response.put("chunkIndex", chunkIndex);
            response.put("data", Base64.getEncoder().encodeToString(data));
            response.put("size", data.length);

            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        }
    }

    /**
     * Endpoint para eliminar un fragmento
     */
    @DeleteMapping("/delete")
    public ResponseEntity<Map<String, String>> deleteChunk(
            @RequestParam String imagenId,
            @RequestParam int chunkIndex) {
        try {
            storageService.deleteChunk(imagenId, chunkIndex);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Fragmento eliminado correctamente");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para eliminar todos los fragmentos de una imagen
     */
    @DeleteMapping("/deleteAll")
    public ResponseEntity<Map<String, String>> deleteAllChunks(@RequestParam String imagenId) {
        try {
            storageService.deleteAllChunks(imagenId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Todos los fragmentos eliminados");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para verificar si un fragmento existe
     */
    @GetMapping("/exists")
    public ResponseEntity<Map<String, Object>> chunkExists(
            @RequestParam String imagenId,
            @RequestParam int chunkIndex) {
        boolean exists = storageService.chunkExists(imagenId, chunkIndex);

        Map<String, Object> response = new HashMap<>();
        response.put("exists", exists);
        response.put("imagenId", imagenId);
        response.put("chunkIndex", chunkIndex);

        return ResponseEntity.ok(response);
    }

    /**
     * Endpoint para obtener estadísticas
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        return ResponseEntity.ok(storageService.getStats());
    }

    /**
     * Endpoint de health check
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Chunkserver");
        return ResponseEntity.ok(response);
    }
}
