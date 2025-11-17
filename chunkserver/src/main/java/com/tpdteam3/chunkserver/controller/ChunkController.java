package com.tpdteam3.chunkserver.controller;

import com.tpdteam3.chunkserver.service.ChunkStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/chunk")
@CrossOrigin(origins = "*")
public class ChunkController {

    @Autowired
    private ChunkStorageService storageService;

    /**
     * ✅ NUEVO: Health check CON INVENTARIO de chunks
     * Reporta no solo que está vivo, sino QUÉ chunks tiene almacenados
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Chunkserver");

        // ✅ NUEVO: Incluir inventario de chunks
        try {
            Map<String, List<Integer>> inventory = storageService.getChunkInventory();
            response.put("inventory", inventory);
            response.put("totalChunks", inventory.values().stream()
                    .mapToInt(List::size)
                    .sum());
        } catch (Exception e) {
            response.put("inventoryError", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    /**
     * ✅ NUEVO: Endpoint para verificar chunks específicos
     * El Master puede preguntar: "¿Tienes estos chunks?"
     */
    @PostMapping("/verify")
    public ResponseEntity<Map<String, Object>> verifyChunks(@RequestBody Map<String, Object> request) {
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> chunksToVerify =
                    (List<Map<String, Object>>) request.get("chunks");

            Map<String, Object> response = new HashMap<>();
            List<Map<String, Object>> verificationResults = new ArrayList<>();

            for (Map<String, Object> chunk : chunksToVerify) {
                String imagenId = (String) chunk.get("imagenId");
                Integer chunkIndex = (Integer) chunk.get("chunkIndex");

                Map<String, Object> result = new HashMap<>();
                result.put("imagenId", imagenId);
                result.put("chunkIndex", chunkIndex);
                result.put("exists", storageService.chunkExists(imagenId, chunkIndex));

                verificationResults.add(result);
            }

            response.put("status", "success");
            response.put("results", verificationResults);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(500).body(error);
        }
    }


    /**
     * Endpoint para escribir un fragmento
     */
    @PostMapping("/write")
    public ResponseEntity<Map<String, String>> writeChunk(@RequestBody Map<String, Object> request) {
        try {
            String imagenId = (String) request.get("imagenId");
            Integer chunkIndex = (Integer) request.get("chunkIndex");
            String data = (String) request.get("data");

            // ✅ MEJORA: Validaciones más específicas
            if (imagenId == null || imagenId.trim().isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "imagenId es requerido y no puede estar vacío");
                return ResponseEntity.badRequest().body(error);
            }

            if (chunkIndex == null || chunkIndex < 0) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "chunkIndex debe ser un número >= 0");
                return ResponseEntity.badRequest().body(error);
            }

            if (data == null || data.trim().isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "data es requerido y no puede estar vacío");
                return ResponseEntity.badRequest().body(error);
            }

            storageService.writeChunk(imagenId, chunkIndex, data);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Fragmento almacenado correctamente");

            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            // Errores de validación (ej: Base64 inválido)
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Datos inválidos: " + e.getMessage());
            return ResponseEntity.badRequest().body(error);
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
            // ✅ MEJORA: Validación de parámetros
            if (imagenId == null || imagenId.trim().isEmpty()) {
                Map<String, Object> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "imagenId es requerido");
                return ResponseEntity.badRequest().body(error);
            }

            if (chunkIndex < 0) {
                Map<String, Object> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "chunkIndex debe ser >= 0");
                return ResponseEntity.badRequest().body(error);
            }

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
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error interno: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
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
            // ✅ MEJORA: Validación de parámetros
            if (imagenId == null || imagenId.trim().isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "imagenId es requerido");
                return ResponseEntity.badRequest().body(error);
            }

            if (chunkIndex < 0) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "chunkIndex debe ser >= 0");
                return ResponseEntity.badRequest().body(error);
            }

            storageService.deleteChunk(imagenId, chunkIndex);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Fragmento eliminado correctamente");

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
     * Endpoint para eliminar todos los fragmentos de una imagen
     */
    @DeleteMapping("/deleteAll")
    public ResponseEntity<Map<String, String>> deleteAllChunks(@RequestParam String imagenId) {
        try {
            // ✅ MEJORA: Validación de parámetros
            if (imagenId == null || imagenId.trim().isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "imagenId es requerido");
                return ResponseEntity.badRequest().body(error);
            }

            storageService.deleteAllChunks(imagenId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Todos los fragmentos eliminados");

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
     * Endpoint para verificar si un fragmento existe
     */
    @GetMapping("/exists")
    public ResponseEntity<Map<String, Object>> chunkExists(
            @RequestParam String imagenId,
            @RequestParam int chunkIndex) {
        try {
            // ✅ MEJORA: Validación de parámetros
            if (imagenId == null || imagenId.trim().isEmpty()) {
                Map<String, Object> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "imagenId es requerido");
                return ResponseEntity.badRequest().body(error);
            }

            boolean exists = storageService.chunkExists(imagenId, chunkIndex);

            Map<String, Object> response = new HashMap<>();
            response.put("exists", exists);
            response.put("imagenId", imagenId);
            response.put("chunkIndex", chunkIndex);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error verificando existencia: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para obtener estadísticas
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        try {
            return ResponseEntity.ok(storageService.getStats());
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error obteniendo estadísticas: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}