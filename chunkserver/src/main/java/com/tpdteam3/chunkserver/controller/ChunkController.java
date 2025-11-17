package com.tpdteam3.chunkserver.controller;

import com.tpdteam3.chunkserver.service.ChunkStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Controlador REST para operaciones de chunks en el chunkserver.
 * Maneja escritura, lectura, eliminación y verificación de salud de fragmentos de archivos.
 */
@RestController
@RequestMapping("/api/chunk")
@CrossOrigin(origins = "*")
public class ChunkController {

    @Autowired
    private ChunkStorageService storageService;

    /**
     * Escribe un fragmento de archivo en disco.
     * Recibe datos en Base64 y los almacena con el nombre: imagenId_chunk_chunkIndex.bin
     *
     * @param request Mapa con imagenId, chunkIndex y data (Base64)
     * @return ResponseEntity con estado de éxito o error
     */
    @PostMapping("/write")
    public ResponseEntity<Map<String, String>> writeChunk(@RequestBody Map<String, Object> request) {
        try {
            String imagenId = (String) request.get("imagenId");
            Integer chunkIndex = (Integer) request.get("chunkIndex");
            String data = (String) request.get("data");

            // Validaciones
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
     * Lee un fragmento de archivo desde disco.
     * Retorna los datos del chunk en formato Base64.
     *
     * @param imagenId   ID único de la imagen
     * @param chunkIndex Índice del fragmento (0, 1, 2, ...)
     * @return ResponseEntity con los datos del chunk en Base64 o error 404 si no existe
     */
    @GetMapping("/read")
    public ResponseEntity<Map<String, Object>> readChunk(
            @RequestParam String imagenId,
            @RequestParam int chunkIndex) {
        try {
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
     * Elimina un fragmento específico de archivo del disco.
     *
     * @param imagenId   ID único de la imagen
     * @param chunkIndex Índice del fragmento a eliminar
     * @return ResponseEntity con confirmación de eliminación o error
     */
    @DeleteMapping("/delete")
    public ResponseEntity<Map<String, String>> deleteChunk(
            @RequestParam String imagenId,
            @RequestParam int chunkIndex) {
        try {
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
     * Elimina todos los fragmentos de una imagen específica del disco.
     *
     * @param imagenId ID único de la imagen cuyos fragmentos se eliminarán
     * @return ResponseEntity con confirmación de eliminación masiva o error
     */
    @DeleteMapping("/deleteAll")
    public ResponseEntity<Map<String, String>> deleteAllChunks(@RequestParam String imagenId) {
        try {
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
     * Verifica si un fragmento específico existe en disco.
     *
     * @param imagenId   ID único de la imagen
     * @param chunkIndex Índice del fragmento a verificar
     * @return ResponseEntity con información de existencia del chunk
     */
    @GetMapping("/exists")
    public ResponseEntity<Map<String, Object>> chunkExists(
            @RequestParam String imagenId,
            @RequestParam int chunkIndex) {
        try {
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
     * Obtiene estadísticas del chunkserver (espacio usado, cantidad de chunks, etc).
     *
     * @return ResponseEntity con estadísticas del servidor
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

    /**
     * ✅ MEJORADO: Health check con INVENTARIO de chunks.
     * <p>
     * Este endpoint es llamado periódicamente por el Master (cada 10 segundos) para:
     * 1. Verificar que el chunkserver está vivo
     * 2. Obtener un inventario completo de qué chunks tiene almacenados
     * <p>
     * El inventario permite al Master detectar:
     * - Chunks que fueron eliminados manualmente del disco
     * - Corrupción de datos
     * - Chunks huérfanos (archivos que el Master no conoce)
     * <p>
     * Formato del inventario: {"imagen-uuid-1": [0, 1, 2], "imagen-uuid-2": [0, 1, 3]}
     *
     * @return ResponseEntity con estado UP + inventario completo de chunks
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Chunkserver");

        try {
            // ✅ NUEVO: Incluir inventario completo de chunks almacenados
            // Esto permite al Master detectar si alguien eliminó chunks manualmente
            Map<String, List<Integer>> inventory = storageService.getChunkInventory();
            response.put("inventory", inventory);

            // Incluir métricas adicionales útiles para el Master
            int totalChunks = inventory.values().stream()
                    .mapToInt(List::size)
                    .sum();
            response.put("totalChunks", totalChunks);
            response.put("totalImages", inventory.size());

        } catch (Exception e) {
            // Si falla obtener inventario, reportarlo pero mantener status UP
            // El chunkserver está vivo pero puede tener problemas de acceso a disco
            response.put("inventoryError", e.getMessage());
            System.err.println("⚠️  Error obteniendo inventario en health check: " + e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    /**
     * ✅ NUEVO: Verifica la existencia de múltiples chunks en una sola llamada.
     * <p>
     * El Master puede usar este endpoint para verificaciones específicas sin
     * pedir el inventario completo, optimizando el tráfico de red.
     *
     * @param request Mapa con lista de chunks a verificar
     * @return ResponseEntity con resultados de verificación para cada chunk
     */
    @PostMapping("/verify")
    public ResponseEntity<Map<String, Object>> verifyChunks(@RequestBody Map<String, Object> request) {
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> chunksToVerify =
                    (List<Map<String, Object>>) request.get("chunks");

            if (chunksToVerify == null || chunksToVerify.isEmpty()) {
                Map<String, Object> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "Se requiere lista de chunks a verificar");
                return ResponseEntity.badRequest().body(error);
            }

            Map<String, Object> response = new HashMap<>();
            List<Map<String, Object>> verificationResults = new java.util.ArrayList<>();

            // Verificar existencia de cada chunk solicitado
            for (Map<String, Object> chunk : chunksToVerify) {
                String imagenId = (String) chunk.get("imagenId");
                Integer chunkIndex = (Integer) chunk.get("chunkIndex");

                if (imagenId == null || chunkIndex == null) {
                    continue; // Saltar entradas inválidas
                }

                Map<String, Object> result = new HashMap<>();
                result.put("imagenId", imagenId);
                result.put("chunkIndex", chunkIndex);
                result.put("exists", storageService.chunkExists(imagenId, chunkIndex));

                verificationResults.add(result);
            }

            response.put("status", "success");
            response.put("results", verificationResults);
            response.put("totalVerified", verificationResults.size());

            return ResponseEntity.ok(response);

        } catch (ClassCastException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Formato de solicitud inválido");
            return ResponseEntity.badRequest().body(error);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error verificando chunks: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}