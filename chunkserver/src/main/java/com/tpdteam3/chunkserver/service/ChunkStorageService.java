package com.tpdteam3.chunkserver.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * Servicio que gestiona el almacenamiento físico de chunks en disco.
 * Implementa operaciones CRUD sobre fragmentos de archivos y mantiene inventario.
 */
@Service
public class ChunkStorageService {

    @Value("${chunkserver.storage.path:./storage}")
    private String storagePath;

    @Value("${server.port:9001}")
    private int serverPort;

    @Value("${chunkserver.id:chunkserver-1}")
    private String chunkserverId;

    private Path resolvedStoragePath;

    /**
     * Inicializa el servicio de almacenamiento al arrancar el chunkserver.
     * Crea el directorio de almacenamiento si no existe y verifica permisos.
     *
     * @throws IOException si no se puede crear el directorio o no hay permisos de escritura
     */
    @PostConstruct
    public void init() throws IOException {
        resolvedStoragePath = Paths.get(storagePath).toAbsolutePath().normalize();

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║         🚀 INICIALIZANDO CHUNKSERVER                   ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("ID: " + chunkserverId);
        System.out.println("Puerto: " + serverPort);
        System.out.println("Ruta configurada: " + storagePath);
        System.out.println("Ruta resuelta: " + resolvedStoragePath);

        if (!Files.exists(resolvedStoragePath)) {
            try {
                Files.createDirectories(resolvedStoragePath);
                System.out.println("✅ Directorio de almacenamiento creado");
            } catch (IOException e) {
                System.err.println("❌ ERROR: No se pudo crear el directorio de almacenamiento");
                System.err.println("   Ruta: " + resolvedStoragePath);
                System.err.println("   Error: " + e.getMessage());
                throw e;
            }
        } else {
            System.out.println("✅ Directorio de almacenamiento existente");
        }

        File storageDir = resolvedStoragePath.toFile();
        if (!storageDir.canWrite()) {
            System.err.println("❌ ADVERTENCIA: Sin permisos de escritura en: " + resolvedStoragePath);
        } else {
            System.out.println("✅ Permisos de escritura verificados");
        }

        long freeSpace = storageDir.getFreeSpace();
        long totalSpace = storageDir.getTotalSpace();
        System.out.println("💾 Espacio disponible: " + (freeSpace / (1024 * 1024)) + " MB / " +
                           (totalSpace / (1024 * 1024)) + " MB");

        System.out.println("✅ Modo: PERSISTENCIA EN DISCO");
        System.out.println();
    }

    /**
     * Almacena un fragmento de archivo en disco.
     * Decodifica los datos Base64 y los escribe como archivo binario.
     *
     * @param imagenId   ID único de la imagen
     * @param chunkIndex Índice del fragmento (0, 1, 2, ...)
     * @param base64Data Datos del chunk codificados en Base64
     * @throws RuntimeException si hay error decodificando Base64 o escribiendo a disco
     */
    public void writeChunk(String imagenId, int chunkIndex, String base64Data) {
        try {
            String filename = generateFilename(imagenId, chunkIndex);
            Path filePath = resolvedStoragePath.resolve(filename);

            byte[] data = Base64.getDecoder().decode(base64Data);
            Files.write(filePath, data);

            System.out.println("✅ Fragmento guardado: " + filename + " (" + data.length + " bytes)");
            System.out.println("   Ruta completa: " + filePath.toAbsolutePath());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Error decodificando datos Base64: " + e.getMessage(), e);
        } catch (IOException e) {
            System.err.println("❌ ERROR escribiendo fragmento:");
            System.err.println("   ImagenId: " + imagenId);
            System.err.println("   ChunkIndex: " + chunkIndex);
            System.err.println("   Ruta: " + resolvedStoragePath);
            System.err.println("   Error: " + e.getMessage());
            throw new RuntimeException("Error escribiendo fragmento a disco: " + e.getMessage(), e);
        }
    }

    /**
     * Lee un fragmento de archivo desde disco.
     *
     * @param imagenId   ID único de la imagen
     * @param chunkIndex Índice del fragmento a leer
     * @return Bytes del chunk leído desde disco
     * @throws RuntimeException si el chunk no existe o hay error leyendo
     */
    public byte[] readChunk(String imagenId, int chunkIndex) {
        try {
            String filename = generateFilename(imagenId, chunkIndex);
            Path filePath = resolvedStoragePath.resolve(filename);

            if (!Files.exists(filePath)) {
                throw new RuntimeException("Fragmento no encontrado: " + filename);
            }

            byte[] data = Files.readAllBytes(filePath);
            System.out.println("✅ Fragmento leído: " + filename + " (" + data.length + " bytes)");
            return data;
        } catch (IOException e) {
            System.err.println("❌ ERROR leyendo fragmento:");
            System.err.println("   ImagenId: " + imagenId);
            System.err.println("   ChunkIndex: " + chunkIndex);
            System.err.println("   Error: " + e.getMessage());
            throw new RuntimeException("Error leyendo fragmento desde disco: " + e.getMessage(), e);
        }
    }

    /**
     * Elimina un fragmento específico del disco.
     *
     * @param imagenId   ID único de la imagen
     * @param chunkIndex Índice del fragmento a eliminar
     * @throws RuntimeException si hay error eliminando el archivo
     */
    public void deleteChunk(String imagenId, int chunkIndex) {
        try {
            String filename = generateFilename(imagenId, chunkIndex);
            Path filePath = resolvedStoragePath.resolve(filename);

            if (Files.exists(filePath)) {
                Files.delete(filePath);
                System.out.println("🗑️ Fragmento eliminado: " + filename);
            } else {
                System.out.println("⚠️ Fragmento no encontrado para eliminar: " + filename);
            }
        } catch (IOException e) {
            System.err.println("❌ ERROR eliminando fragmento:");
            System.err.println("   ImagenId: " + imagenId);
            System.err.println("   ChunkIndex: " + chunkIndex);
            System.err.println("   Error: " + e.getMessage());
            throw new RuntimeException("Error eliminando fragmento del disco: " + e.getMessage(), e);
        }
    }

    /**
     * Elimina todos los fragmentos de una imagen específica del disco.
     * Busca y elimina todos los archivos que coincidan con el patrón: imagenId_chunk_*.bin
     *
     * @param imagenId ID único de la imagen cuyos fragmentos se eliminarán
     * @throws RuntimeException si hay error durante la eliminación
     */
    public void deleteAllChunks(String imagenId) {
        try {
            String prefix = imagenId + "_chunk_";

            try (Stream<Path> files = Files.list(resolvedStoragePath)) {
                long deletedCount = files
                        .filter(path -> path.getFileName().toString().startsWith(prefix))
                        .peek(path -> {
                            try {
                                Files.delete(path);
                                System.out.println("🗑️ Eliminado: " + path.getFileName());
                            } catch (IOException e) {
                                System.err.println("❌ Error eliminando: " + path);
                            }
                        })
                        .count();

                System.out.println("🗑️ Total eliminados: " + deletedCount + " fragmentos para imagen: " + imagenId);
            }
        } catch (IOException e) {
            System.err.println("❌ ERROR eliminando fragmentos:");
            System.err.println("   ImagenId: " + imagenId);
            System.err.println("   Error: " + e.getMessage());
            throw new RuntimeException("Error eliminando fragmentos: " + e.getMessage(), e);
        }
    }

    /**
     * ✅ NUEVO: Obtiene un inventario completo de todos los chunks almacenados en disco.
     * <p>
     * Este método escanea el directorio de almacenamiento y construye un mapa donde:
     * - La clave es el imagenId
     * - El valor es una lista de índices de chunks que existen para esa imagen
     * <p>
     * Ejemplo de retorno:
     * {
     * "imagen-uuid-1": [0, 1, 2, 3],
     * "imagen-uuid-2": [0, 1]
     * }
     * <p>
     * Este inventario es usado por el Master en el health check para detectar:
     * 1. Chunks que fueron eliminados manualmente (Master espera chunk pero no está en inventario)
     * 2. Chunks huérfanos (están en inventario pero Master no los conoce)
     *
     * @return Mapa con imagenId como clave y lista de índices de chunks como valor
     */
    public Map<String, List<Integer>> getChunkInventory() {
        try {
            if (!Files.exists(resolvedStoragePath)) {
                return new HashMap<>();
            }

            Map<String, List<Integer>> inventory = new HashMap<>();

            try (Stream<Path> files = Files.list(resolvedStoragePath)) {
                files.filter(Files::isRegularFile)
                        .forEach(path -> {
                            String filename = path.getFileName().toString();

                            // Parsear archivos con formato: imagenId_chunk_N.bin
                            if (filename.matches(".*_chunk_\\d+\\.bin")) {
                                try {
                                    String[] parts = filename.split("_chunk_");
                                    String imagenId = parts[0];
                                    int chunkIndex = Integer.parseInt(
                                            parts[1].replace(".bin", "")
                                    );

                                    // Agregar chunk al inventario
                                    inventory.computeIfAbsent(imagenId, k -> new ArrayList<>())
                                            .add(chunkIndex);
                                } catch (Exception e) {
                                    System.err.println("⚠️ No se pudo parsear archivo: " + filename);
                                }
                            }
                        });
            }

            // Ordenar índices de chunks para facilitar comparaciones
            inventory.values().forEach(Collections::sort);

            return inventory;

        } catch (Exception e) {
            System.err.println("❌ Error obteniendo inventario: " + e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * Verifica si un fragmento específico existe en disco.
     *
     * @param imagenId   ID único de la imagen
     * @param chunkIndex Índice del fragmento a verificar
     * @return true si el chunk existe, false en caso contrario
     */
    public boolean chunkExists(String imagenId, int chunkIndex) {
        String filename = generateFilename(imagenId, chunkIndex);
        Path filePath = resolvedStoragePath.resolve(filename);
        return Files.exists(filePath);
    }

    /**
     * Obtiene estadísticas del chunkserver.
     * Incluye: total de chunks, espacio usado, espacio disponible, etc.
     *
     * @return Mapa con estadísticas del servidor
     */
    public Map<String, Object> getStats() {
        try {
            Map<String, Object> stats = new HashMap<>();

            if (!Files.exists(resolvedStoragePath)) {
                stats.put("totalChunks", 0);
                stats.put("totalStorageUsed", 0L);
                stats.put("storageUsedMB", 0.0);
                stats.put("storagePath", resolvedStoragePath.toString());
                stats.put("status", "directory_not_found");
                return stats;
            }

            try (Stream<Path> files = Files.list(resolvedStoragePath)) {
                long[] totalSize = {0};
                long count = files
                        .filter(Files::isRegularFile)
                        .peek(path -> {
                            try {
                                totalSize[0] += Files.size(path);
                            } catch (IOException e) {
                                // Ignorar
                            }
                        })
                        .count();

                File storageDir = resolvedStoragePath.toFile();
                stats.put("chunkserverId", chunkserverId);
                stats.put("totalChunks", count);
                stats.put("totalStorageUsed", totalSize[0]);
                stats.put("storageUsedMB", totalSize[0] / (1024.0 * 1024.0));
                stats.put("storagePath", resolvedStoragePath.toAbsolutePath().toString());
                stats.put("freeSpaceMB", storageDir.getFreeSpace() / (1024 * 1024));
                stats.put("totalSpaceMB", storageDir.getTotalSpace() / (1024 * 1024));
                stats.put("canWrite", storageDir.canWrite());
                stats.put("status", "ok");
            }

            return stats;
        } catch (IOException e) {
            System.err.println("❌ ERROR obteniendo estadísticas: " + e.getMessage());
            throw new RuntimeException("Error obteniendo estadísticas: " + e.getMessage(), e);
        }
    }

    /**
     * Genera el nombre de archivo para un chunk.
     * Formato: imagenId_chunk_chunkIndex.bin
     *
     * @param imagenId   ID único de la imagen
     * @param chunkIndex Índice del fragmento
     * @return Nombre del archivo generado
     */
    private String generateFilename(String imagenId, int chunkIndex) {
        return imagenId + "_chunk_" + chunkIndex + ".bin";
    }
}