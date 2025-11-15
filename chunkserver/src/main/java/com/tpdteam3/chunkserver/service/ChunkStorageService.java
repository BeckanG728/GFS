package com.tpdteam3.chunkserver.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Service
public class ChunkStorageService {

    @Value("${chunkserver.storage.path:./storage}")
    private String storagePath;

    @Value("${server.port:9001}")
    private int serverPort;

    @Value("${chunkserver.id:chunkserver-2}")
    private String chunkserverId;

    private Path resolvedStoragePath;

    @PostConstruct
    public void init() throws IOException {
        // Resolver la ruta de almacenamiento
        resolvedStoragePath = Paths.get(storagePath).toAbsolutePath().normalize();

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║         🚀 INICIALIZANDO CHUNKSERVER                   ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("ID: " + chunkserverId);
        System.out.println("Puerto: " + serverPort);
        System.out.println("Ruta configurada: " + storagePath);
        System.out.println("Ruta resuelta: " + resolvedStoragePath);

        // Crear directorio si no existe
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

        // Verificar permisos de escritura
        File storageDir = resolvedStoragePath.toFile();
        if (!storageDir.canWrite()) {
            System.err.println("❌ ADVERTENCIA: Sin permisos de escritura en: " + resolvedStoragePath);
        } else {
            System.out.println("✅ Permisos de escritura verificados");
        }

        // Mostrar espacio disponible
        long freeSpace = storageDir.getFreeSpace();
        long totalSpace = storageDir.getTotalSpace();
        System.out.println("💾 Espacio disponible: " + (freeSpace / (1024 * 1024)) + " MB / " +
                           (totalSpace / (1024 * 1024)) + " MB");

        System.out.println("✅ Modo: PERSISTENCIA EN DISCO");
        System.out.println();
    }

    /**
     * Almacena un fragmento EN DISCO
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
     * Lee un fragmento DESDE DISCO
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
     * Elimina un fragmento DEL DISCO
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
     * Elimina todos los fragmentos de una imagen DEL DISCO
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
     * Obtiene estadísticas del servidor (DESDE DISCO)
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
     * Verifica si un fragmento existe EN DISCO
     */
    public boolean chunkExists(String imagenId, int chunkIndex) {
        String filename = generateFilename(imagenId, chunkIndex);
        Path filePath = resolvedStoragePath.resolve(filename);
        return Files.exists(filePath);
    }

    /**
     * Genera nombre de archivo único para un fragmento
     */
    private String generateFilename(String imagenId, int chunkIndex) {
        return imagenId + "_chunk_" + chunkIndex + ".bin";
    }
}