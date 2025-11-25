package com.tpdteam3.master.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tpdteam3.master.model.FileMetadata;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Servicio para persistir metadatos en disco usando JSON
 * Implementa patrón Write-Ahead con archivo temporal para atomicidad
 */
@Service
public class MetadataPersistenceService {

    @Value("${master.metadata.storage.path:./metadata}")
    private String metadataStoragePath;

    private Path metadataFilePath;
    private Path tempMetadataFilePath;
    private final ObjectMapper objectMapper;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public MetadataPersistenceService() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @PostConstruct
    public void init() throws IOException {
        // Resolver ruta de almacenamiento
        Path storagePath = Paths.get(metadataStoragePath).toAbsolutePath().normalize();

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║       INICIALIZANDO PERSISTENCIA DE METADATOS          ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println(" Ruta de persistencia: " + storagePath);

        // Crear directorio si no existe
        if (!Files.exists(storagePath)) {
            Files.createDirectories(storagePath);
            System.out.println("Directorio de metadatos creado");
        } else {
            System.out.println("Directorio de metadatos existente");
        }

        // Definir rutas de archivos
        metadataFilePath = storagePath.resolve("file_metadata.json");
        tempMetadataFilePath = storagePath.resolve("file_metadata.tmp.json");

        // Verificar permisos
        File storageDir = storagePath.toFile();
        if (!storageDir.canWrite()) {
            System.err.println("ADVERTENCIA: Sin permisos de escritura en: " + storagePath);
        } else {
            System.out.println("Permisos de escritura verificados");
        }

        // Mostrar estado
        if (Files.exists(metadataFilePath)) {
            long size = Files.size(metadataFilePath);
            System.out.println("Archivo de metadatos existente: " + size + " bytes");
        } else {
            System.out.println("Archivo de metadatos será creado en primera escritura");
        }

        System.out.println();
    }

    /**
     * Carga todos los metadatos desde disco
     */
    public Map<String, FileMetadata> loadMetadata() {
        lock.readLock().lock();
        try {
            if (!Files.exists(metadataFilePath)) {
                System.out.println("No hay metadatos previos para cargar");
                return new ConcurrentHashMap<>();
            }

            System.out.println("Cargando metadatos desde disco...");

            // Leer archivo JSON
            Map<String, FileMetadata> metadata = objectMapper.readValue(
                    metadataFilePath.toFile(),
                    objectMapper.getTypeFactory().constructMapType(
                            HashMap.class, String.class, FileMetadata.class
                    )
            );

            System.out.println("Metadatos cargados exitosamente");
            System.out.println("   └─ Total de archivos: " + metadata.size());

            // Mostrar resumen
            if (!metadata.isEmpty()) {
                long totalSize = metadata.values().stream()
                        .mapToLong(FileMetadata::getSize)
                        .sum();
                int totalChunks = metadata.values().stream()
                        .mapToInt(m -> m.getChunks().size())
                        .sum();

                System.out.println("   └─ Tamaño total: " + (totalSize / 1024) + " KB");
                System.out.println("   └─ Total réplicas: " + totalChunks);
            }
            System.out.println();

            return new ConcurrentHashMap<>(metadata);

        } catch (IOException e) {
            System.err.println("ERROR cargando metadatos: " + e.getMessage());
            System.err.println("   Se iniciará con metadatos vacíos");
            e.printStackTrace();
            return new ConcurrentHashMap<>();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Guarda todos los metadatos a disco de forma atómica
     * Usa patrón Write-Ahead: escribe a archivo temporal y luego renombra
     */
    public void saveMetadata(Map<String, FileMetadata> metadata) {
        lock.writeLock().lock();
        try {
            // 1. Escribir a archivo temporal
            objectMapper.writeValue(tempMetadataFilePath.toFile(), metadata);

            // 2. Reemplazar archivo original (operación atómica)
            Files.move(
                    tempMetadataFilePath,
                    metadataFilePath,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
            );

            System.out.println("Metadatos persistidos: " + metadata.size() + " archivos");

        } catch (IOException e) {
            System.err.println("ERROR persistiendo metadatos: " + e.getMessage());
            e.printStackTrace();

            // Intentar limpiar archivo temporal
            try {
                if (Files.exists(tempMetadataFilePath)) {
                    Files.delete(tempMetadataFilePath);
                }
            } catch (IOException cleanupEx) {
                System.err.println("  No se pudo limpiar archivo temporal");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Guarda un solo archivo de metadatos (más eficiente para actualizaciones)
     */
    public void saveFileMetadata(Map<String, FileMetadata> allMetadata) {
        saveMetadata(allMetadata);
    }

    /**
     * Elimina el archivo de metadatos de un archivo específico
     */
    public void deleteFileMetadata(String imagenId, Map<String, FileMetadata> allMetadata) {
        allMetadata.remove(imagenId);
        saveMetadata(allMetadata);
        System.out.println("Metadatos eliminados de persistencia: " + imagenId);
    }

    /**
     * Obtiene estadísticas del sistema de persistencia
     */
    public Map<String, Object> getStorageStats() {
        Map<String, Object> stats = new HashMap<>();

        try {
            if (Files.exists(metadataFilePath)) {
                long fileSize = Files.size(metadataFilePath);
                stats.put("metadataFileExists", true);
                stats.put("metadataFileSizeBytes", fileSize);
                stats.put("metadataFileSizeKB", fileSize / 1024.0);
                stats.put("metadataFilePath", metadataFilePath.toAbsolutePath().toString());
            } else {
                stats.put("metadataFileExists", false);
                stats.put("metadataFilePath", metadataFilePath.toAbsolutePath().toString());
            }

            File storageDir = Paths.get(metadataStoragePath).toFile();
            stats.put("canWrite", storageDir.canWrite());
            stats.put("freeSpaceMB", storageDir.getFreeSpace() / (1024 * 1024));

        } catch (IOException e) {
            stats.put("error", e.getMessage());
        }

        return stats;
    }
}