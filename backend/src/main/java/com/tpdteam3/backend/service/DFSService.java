package com.tpdteam3.backend.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class DFSService {

    private static final Logger logger = LoggerFactory.getLogger(DFSService.class);
    private static final int CHUNK_SIZE = 32 * 1024; // 32KB

    private final DFSMasterClient masterClient;
    private final DFSChunkserverClient chunkServerClient;

    @Autowired
    public DFSService(DFSMasterClient masterClient, DFSChunkserverClient chunkServerClient) {
        this.masterClient = masterClient;
        this.chunkServerClient = chunkServerClient;
    }

    public String uploadImagen(MultipartFile file) throws Exception {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("El archivo no puede estar vacío");
        }

        String imagenId = UUID.randomUUID().toString();
        byte[] imageBytes = file.getBytes();

        logger.info("Iniciando upload de imagen - ID: {}, Size: {} bytes", imagenId, imageBytes.length);

        List<Map<String, Object>> allChunks = masterClient.requestUploadChunks(imagenId, imageBytes.length);

        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(chunk -> (Integer) chunk.get("chunkIndex")));

        logger.info("Chunks asignados - Total chunks: {}, Total replicas: {}",
                chunksByIndex.size(), allChunks.size());

        int offset = 0;
        int successfulWrites = 0;
        int failedWrites = 0;

        for (Map.Entry<Integer, List<Map<String, Object>>> entry : chunksByIndex.entrySet()) {
            int chunkIndex = entry.getKey();
            List<Map<String, Object>> replicas = entry.getValue();

            int length = Math.min(CHUNK_SIZE, imageBytes.length - offset);
            byte[] chunkData = Arrays.copyOfRange(imageBytes, offset, offset + length);
            String base64Data = Base64.getEncoder().encodeToString(chunkData);

            logger.debug("Procesando chunk {} - Size: {} bytes, Replicas: {}",
                    chunkIndex, length, replicas.size());

            for (Map<String, Object> replica : replicas) {
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                Integer replicaIndex = replica.containsKey("replicaIndex")
                        ? (Integer) replica.get("replicaIndex")
                        : 0;

                try {
                    chunkServerClient.writeChunk(imagenId, chunkIndex, base64Data, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    logger.debug("Chunk {} escrito exitosamente - Type: {}, CS: {}",
                            chunkIndex, replicaType, chunkserverUrl);

                    successfulWrites++;
                } catch (Exception e) {
                    String replicaType = replicaIndex == 0 ? "PRIMARY" : "REPLICA_" + replicaIndex;
                    logger.warn("Fallo al escribir chunk {} en {} - Type: {}, Error: {}",
                            chunkIndex, chunkserverUrl, replicaType, e.getMessage());
                    failedWrites++;
                }
            }

            offset += length;
        }

        if (failedWrites > 0) {
            logger.warn("Upload completado con errores - ID: {}, Exitosas: {}, Fallidas: {}",
                    imagenId, successfulWrites, failedWrites);
        } else {
            logger.info("Upload completado exitosamente - ID: {}, Total writes: {}", imagenId, successfulWrites);
        }

        return imagenId;
    }

    public byte[] downloadImagen(String imagenId) throws Exception {
        logger.info("Iniciando download de imagen - ID: {}", imagenId);

        List<Map<String, Object>> allChunks = masterClient.getImageMetadata(imagenId);

        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(chunk -> (Integer) chunk.get("chunkIndex")));

        logger.info("Metadata obtenida - Total chunks: {}, Total replicas: {}",
                chunksByIndex.size(), allChunks.size());

        List<byte[]> chunkDataList = new ArrayList<>(chunksByIndex.size());
        int successfulReads = 0;
        int fallbacksUsed = 0;

        for (int i = 0; i < chunksByIndex.size(); i++) {
            List<Map<String, Object>> replicas = chunksByIndex.get(i);

            if (replicas == null || replicas.isEmpty()) {
                logger.error("Chunk {} no disponible - ID: {}", i, imagenId);
                throw new RuntimeException("Fragmento " + i + " no disponible");
            }

            logger.debug("Procesando chunk {} - Replicas disponibles: {}", i, replicas.size());

            byte[] chunkData = null;
            int attemptCount = 0;

            for (Map<String, Object> replica : replicas) {
                attemptCount++;
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                Integer replicaIndex = replica.containsKey("replicaIndex")
                        ? (Integer) replica.get("replicaIndex")
                        : 0;

                try {
                    chunkData = chunkServerClient.readChunk(imagenId, i, chunkserverUrl); // Data bytes []

                    String replicaType = replicaIndex == 0 ? "PRIMARY" : "REPLICA_" + replicaIndex;
                    logger.debug("Chunk {} leido exitosamente - Type: {}, URL: {}", i,
                            replicaType, chunkserverUrl);

                    successfulReads++;

                    if (attemptCount > 1) {
                        fallbacksUsed++;
                        logger.info("Fallback utilizado para chunk {} - Intento: {}/{}",
                                i, attemptCount, replicas.size());
                    }

                    break;

                } catch (Exception e) {
                    String replicaType = replicaIndex == 0 ? "PRIMARY" : "REPLICA_" + replicaIndex;
                    logger.warn("Error leyendo chunk {} desde {} - Type: {}, Intento: {}/{}, Error: {}",
                            i, chunkserverUrl, replicaType, attemptCount, replicas.size(), e.getMessage());

                    if (attemptCount < replicas.size()) {
                        logger.debug("Intentando siguiente replica para chunk {}", i);
                    }
                }
            }

            if (chunkData == null) {
                logger.error("Fallback agotado para chunk {} - ID: {}, Total replicas intentadas: {}",
                        i, imagenId, replicas.size());
                throw new RuntimeException("FALLBACK AGOTADO: No se pudo leer fragmento " + i +
                                           " desde ninguna de las " + replicas.size() + " réplicas");
            }

            chunkDataList.add(chunkData);
        }

        int totalSize = chunkDataList.stream().mapToInt(chunk -> chunk.length).sum();
        byte[] fullImage = new byte[totalSize];
        int offset = 0;
        for (byte[] chunk : chunkDataList) {
            System.arraycopy(chunk, 0, fullImage, offset, chunk.length);
            offset += chunk.length;
        }

        if (fallbacksUsed > 0) {
            logger.info("Download completado con fallbacks - ID: {}, Size: {} bytes, Chunks leidos: {}, Fallbacks: {}",
                    imagenId, totalSize, successfulReads, fallbacksUsed);
        } else {
            logger.info("Download completado exitosamente - ID: {}, Size: {} bytes, Chunks leidos: {}",
                    imagenId, totalSize, successfulReads);
        }

        return fullImage;
    }

    public void deleteImagen(String imagenId) throws Exception {
        logger.info("Eliminando imagen - ID: {}", imagenId);
        masterClient.deleteImage(imagenId);
        logger.info("Imagen eliminada exitosamente - ID: {}", imagenId);
    }
}
