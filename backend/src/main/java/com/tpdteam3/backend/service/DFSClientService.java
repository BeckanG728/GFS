package com.tpdteam3.backend.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class DFSClientService {

    private static final int CHUNK_SIZE = 32 * 1024; // 32KB

    private final MasterCommunicationService masterService;
    private final ChunkServerCommunicationService chunkServerService;

    @Autowired
    public DFSClientService(MasterCommunicationService masterService, 
                           ChunkServerCommunicationService chunkServerService) {
        this.masterService = masterService;
        this.chunkServerService = chunkServerService;
    }

    public String uploadImagen(MultipartFile file) throws Exception {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("El archivo no puede estar vacío");
        }

        String imagenId = UUID.randomUUID().toString();
        byte[] imageBytes = file.getBytes();

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📤 SUBIENDO IMAGEN (SERVIDORES ACTIVOS)             ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);
        System.out.println("   Tamaño: " + imageBytes.length + " bytes");

        List<Map<String, Object>> allChunks = masterService.requestUploadChunks(imagenId, imageBytes.length);

        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(chunk -> (Integer) chunk.get("chunkIndex")));

        System.out.println("   Fragmentos: " + chunksByIndex.size());
        System.out.println("   Total réplicas: " + allChunks.size());
        System.out.println("   ✅ Todas las réplicas están en servidores ACTIVOS");
        System.out.println();

        int offset = 0;
        int successfulWrites = 0;
        int failedWrites = 0;

        for (Map.Entry<Integer, List<Map<String, Object>>> entry : chunksByIndex.entrySet()) {
            int chunkIndex = entry.getKey();
            List<Map<String, Object>> replicas = entry.getValue();

            int length = Math.min(CHUNK_SIZE, imageBytes.length - offset);
            byte[] chunkData = Arrays.copyOfRange(imageBytes, offset, offset + length);
            String base64Data = Base64.getEncoder().encodeToString(chunkData);

            System.out.println("   📦 Fragmento " + chunkIndex + " (" + length + " bytes):");

            for (Map<String, Object> replica : replicas) {
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                Integer replicaIndex = replica.containsKey("replicaIndex")
                        ? (Integer) replica.get("replicaIndex")
                        : 0;

                try {
                    chunkServerService.writeChunk(imagenId, chunkIndex, base64Data, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("      ✅ [" + replicaType + "] → " + chunkserverUrl);
                    successfulWrites++;
                } catch (Exception e) {
                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.err.println("      ⚠️ [" + replicaType + "] → " + chunkserverUrl +
                                       " - Error inesperado: " + e.getMessage());
                    failedWrites++;
                }
            }

            offset += length;
        }

        System.out.println();
        System.out.println("📊 Resultado:");
        System.out.println("   ✅ Exitosas: " + successfulWrites);
        if (failedWrites > 0) {
            System.out.println("   ⚠️ Fallidas: " + failedWrites + " (inesperadas)");
        }
        System.out.println();

        return imagenId;
    }

    public byte[] downloadImagen(String imagenId) throws Exception {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📥 DESCARGANDO CON FALLBACK INTELIGENTE             ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);

        List<Map<String, Object>> allChunks = masterService.getImageMetadata(imagenId);

        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(chunk -> (Integer) chunk.get("chunkIndex")));

        System.out.println("   Fragmentos: " + chunksByIndex.size());
        System.out.println("   Total réplicas: " + allChunks.size());
        System.out.println("   ✅ Todas las réplicas están en servidores ACTIVOS");
        System.out.println();

        List<byte[]> chunkDataList = new ArrayList<>();
        
        for (int i = 0; i < chunksByIndex.size(); i++) {
            List<Map<String, Object>> replicas = chunksByIndex.get(i);
            byte[] chunkData = null;
            
            for (Map<String, Object> replica : replicas) {
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                try {
                    String base64Data = chunkServerService.readChunk(imagenId, i, chunkserverUrl);
                    chunkData = Base64.getDecoder().decode(base64Data);
                    System.out.println("   ✅ Fragmento " + i + " leído desde: " + chunkserverUrl);
                    break;
                } catch (Exception e) {
                    System.err.println("   ⚠️ Error leyendo fragmento " + i + " desde " + chunkserverUrl + ": " + e.getMessage());
                }
            }
            
            if (chunkData == null) {
                throw new RuntimeException("No se pudo leer el fragmento " + i + " desde ninguna réplica");
            }
            
            chunkDataList.add(chunkData);
        }
        
        int totalSize = chunkDataList.stream().mapToInt(chunk -> chunk.length).sum();
        byte[] imageBytes = new byte[totalSize];
        int offset = 0;
        
        for (byte[] chunk : chunkDataList) {
            System.arraycopy(chunk, 0, imageBytes, offset, chunk.length);
            offset += chunk.length;
        }
        
        System.out.println("   ✅ Imagen reconstruida: " + imageBytes.length + " bytes");
        System.out.println();
        
        return imageBytes;
    }

    public void deleteImagen(String imagenId) throws Exception {
        if (imagenId == null || imagenId.trim().isEmpty()) {
            throw new IllegalArgumentException("imagenId no puede estar vacío");
        }

        System.out.println("🗑️ Eliminando: " + imagenId);
        masterService.deleteImage(imagenId);
    }
}