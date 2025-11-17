package com.tpdteam3.backend.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class DFSService {

    private static final int CHUNK_SIZE = 32 * 1024; // 32KB

    private final DFSMasterClient masterClient;
    private final DFSChunkServerClient chunkServerClient;

    @Autowired
    public DFSService(DFSMasterClient masterClient, DFSChunkServerClient chunkServerClient) {
        this.masterClient = masterClient;
        this.chunkServerClient = chunkServerClient;
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

        List<Map<String, Object>> allChunks = masterClient.requestUploadChunks(imagenId, imageBytes.length);

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
                    chunkServerClient.writeChunk(imagenId, chunkIndex, base64Data, chunkserverUrl);

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

        List<Map<String, Object>> allChunks = masterClient.getImageMetadata(imagenId);

        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(chunk -> (Integer) chunk.get("chunkIndex")));

        System.out.println("   Fragmentos: " + chunksByIndex.size());
        System.out.println("   Réplicas disponibles: " + allChunks.size());
        System.out.println("   ✅ Todas pre-filtradas por Health Monitor");
        System.out.println();

        List<byte[]> chunkDataList = new ArrayList<>(chunksByIndex.size());
        int successfulReads = 0;
        int fallbacksUsed = 0;

        for (int i = 0; i < chunksByIndex.size(); i++) {
            List<Map<String, Object>> replicas = chunksByIndex.get(i);

            if (replicas == null || replicas.isEmpty()) {
                throw new RuntimeException("Fragmento " + i + " no disponible");
            }

            System.out.println("   📦 Fragmento " + i + " (" + replicas.size() + " réplicas activas):");

            byte[] chunkData = null;
            int attemptCount = 0;

            for (Map<String, Object> replica : replicas) {
                attemptCount++;
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                Integer replicaIndex = replica.containsKey("replicaIndex")
                        ? (Integer) replica.get("replicaIndex")
                        : 0;

                try {
                    chunkData = chunkServerClient.readChunk(imagenId, i, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("      ✅ [" + replicaType + "] → " + chunkserverUrl);

                    successfulReads++;

                    if (attemptCount > 1) {
                        fallbacksUsed++;
                        System.out.println("      🔄 FALLBACK usado (intento #" + attemptCount + ")");
                    }

                    break;

                } catch (Exception e) {
                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.err.println("      ⚠️ [" + replicaType + "] → " + chunkserverUrl +
                                       " - Error transitorio: " + e.getMessage());

                    if (attemptCount < replicas.size()) {
                        System.out.println("      🔄 Intentando fallback a siguiente réplica...");
                    }
                }
            }

            if (chunkData == null) {
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

        System.out.println();
        System.out.println("📊 Resultado:");
        System.out.println("   ✅ Fragmentos leídos: " + successfulReads);
        System.out.println("   🔄 Fallbacks usados: " + fallbacksUsed);
        System.out.println("   📦 Tamaño total: " + totalSize + " bytes");

        if (fallbacksUsed == 0) {
            System.out.println("   🎯 Eficiencia perfecta: Sin fallbacks necesarios");
        } else {
            System.out.println("   ⚠️ Health checks detectaron " + fallbacksUsed + " fallos transitorios");
        }
        System.out.println();

        return fullImage;
    }

    public void deleteImagen(String imagenId) throws Exception {
        System.out.println("🗑️ Eliminando: " + imagenId);
        masterClient.deleteImage(imagenId);
    }
}
