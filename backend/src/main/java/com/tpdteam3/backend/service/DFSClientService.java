package com.tpdteam3.backend.service;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class DFSClientService {

    @Value("${dfs.master.url:https://backend.tpdteam3.com/master}")
    private String masterUrl;

    private final RestTemplate restTemplate = new RestTemplate();
    private static final int CHUNK_SIZE = 32 * 1024; // 32KB por fragmento
    private final Random random = new Random(); // Para selección aleatoria de réplicas

    /**
     * Sube una imagen al sistema distribuido CON REPLICACIÓN
     */
    public String uploadImagen(MultipartFile file) throws Exception {
        String imagenId = UUID.randomUUID().toString();
        byte[] imageBytes = file.getBytes();

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📤 SUBIENDO IMAGEN CON REPLICACIÓN                   ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);
        System.out.println("   Tamaño: " + imageBytes.length + " bytes (" + (imageBytes.length / 1024) + " KB)");

        // 1. Consultar al Master dónde escribir
        String uploadUrl = masterUrl + "/api/master/upload";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> request = new HashMap<>();
        request.put("imagenId", imagenId);
        request.put("size", imageBytes.length);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        ResponseEntity<Map> response = restTemplate.postForEntity(uploadUrl, entity, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error consultando Master para upload");
        }

        // 2. Obtener plan de replicación
        Map<String, Object> responseBody = response.getBody();
        List<Map<String, Object>> allChunks = (List<Map<String, Object>>) responseBody.get("chunks");

        // 3. Agrupar réplicas por chunkIndex
        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(chunk -> (Integer) chunk.get("chunkIndex")));

        System.out.println("   Fragmentos únicos: " + chunksByIndex.size());
        System.out.println("   Total de réplicas: " + allChunks.size());
        System.out.println();

        // 4. Dividir imagen y escribir cada fragmento en TODAS sus réplicas
        int offset = 0;
        int successfulWrites = 0;
        int failedWrites = 0;

        for (Map.Entry<Integer, List<Map<String, Object>>> entry : chunksByIndex.entrySet()) {
            int chunkIndex = entry.getKey();
            List<Map<String, Object>> replicas = entry.getValue();

            // Calcular datos del fragmento
            int length = Math.min(CHUNK_SIZE, imageBytes.length - offset);
            byte[] chunkData = Arrays.copyOfRange(imageBytes, offset, offset + length);
            String base64Data = Base64.getEncoder().encodeToString(chunkData);

            System.out.println("   📦 Fragmento " + chunkIndex + " (" + length + " bytes):");

            // Escribir en TODAS las réplicas
            for (Map<String, Object> replica : replicas) {
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                int replicaIndex = replica.containsKey("replicaIndex")
                        ? (Integer) replica.get("replicaIndex")
                        : 0;

                try {
                    writeChunkToServer(imagenId, chunkIndex, base64Data, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("      ✅ [" + replicaType + "] → " + chunkserverUrl);
                    successfulWrites++;
                } catch (Exception e) {
                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.err.println("      ❌ [" + replicaType + "] → " + chunkserverUrl + " - Error: " + e.getMessage());
                    failedWrites++;
                }
            }

            offset += length;
        }

        System.out.println();
        System.out.println("📊 Resultado de escritura:");
        System.out.println("   ✅ Exitosas: " + successfulWrites);
        System.out.println("   ❌ Fallidas: " + failedWrites);
        System.out.println("   📈 Tasa de éxito: " + (successfulWrites * 100 / (successfulWrites + failedWrites)) + "%");
        System.out.println();

        // Considerar exitoso si al menos una réplica de cada chunk se escribió
        if (successfulWrites < chunksByIndex.size()) {
            throw new RuntimeException("No se pudo escribir al menos una réplica de cada fragmento");
        }

        return imagenId;
    }

    /**
     * Escribe un chunk a un chunkserver específico
     */
    private void writeChunkToServer(String imagenId, int chunkIndex, String base64Data, String chunkserverUrl)
            throws Exception {
        String writeUrl = chunkserverUrl + "/api/chunk/write";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> request = new HashMap<>();
        request.put("imagenId", imagenId);
        request.put("chunkIndex", chunkIndex);
        request.put("data", base64Data);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        restTemplate.postForEntity(writeUrl, entity, String.class);
    }

    /**
     * Descarga una imagen CON LOAD BALANCING y FAILOVER automático
     */
    public byte[] downloadImagen(String imagenId) throws Exception {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📥 DESCARGANDO CON LOAD BALANCING + FAILOVER         ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);

        // 1. Consultar metadatos
        String metadataUrl = masterUrl + "/api/master/metadata?imagenId=" + imagenId;
        ResponseEntity<Map> response = restTemplate.getForEntity(metadataUrl, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error consultando Master para download");
        }

        Map<String, Object> metadata = response.getBody();
        List<Map<String, Object>> allChunks = (List<Map<String, Object>>) metadata.get("chunks");

        // 2. Agrupar réplicas por chunkIndex
        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(chunk -> (Integer) chunk.get("chunkIndex")));

        System.out.println("   Fragmentos a descargar: " + chunksByIndex.size());
        System.out.println("   Réplicas disponibles: " + allChunks.size());

        // Mostrar distribución de réplicas
        Map<String, Integer> replicaDistribution = new HashMap<>();
        for (Map<String, Object> chunk : allChunks) {
            String url = (String) chunk.get("chunkserverUrl");
            replicaDistribution.merge(url, 1, Integer::sum);
        }
        System.out.println("   Distribución de réplicas:");
        replicaDistribution.forEach((url, count) ->
                System.out.println("      " + url + ": " + count + " réplicas"));
        System.out.println();

        // 3. Descargar cada fragmento con LOAD BALANCING y failover
        List<byte[]> chunkDataList = new ArrayList<>(chunksByIndex.size());
        int totalSize = 0;
        int successfulReads = 0;
        int failoverUsed = 0;

        // Estadísticas de uso de servidores
        Map<String, Integer> serverUsageCount = new HashMap<>();

        for (int i = 0; i < chunksByIndex.size(); i++) {
            List<Map<String, Object>> replicas = chunksByIndex.get(i);

            if (replicas == null || replicas.isEmpty()) {
                throw new RuntimeException("No hay réplicas disponibles para fragmento " + i);
            }

            // 🎯 LOAD BALANCING: Mezclar réplicas aleatoriamente
            List<Map<String, Object>> shuffledReplicas = new ArrayList<>(replicas);
            Collections.shuffle(shuffledReplicas, random);

            System.out.println("   📦 Fragmento " + i + " (" + shuffledReplicas.size() + " réplicas disponibles):");

            byte[] chunkData = null;
            boolean readSuccess = false;
            int attemptCount = 0;

            // Intentar leer desde réplicas en orden aleatorio
            for (Map<String, Object> replica : shuffledReplicas) {
                attemptCount++;
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                int replicaIndex = replica.containsKey("replicaIndex")
                        ? (Integer) replica.get("replicaIndex")
                        : 0;

                try {
                    chunkData = readChunkFromServer(imagenId, i, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("      ✅ [" + replicaType + "] → " + chunkserverUrl +
                                       " (" + chunkData.length + " bytes)");

                    // Incrementar contador de uso del servidor
                    serverUsageCount.merge(chunkserverUrl, 1, Integer::sum);

                    readSuccess = true;
                    successfulReads++;

                    // Detectar si fue failover (no primera opción)
                    if (attemptCount > 1) {
                        failoverUsed++;
                        System.out.println("      ⚠️  FAILOVER activado (intento #" + attemptCount + ")");
                    }

                    break; // Salir si lectura exitosa
                } catch (Exception e) {
                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.err.println("      ❌ [" + replicaType + "] → " + chunkserverUrl +
                                       " - Error: " + e.getMessage());

                    // Si no es la última réplica, continuar con la siguiente
                    if (attemptCount < shuffledReplicas.size()) {
                        System.out.println("      🔄 Intentando siguiente réplica...");
                    }
                }
            }

            if (!readSuccess || chunkData == null) {
                throw new RuntimeException("No se pudo leer el fragmento " + i + " desde ninguna réplica");
            }

            chunkDataList.add(chunkData);
            totalSize += chunkData.length;
        }

        // 4. Reconstruir imagen completa
        byte[] fullImage = new byte[totalSize];
        int offset = 0;
        for (byte[] chunk : chunkDataList) {
            System.arraycopy(chunk, 0, fullImage, offset, chunk.length);
            offset += chunk.length;
        }

        System.out.println();
        System.out.println("📊 Resultado de descarga:");
        System.out.println("   ✅ Fragmentos leídos: " + successfulReads);
        System.out.println("   🔄 Failovers usados: " + failoverUsed);
        System.out.println("   📦 Tamaño total: " + totalSize + " bytes");

        // Mostrar distribución de carga
        System.out.println("   🎯 Distribución de carga:");
        int finalSuccessfulReads = successfulReads;
        serverUsageCount.forEach((url, count) ->
                System.out.println("      " + url + ": " + count + " lecturas (" +
                                   (count * 100.0 / finalSuccessfulReads) + "%)"));
        System.out.println();

        return fullImage;
    }

    /**
     * Lee un chunk desde un chunkserver específico
     */
    private byte[] readChunkFromServer(String imagenId, int chunkIndex, String chunkserverUrl)
            throws Exception {
        String readUrl = chunkserverUrl + "/api/chunk/read?imagenId=" + imagenId + "&chunkIndex=" + chunkIndex;
        ResponseEntity<Map> chunkResponse = restTemplate.getForEntity(readUrl, Map.class);

        if (!chunkResponse.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error al leer chunk");
        }

        Map<String, Object> chunkData = chunkResponse.getBody();
        String base64Data = (String) chunkData.get("data");
        return Base64.getDecoder().decode(base64Data);
    }

    /**
     * Elimina una imagen del sistema distribuido (todas las réplicas)
     */
    public void deleteImagen(String imagenId) throws Exception {
        System.out.println("🗑️ Eliminando todas las réplicas de: " + imagenId);
        String deleteUrl = masterUrl + "/api/master/delete?imagenId=" + imagenId;
        restTemplate.delete(deleteUrl);
    }
}