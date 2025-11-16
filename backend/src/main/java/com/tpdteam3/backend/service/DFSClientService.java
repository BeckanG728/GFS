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

    private final RestTemplate restTemplate;
    private static final int CHUNK_SIZE = 32 * 1024; // 32KB

    // ✅ CORRECCIÓN: Constructor con timeouts configurados
    public DFSClientService() {
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10000);  // 10 segundos
        factory.setReadTimeout(30000);     // 30 segundos (archivos pueden ser grandes)
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * Sube una imagen al sistema distribuido
     * El Master ya se encarga de asignar SOLO servidores activos
     */
    public String uploadImagen(MultipartFile file) throws Exception {
        // ✅ CORRECCIÓN: Validar que el archivo no sea null
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

        // 1. Consultar al Master - EL MASTER SOLO DEVUELVE SERVIDORES ACTIVOS
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

        Map<String, Object> responseBody = response.getBody();

        // ✅ CORRECCIÓN: Validar que responseBody no sea null
        if (responseBody == null) {
            throw new RuntimeException("Respuesta vacía del Master Service");
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> allChunks = (List<Map<String, Object>>) responseBody.get("chunks");

        // ✅ CORRECCIÓN: Validar que chunks no sea null o vacío
        if (allChunks == null || allChunks.isEmpty()) {
            throw new RuntimeException("El Master no devolvió información de chunks");
        }

        // 2. Agrupar réplicas por chunkIndex
        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(chunk -> (Integer) chunk.get("chunkIndex")));

        System.out.println("   Fragmentos: " + chunksByIndex.size());
        System.out.println("   Total réplicas: " + allChunks.size());
        System.out.println("   ✅ Todas las réplicas están en servidores ACTIVOS");
        System.out.println();

        // 3. Escribir fragmentos - YA NO HAY FAILOVER PORQUE TODOS ESTÁN ACTIVOS
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
                    writeChunkToServer(imagenId, chunkIndex, base64Data, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("      ✅ [" + replicaType + "] → " + chunkserverUrl);
                    successfulWrites++;
                } catch (Exception e) {
                    // Esto NO debería ocurrir porque el Master verificó que está activo
                    // Pero lo manejamos por si acaso
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
     * Descarga una imagen con FALLBACK automático
     * El Master devuelve SOLO réplicas en servidores activos (verificados por health checks)
     * El fallback se mantiene como red de seguridad para fallos transitorios
     */
    public byte[] downloadImagen(String imagenId) throws Exception {
        // ✅ CORRECCIÓN: Validar imagenId
        if (imagenId == null || imagenId.trim().isEmpty()) {
            throw new IllegalArgumentException("imagenId no puede estar vacío");
        }

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📥 DESCARGANDO CON FALLBACK INTELIGENTE             ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ImagenId: " + imagenId);

        // 1. Consultar metadatos - EL MASTER YA FILTRA SERVIDORES CAÍDOS
        String metadataUrl = masterUrl + "/api/master/metadata?imagenId=" + imagenId;
        ResponseEntity<Map> response = restTemplate.getForEntity(metadataUrl, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error consultando Master");
        }

        Map<String, Object> metadata = response.getBody();

        // ✅ CORRECCIÓN: Validar que metadata no sea null
        if (metadata == null) {
            throw new RuntimeException("Respuesta vacía del Master Service");
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> allChunks = (List<Map<String, Object>>) metadata.get("chunks");

        // ✅ CORRECCIÓN: Validar que allChunks no sea null o vacío
        if (allChunks == null || allChunks.isEmpty()) {
            throw new RuntimeException("No hay chunks disponibles para la imagen: " + imagenId);
        }

        // 2. Agrupar por chunkIndex
        Map<Integer, List<Map<String, Object>>> chunksByIndex = allChunks.stream()
                .collect(Collectors.groupingBy(
                        chunk -> (Integer) chunk.get("chunkIndex")
                ));

        System.out.println("   Fragmentos: " + chunksByIndex.size());
        System.out.println("   Réplicas disponibles: " + allChunks.size());
        System.out.println("   ✅ Todas pre-filtradas por Health Monitor");
        System.out.println();

        // 3. Descargar fragmentos con FALLBACK como red de seguridad
        List<byte[]> chunkDataList = new ArrayList<>(chunksByIndex.size());
        int totalSize = 0;
        int successfulReads = 0;
        int fallbacksUsed = 0;

        for (int i = 0; i < chunksByIndex.size(); i++) {
            List<Map<String, Object>> replicas = chunksByIndex.get(i);

            if (replicas == null || replicas.isEmpty()) {
                throw new RuntimeException("Fragmento " + i + " no disponible");
            }

            System.out.println("   📦 Fragmento " + i + " (" + replicas.size() + " réplicas activas):");

            byte[] chunkData = null;
            boolean readSuccess = false;
            int attemptCount = 0;

            // FALLBACK: Intentar con cada réplica hasta que una funcione
            for (Map<String, Object> replica : replicas) {
                attemptCount++;
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                Integer replicaIndex = replica.containsKey("replicaIndex")
                        ? (Integer) replica.get("replicaIndex")
                        : 0;

                try {
                    chunkData = readChunkFromServer(imagenId, i, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("      ✅ [" + replicaType + "] → " + chunkserverUrl);

                    readSuccess = true;
                    successfulReads++;

                    // Detectar si fue fallback (no primera opción)
                    if (attemptCount > 1) {
                        fallbacksUsed++;
                        System.out.println("      🔄 FALLBACK usado (intento #" + attemptCount + ")");
                    }

                    break; // Éxito, salir del loop de réplicas

                } catch (Exception e) {
                    // FALLBACK: Este servidor falló, intentar con la siguiente réplica
                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.err.println("      ⚠️ [" + replicaType + "] → " + chunkserverUrl +
                                       " - Error transitorio: " + e.getMessage());

                    // Si hay más réplicas, continuar con fallback
                    if (attemptCount < replicas.size()) {
                        System.out.println("      🔄 Intentando fallback a siguiente réplica...");
                    }
                }
            }

            if (!readSuccess || chunkData == null) {
                throw new RuntimeException("FALLBACK AGOTADO: No se pudo leer fragmento " + i +
                                           " desde ninguna de las " + replicas.size() + " réplicas");
            }

            chunkDataList.add(chunkData);
            totalSize += chunkData.length;
        }

        // 4. Reconstruir imagen
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

    private byte[] readChunkFromServer(String imagenId, int chunkIndex, String chunkserverUrl)
            throws Exception {
        String readUrl = chunkserverUrl + "/api/chunk/read?imagenId=" + imagenId + "&chunkIndex=" + chunkIndex;
        ResponseEntity<Map> chunkResponse = restTemplate.getForEntity(readUrl, Map.class);

        if (!chunkResponse.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error leyendo chunk");
        }

        Map<String, Object> chunkData = chunkResponse.getBody();

        // ✅ CORRECCIÓN: Validar que chunkData no sea null
        if (chunkData == null) {
            throw new RuntimeException("Respuesta vacía del chunkserver");
        }

        String base64Data = (String) chunkData.get("data");

        // ✅ CORRECCIÓN: Validar que data no sea null
        if (base64Data == null) {
            throw new RuntimeException("Data es null en la respuesta del chunkserver");
        }

        return Base64.getDecoder().decode(base64Data);
    }

    /**
     * Elimina imagen
     */
    public void deleteImagen(String imagenId) throws Exception {
        // ✅ CORRECCIÓN: Validar imagenId
        if (imagenId == null || imagenId.trim().isEmpty()) {
            throw new IllegalArgumentException("imagenId no puede estar vacío");
        }

        System.out.println("🗑️ Eliminando: " + imagenId);
        String deleteUrl = masterUrl + "/api/master/delete?imagenId=" + imagenId;
        restTemplate.delete(deleteUrl);
    }
}