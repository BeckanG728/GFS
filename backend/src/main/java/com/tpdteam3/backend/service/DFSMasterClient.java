package com.tpdteam3.backend.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DFSMasterClient {

    @Value("${dfs.master.url:https://backend.tpdteam3.com/master}")
    private String masterUrl;

    private final RestTemplate restTemplate;

    public DFSMasterClient() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10000);
        factory.setReadTimeout(30000);
        this.restTemplate = new RestTemplate(factory);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> requestUploadChunks(String imagenId, int size) throws Exception {
        String uploadUrl = masterUrl + "/api/master/upload";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> request = new HashMap<>();
        request.put("imagenId", imagenId);
        request.put("size", size);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        ResponseEntity<Map> response = restTemplate.postForEntity(uploadUrl, entity, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error consultando Master para upload");
        }

        Map<String, Object> responseBody = response.getBody();
        if (responseBody == null) {
            throw new RuntimeException("Respuesta vacía del Master Service");
        }

        List<Map<String, Object>> chunks = (List<Map<String, Object>>) responseBody.get("chunks");
        if (chunks == null || chunks.isEmpty()) {
            throw new RuntimeException("El Master no devolvió información de chunks");
        }

        return chunks;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getImageMetadata(String imagenId) throws Exception {
        if (imagenId == null || imagenId.trim().isEmpty()) {
            throw new IllegalArgumentException("imagenId no puede estar vacío");
        }

        String metadataUrl = masterUrl + "/api/master/metadata?imagenId=" + imagenId;
        ResponseEntity<Map> response = restTemplate.getForEntity(metadataUrl, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error consultando Master");
        }

        Map<String, Object> metadata = response.getBody();
        if (metadata == null) {
            throw new RuntimeException("Respuesta vacía del Master Service");
        }

        List<Map<String, Object>> chunks = (List<Map<String, Object>>) metadata.get("chunks");
        if (chunks == null || chunks.isEmpty()) {
            throw new RuntimeException("No hay chunks disponibles para la imagen: " + imagenId);
        }

        return chunks;
    }

    public void deleteImage(String imagenId) throws Exception {
        if (imagenId == null || imagenId.trim().isEmpty()) {
            throw new IllegalArgumentException("imagenId no puede estar vacío");
        }

        String deleteUrl = masterUrl + "/api/master/delete?imagenId=" + imagenId;
        restTemplate.delete(deleteUrl);
    }
}
