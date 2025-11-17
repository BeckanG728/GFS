package com.tpdteam3.backend.service;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class ChunkServerCommunicationService {

    private final RestTemplate restTemplate;

    public ChunkServerCommunicationService() {
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10000);
        factory.setReadTimeout(30000);
        this.restTemplate = new RestTemplate(factory);
    }

    public void writeChunk(String imagenId, int chunkIndex, String base64Data, String chunkserverUrl) throws Exception {
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

    public String readChunk(String imagenId, int chunkIndex, String chunkserverUrl) throws Exception {
        String readUrl = chunkserverUrl + "/api/chunk/read?imagenId=" + imagenId + "&chunkIndex=" + chunkIndex;
        ResponseEntity<Map> response = restTemplate.getForEntity(readUrl, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error leyendo chunk desde: " + chunkserverUrl);
        }

        Map<String, Object> responseBody = response.getBody();
        if (responseBody == null) {
            throw new RuntimeException("Respuesta vacía del ChunkServer");
        }

        return (String) responseBody.get("data");
    }
}