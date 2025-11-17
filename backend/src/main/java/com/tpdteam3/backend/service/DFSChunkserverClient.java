package com.tpdteam3.backend.service;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class DFSChunkserverClient {

    private final RestTemplate restTemplate;

    public DFSChunkserverClient() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
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

    public byte[] readChunk(String imagenId, int chunkIndex, String chunkserverUrl) throws Exception {
        String readUrl = chunkserverUrl + "/api/chunk/read?imagenId=" + imagenId + "&chunkIndex=" + chunkIndex;
        ResponseEntity<Map> response = restTemplate.getForEntity(readUrl, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error leyendo chunk");
        }

        Map<String, Object> chunkData = response.getBody();
        if (chunkData == null) {
            throw new RuntimeException("Respuesta vacía del chunkserver");
        }

        String base64Data = (String) chunkData.get("data");
        if (base64Data == null) {
            throw new RuntimeException("Data es null en la respuesta del chunkserver");
        }

        return java.util.Base64.getDecoder().decode(base64Data);
    }
}
