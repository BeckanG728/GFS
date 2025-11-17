package com.tpdteam3.master.service;

import com.tpdteam3.master.model.ChunkserverStatus;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Servicio que mantiene el registro de chunkservers para monitoreo pasivo.
 * <p>
 * Responsabilidades:
 * 1. Registrar y desregistrar chunkservers
 * 2. Mantener el estado de cada chunkserver registrado
 * 3. Proveer almacenamiento centralizado de estados para otros servicios
 */
@Service
public class ChunkserverRegistry {
    // Almacena el estado de cada chunkserver registrado
    private final Map<String, ChunkserverStatus> chunkserverStatuses = new ConcurrentHashMap<>();

    @PostConstruct
    public void initialize() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📋 CHUNKSERVER REGISTRY - INICIALIZADO               ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("✅ Registro listo para gestionar chunkservers");
        System.out.println();
    }

    /**
     * Registra un nuevo chunkserver en el sistema.
     * Se llama cuando un chunkserver se conecta al Master.
     *
     * @param url URL base del chunkserver (ej: http://localhost:9001/chunkserver1)
     */
    public void registerChunkserver(String url) {
        if (!chunkserverStatuses.containsKey(url)) {
            chunkserverStatuses.put(url, new ChunkserverStatus(url));
            System.out.println("📋 Chunkserver registrado: " + url);
        }
    }

    /**
     * Remueve un chunkserver del registro.
     * Se llama cuando un chunkserver se desconecta del Master.
     *
     * @param url URL base del chunkserver a remover
     */
    public void unregisterChunkserver(String url) {
        chunkserverStatuses.remove(url);
        System.out.println("📋 Chunkserver removido del registro: " + url);
    }
}