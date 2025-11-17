package com.tpdteam3.master.service;

import com.tpdteam3.master.model.ChunkserverStatus;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Servicio que monitorea continuamente la salud e integridad de los chunkservers.
 * <p>
 * Responsabilidades:
 * 1. Verificar que cada chunkserver esté respondiendo (health check)
 * 2. Obtener inventario de chunks de cada servidor
 * 3. Mantener lista actualizada de servidores activos/inactivos
 * 4. Proveer información para que otros servicios puedan actuar
 */
@Service
public class ChunkserverHealthMonitor {

    @Autowired
    private IntegrityMonitorService integrityMonitor;

    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Almacena el estado de salud de cada chunkserver registrado
    private final Map<String, ChunkserverStatus> chunkserverStatuses = new ConcurrentHashMap<>();

    // Configuración de health checks
    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 10; // Verificar cada 10 segundos
    private static final int HEALTH_CHECK_TIMEOUT_MS = 3000;     // Timeout de 3 segundos
    private static final int MAX_CONSECUTIVE_FAILURES = 3;        // Marcar como DOWN después de 3 fallos
    private static final int RECOVERY_THRESHOLD = 2;              // Marcar como UP después de 2 éxitos

    /**
     * Constructor que configura el RestTemplate con timeouts apropiados.
     * Los timeouts evitan que health checks bloqueados afecten el sistema.
     */
    public ChunkserverHealthMonitor() {
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(HEALTH_CHECK_TIMEOUT_MS);
        factory.setReadTimeout(HEALTH_CHECK_TIMEOUT_MS);
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * Inicia el monitoreo periódico de salud al arrancar el servicio.
     * Se ejecuta automáticamente después de la construcción del bean.
     */
    @PostConstruct
    public void startMonitoring() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🏥 HEALTH MONITOR - MODO PASIVO                      ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("⚠️  Este servicio está en modo pasivo");
        System.out.println("✅ Los heartbeats son recibidos por MasterHeartbeatHandler");
        System.out.println();
    }


    /**
     * Detiene el monitoreo al apagar el servicio.
     * Limpia recursos del scheduler de forma ordenada.
     */
    @PreDestroy
    public void stopMonitoring() {
        System.out.println("🛑 Deteniendo Health Monitor...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Registra un nuevo chunkserver para monitoreo continuo.
     * Se llama automáticamente cuando un chunkserver se registra en el Master.
     *
     * @param url URL base del chunkserver (ej: http://localhost:9001/chunkserver1)
     */
    public void registerChunkserver(String url) {
        if (!chunkserverStatuses.containsKey(url)) {
            chunkserverStatuses.put(url, new ChunkserverStatus(url));
            System.out.println("📋 Chunkserver registrado para monitoreo: " + url);
        }
    }

    /**
     * Remueve un chunkserver del monitoreo.
     * Se llama cuando un chunkserver se desregistra del Master.
     *
     * @param url URL base del chunkserver a remover
     */
    public void unregisterChunkserver(String url) {
        chunkserverStatuses.remove(url);
        System.out.println("📋 Chunkserver removido del monitoreo: " + url);
    }
}