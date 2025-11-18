package com.tpdteam3.chunkserver.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Servicio que gestiona el registro automático del chunkserver con el Master
 */
@Service
public class RegistrationService {

    @Value("${server.port}")
    private int serverPort;

    @Value("${server.servlet.context-path:/}")
    private String contextPath;

    @Value("${chunkserver.id}")
    private String chunkserverId;

    @Value("${chunkserver.master.url:http://localhost:9000/master}")
    private String masterUrl;

    @Value("${chunkserver.hostname:localhost}")
    private String hostname;

    @Value("${chunkserver.registration.retry.max:5}")
    private int maxRetries;

    @Value("${chunkserver.registration.retry.delay:5}")
    private int retryDelaySeconds;

    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private String chunkserverUrl;
    private boolean registered = false;

    // ✅ NUEVO: Tracking de conexión con el Master
    private int consecutiveFailures = 0;
    private static final int MAX_FAILURES_BEFORE_REREGISTER = 3;

    public RegistrationService() {
        this.restTemplate = new RestTemplate();
    }

    @PostConstruct
    public void init() {
        // Construir URL del chunkserver
        String cleanContextPath = contextPath.equals("/") ? "" : contextPath;
        chunkserverUrl = String.format("http://%s:%d%s", hostname, serverPort, cleanContextPath);

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📡 INICIANDO AUTO-REGISTRO DE CHUNKSERVER            ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ID: " + chunkserverId);
        System.out.println("   URL: " + chunkserverUrl);
        System.out.println("   Master: " + masterUrl);
        System.out.println();

        // Intentar registro con reintentos
        registerWithRetry();

        // ✅ MEJORADO: Programar verificación periódica cada 15 segundos
        // Esto detectará cuando el Master se reinicie
        scheduler.scheduleAtFixedRate(
                this::verifyAndMaintainRegistration,
                15,
                15,
                TimeUnit.SECONDS
        );
    }

    @PreDestroy
    public void shutdown() {
        System.out.println("🛑 Desregistrando chunkserver del Master...");

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (registered) {
            unregisterFromMaster();
        }
    }

    /**
     * Intenta registrarse con el Master con reintentos
     */
    private void registerWithRetry() {
        int attempts = 0;

        while (attempts < maxRetries && !registered) {
            attempts++;

            try {
                System.out.println("📡 Intento de registro #" + attempts + "...");
                registerWithMaster();
                registered = true;
                consecutiveFailures = 0; // ✅ NUEVO: Reset contador
                System.out.println("✅ Registro exitoso en el Master!");
                System.out.println();
                return;
            } catch (Exception e) {
                System.err.println("❌ Error en intento #" + attempts + ": " + e.getMessage());

                if (attempts < maxRetries) {
                    System.out.println("⏳ Reintentando en " + retryDelaySeconds + " segundos...");
                    try {
                        Thread.sleep(retryDelaySeconds * 1000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        if (!registered) {
            System.err.println("❌ ADVERTENCIA: No se pudo registrar en el Master después de " +
                               maxRetries + " intentos");
            System.err.println("   El chunkserver continuará ejecutándose pero no recibirá peticiones");
            System.err.println("   Verifique que el Master esté disponible en: " + masterUrl);
            System.err.println();
        }
    }

    /**
     * Registra el chunkserver en el Master
     */
    private void registerWithMaster() {
        String registerUrl = masterUrl + "/api/master/register";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, String> request = new HashMap<>();
        request.put("url", chunkserverUrl);
        request.put("id", chunkserverId);

        HttpEntity<Map<String, String>> entity = new HttpEntity<>(request, headers);

        ResponseEntity<Map> response = restTemplate.postForEntity(registerUrl, entity, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Master rechazó el registro: " + response.getStatusCode());
        }
    }

    /**
     * ✅ NUEVO: Verifica el estado del Master y mantiene el registro activo
     * Si el Master se reinició, detecta que no nos conoce y nos re-registramos
     */
    private void verifyAndMaintainRegistration() {
        try {
            // Verificar si el Master nos conoce
            String chunkserversUrl = masterUrl + "/api/master/chunkservers";
            ResponseEntity<Map> response = restTemplate.getForEntity(chunkserversUrl, Map.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> body = response.getBody();

                if (body != null) {
                    // Verificar si estamos en la lista de servidores
                    @SuppressWarnings("unchecked")
                    java.util.List<String> allServers =
                            (java.util.List<String>) body.get("allServers");

                    if (allServers == null || !allServers.contains(chunkserverUrl)) {
                        // El Master no nos conoce - probablemente se reinició
                        System.out.println("⚠️  DETECTADO: Master no nos tiene registrado");
                        System.out.println("   (El Master probablemente se reinició)");
                        System.out.println("   🔄 Iniciando re-registro...");

                        registered = false;
                        consecutiveFailures = 0;
                        registerWithRetry();

                    } else {
                        // ✅ Todo bien - el Master nos conoce
                        consecutiveFailures = 0;

                        if (!registered) {
                            System.out.println("✅ Reconexión confirmada con el Master");
                            registered = true;
                        }
                    }
                }
            }

        } catch (Exception e) {
            consecutiveFailures++;

            if (consecutiveFailures >= MAX_FAILURES_BEFORE_REREGISTER) {
                if (registered) {
                    System.err.println("⚠️  Perdida comunicación con Master (" +
                                       consecutiveFailures + " fallos consecutivos)");
                    System.err.println("   Esperando reconexión...");
                    registered = false;
                }

                // Intentar re-registrarse
                try {
                    registerWithMaster();
                    registered = true;
                    consecutiveFailures = 0;
                    System.out.println("✅ Re-registro exitoso después de fallos");
                } catch (Exception reregisterEx) {
                    // Falló el re-registro, seguir intentando en próximo ciclo
                }
            }
        }
    }

    /**
     * Desregistra el chunkserver del Master (shutdown graceful)
     */
    private void unregisterFromMaster() {
        try {
            String unregisterUrl = masterUrl + "/api/master/unregister";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, String> request = new HashMap<>();
            request.put("url", chunkserverUrl);

            HttpEntity<Map<String, String>> entity = new HttpEntity<>(request, headers);

            restTemplate.postForEntity(unregisterUrl, entity, Map.class);
            System.out.println("✅ Desregistro exitoso del Master");
        } catch (Exception e) {
            System.err.println("⚠️  No se pudo desregistrar del Master: " + e.getMessage());
        }
    }

    public boolean isRegistered() {
        return registered;
    }

    public String getChunkserverUrl() {
        return chunkserverUrl;
    }
}