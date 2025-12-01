package com.tpdteam3.backend.controller;

import com.tpdteam3.backend.dto.RequestLogin;
import com.tpdteam3.backend.service.UsuarioService;
import com.tpdteam3.backend.util.JwtUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class UsuarioController {

    @Autowired
    private UsuarioService usuarioService;

    @Value("${spring.datasource.url}")
    private String datasourceUrl;

    @Value("${spring.datasource.username}")
    private String datasourceUsername;

    @Value("${DB_NAME:NO_CONFIGURADO}")
    private String dbName;

    @Value("${DB_USER:NO_CONFIGURADO}")
    private String dbUser;

    @Value("${DB_PASS:NO_CONFIGURADO}")
    private String dbPass;

    @Autowired
    private JwtUtil jwtUtil;

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody RequestLogin request) {
        boolean isValid = usuarioService.validarUsuario(request.getLogin(), request.getPassword());

        if (isValid) {
            // Generar token JWT
            String token = jwtUtil.generateToken(request.getLogin());

            Map<String, String> response = new HashMap<>();
            response.put("resultado", "Ok");
            response.put("mensaje", "Credenciales correctas");
            response.put("token", token);

            return ResponseEntity.ok(response);
        } else {
            Map<String, String> response = new HashMap<>();
            response.put("resultado", "Error");
            response.put("mensaje", "Credenciales inválidas");

            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(response);
        }
    }

    @GetMapping("/verificar-env")
    public ResponseEntity<?> verificarVariablesEntorno() {
        Map<String, Object> info = new HashMap<>();

        // Variables de entorno directas
        info.put("DB_NAME_ENV", System.getenv("DB_NAME"));
        info.put("DB_USER_ENV", System.getenv("DB_USER"));
        info.put("DB_PASS_ENV", System.getenv("DB_PASS"));

        // Valores inyectados por Spring
        info.put("DB_NAME_SPRING", dbName);
        info.put("DB_USER_SPRING", dbUser);
        info.put("DB_PASS_SPRING", dbPass.isEmpty() ? "VACÍO" : "***OCULTO***");

        // URL final de conexión
        info.put("datasource_url", datasourceUrl);
        info.put("datasource_username", datasourceUsername);

        // System properties (alternativa)
        info.put("DB_NAME_SYSPROP", System.getProperty("DB_NAME", "NO_CONFIGURADO"));
        info.put("DB_USER_SYSPROP", System.getProperty("DB_USER", "NO_CONFIGURADO"));

        return ResponseEntity.ok(info);
    }
}