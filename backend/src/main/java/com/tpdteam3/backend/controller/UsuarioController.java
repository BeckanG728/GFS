package com.tpdteam3.backend.controller;

import com.tpdteam3.backend.dto.RequestLogin;
import com.tpdteam3.backend.service.UsuarioService;
import com.tpdteam3.backend.util.JwtUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class UsuarioController {

    @Autowired
    private UsuarioService usuarioService;

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
}