package com.tpdteam3.backend.service;



import com.tpdteam3.backend.entity.Usuario;
import com.tpdteam3.backend.repository.UsuarioRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class UsuarioService {

    @Autowired
    private UsuarioRepository usuarioRepository;


    public boolean validarUsuario(String login, String pass) {
        Optional<Usuario> u = usuarioRepository.findByLogiUsua(login);
        if (u.isPresent()) {
            return u.get().getPassUsua().equals(pass);
        }
        return false;
    }
}
