package com.tpdteam3.backend.repository;


import com.tpdteam3.backend.entity.Usuario;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface UsuarioRepository extends JpaRepository<Usuario, Integer> {
    Optional<Usuario> findByLogiUsua(String login);
}
