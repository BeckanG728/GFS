package com.tpdteam3.backend.repository;


import com.tpdteam3.backend.entity.Cliente;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ClienteRepository extends JpaRepository<Cliente, Integer> {
}
