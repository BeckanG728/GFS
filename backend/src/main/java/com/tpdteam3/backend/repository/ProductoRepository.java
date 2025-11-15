package com.tpdteam3.backend.repository;


import com.tpdteam3.backend.entity.Producto;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductoRepository extends JpaRepository<Producto, Integer> {
}
