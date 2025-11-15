package com.tpdteam3.backend.service;


import com.tpdteam3.backend.entity.Producto;
import com.tpdteam3.backend.repository.ProductoRepository;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductoService {
    private final ProductoRepository productoRepository;

    public ProductoService(ProductoRepository productoRepository) {
        this.productoRepository = productoRepository;
    }

    public List<Producto> listar() {
        return productoRepository.findAll();
    }

    public Producto obtener(Integer id) {
        return productoRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Producto no encontrado"));
    }

    public Producto crear(Producto producto) {
        return productoRepository.save(producto);
    }

    public Producto actualizar(Producto producto) {
        try {
            return productoRepository.save(producto);
        } catch (ObjectOptimisticLockingFailureException e) {
            throw new RuntimeException("El producto fue modificado por otro usuario. Recarga y vuelve a intentar");
        }
    }

    public void eliminar(Integer id) {
        productoRepository.deleteById(id);
    }
}
