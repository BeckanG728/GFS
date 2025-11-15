package com.tpdteam3.backend.controller;


import com.tpdteam3.backend.entity.Producto;
import com.tpdteam3.backend.service.ProductoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/productos")
public class ProductoController {

    @Autowired
    private ProductoService productoService;

    @GetMapping
    public ResponseEntity<List<Producto>> listarProductos() {
        try {
            List<Producto> productos = productoService.listar();
            return ResponseEntity.ok(productos);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<Producto> obtenerProducto(@PathVariable Integer id) {
        try {
            Producto producto = productoService.obtener(id);
            return ResponseEntity.ok(producto);
        } catch (RuntimeException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping
    public ResponseEntity<?> crearProducto(@RequestBody Producto producto) {
        try {
            // Validaciones
            if (producto.getNombProd() == null || producto.getNombProd().trim().isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "El nombre del producto es requerido");
                return ResponseEntity.badRequest().body(error);
            }

            if (producto.getPrecProd() == null || producto.getPrecProd() < 0) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "El precio debe ser mayor o igual a 0");
                return ResponseEntity.badRequest().body(error);
            }

            if (producto.getStocProd() == null || producto.getStocProd() < 0) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "El stock debe ser mayor o igual a 0");
                return ResponseEntity.badRequest().body(error);
            }

            Producto nuevoProducto = productoService.crear(producto);
            return ResponseEntity.status(HttpStatus.CREATED).body(nuevoProducto);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error al crear el producto: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    @PutMapping("/{id}")
    public ResponseEntity<Map<String, String>> actualizarProducto(
            @PathVariable Integer id,
            @RequestBody Producto producto) {
        try {
            producto.setCodiProd(id);
            Producto productoActualizado = productoService.actualizar(producto);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Producto actualizado correctamente");

            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            Map<String, String> errorResponse = new HashMap<>();

            if (e.getMessage().contains("modificado por otro usuario")) {
                errorResponse.put("status", "conflict");
                errorResponse.put("message", e.getMessage());
                return ResponseEntity.status(HttpStatus.CONFLICT).body(errorResponse);
            }

            errorResponse.put("status", "error");
            errorResponse.put("message", "Producto no encontrado");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Error interno del servidor: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, String>> eliminarProducto(@PathVariable Integer id) {
        try {
            productoService.eliminar(id);
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Producto eliminado correctamente");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error al eliminar el producto: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}