package com.tpdteam3.backend.controller;

import com.tpdteam3.backend.entity.Producto;
import com.tpdteam3.backend.service.ProductoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(ProductoController.class);

    @Autowired
    private ProductoService productoService;

    @GetMapping
    public ResponseEntity<List<Producto>> listarProductos() {
        try {
            List<Producto> productos = productoService.listar();
            return ResponseEntity.ok(productos);
        } catch (Exception e) {
            logger.error("Error al listar productos - Error: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<Producto> obtenerProducto(@PathVariable Integer id) {
        try {
            Producto producto = productoService.obtener(id);
            logger.debug("Producto obtenido - ID: {}", id);
            return ResponseEntity.ok(producto);
        } catch (RuntimeException e) {
            logger.warn("Producto no encontrado - ID: {}", id);
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping
    public ResponseEntity<?> crearProducto(@RequestBody Producto producto) {
        try {
            // Validaciones
            if (producto.getNombProd() == null || producto.getNombProd().trim().isEmpty()) {
                logger.warn("Intento de crear producto sin nombre");
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "El nombre del producto es requerido");
                return ResponseEntity.badRequest().body(error);
            }

            if (producto.getPrecProd() == null || producto.getPrecProd() < 0) {
                logger.warn("Intento de crear producto con precio invalido - Precio: {}", producto.getPrecProd());
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "El precio debe ser mayor o igual a 0");
                return ResponseEntity.badRequest().body(error);
            }

            if (producto.getStocProd() == null || producto.getStocProd() < 0) {
                logger.warn("Intento de crear producto con stock invalido - Stock: {}", producto.getStocProd());
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "El stock debe ser mayor o igual a 0");
                return ResponseEntity.badRequest().body(error);
            }

            Producto nuevoProducto = productoService.crear(producto);
            logger.info("Producto creado exitosamente - ID: {}, Nombre: {}",
                    nuevoProducto.getCodiProd(), nuevoProducto.getNombProd());
            return ResponseEntity.status(HttpStatus.CREATED).body(nuevoProducto);
        } catch (Exception e) {
            logger.error("Error al crear producto - Error: {}", e.getMessage(), e);
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
            productoService.actualizar(producto);
            logger.info("Producto actualizado exitosamente - ID: {}", id);
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Producto actualizado correctamente");

            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            Map<String, String> errorResponse = new HashMap<>();

            if (e.getMessage().contains("modificado por otro usuario")) {
                logger.warn("Conflicto de version optimista - ID: {}", id);
                errorResponse.put("status", "conflict");
                errorResponse.put("message", e.getMessage());
                return ResponseEntity.status(HttpStatus.CONFLICT).body(errorResponse);
            }

            logger.error("Producto no encontrado al actualizar - ID: {}", id);
            errorResponse.put("status", "error");
            errorResponse.put("message", "Producto no encontrado");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
        } catch (Exception e) {
            logger.error("Error al actualizar producto - ID: {}, Error: {}", id, e.getMessage(), e);
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
            logger.info("Producto eliminado exitosamente - ID: {}", id);
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Producto eliminado correctamente");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error al eliminar producto - ID: {}, Error: {}", id, e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error al eliminar el producto: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}