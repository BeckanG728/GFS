package com.tpdteam3.backend.controller;

import com.tpdteam3.backend.entity.Producto;
import com.tpdteam3.backend.service.DFSService;
import com.tpdteam3.backend.service.ProductoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/imagenes")
public class ImagenController {

    private static final Logger logger = LoggerFactory.getLogger(ImagenController.class);

    @Autowired
    private DFSService dfsService;

    @Autowired
    private ProductoService productoService;

    /**
     * Subir imagen para un producto
     */
    @PostMapping("/upload/{productoId}")
    public ResponseEntity<Map<String, String>> uploadImagen(
            @PathVariable Integer productoId,
            @RequestParam("file") MultipartFile file) {

        logger.info("Solicitud de upload de imagen - ProductoID: {}, Filename: {}",
                productoId, file.getOriginalFilename());

        try {
            // 1. Validar que el archivo sea una imagen
            String contentType = file.getContentType();
            if (contentType == null || !contentType.startsWith("image/")) {
                logger.warn("Archivo invalido - ProductoID: {}, ContentType: {}", productoId, contentType);
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "El archivo debe ser una imagen");
                return ResponseEntity.badRequest().body(error);
            }

            // 2. Obtener producto
            Producto producto;
            try {
                producto = productoService.obtener(productoId);
                logger.debug("Producto encontrado - ID: {}, Nombre: {}", productoId, producto.getNombProd());
            } catch (RuntimeException e) {
                logger.error("Producto no encontrado - ID: {}", productoId);
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "Producto no encontrado con ID: " + productoId);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
            }

            // 3. Si ya tiene imagen, eliminar la anterior
            if (producto.getImagenId() != null) {
                try {
                    dfsService.deleteImagen(producto.getImagenId());
                    logger.info("Imagen anterior eliminada - ImagenID: {}", producto.getImagenId());
                } catch (Exception e) {
                    logger.warn("Error eliminando imagen anterior - ImagenID: {}, Error: {}",
                            producto.getImagenId(), e.getMessage());
                }
            }

            // 4. Subir nueva imagen al sistema distribuido
            logger.info("Subiendo imagen al DFS - ProductoID: {}", productoId);
            String imagenId = dfsService.uploadImagen(file);
            logger.info("Imagen subida exitosamente - ImagenID: {}", imagenId);

            // 5. Actualizar producto con referencia a la imagen
            producto.setImagenId(imagenId);
            producto.setImagenNombre(file.getOriginalFilename());
            productoService.actualizar(producto);
            logger.info("Producto actualizado en BD - ProductoID: {}, ImagenID: {}", productoId, imagenId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Imagen subida correctamente");
            response.put("imagenId", imagenId);

            return ResponseEntity.ok(response);

        } catch (ResourceAccessException e) {
            // Error de conexión con Master Service
            logger.error("Error de conexion con Master Service - ProductoID: {}, Error: {}",
                    productoId, e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "No se puede conectar al Master Service. Verifica que esté corriendo en http://localhost:9000/master");
            error.put("detalle", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error);

        } catch (Exception e) {
            // Cualquier otro error
            logger.error("Error general al subir imagen - ProductoID: {}, Error: {}",
                    productoId, e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error al subir imagen: " + e.getMessage());
            error.put("tipo", e.getClass().getSimpleName());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Descargar imagen de un producto
     */
    @GetMapping("/download/{productoId}")
    public ResponseEntity<byte[]> downloadImagen(@PathVariable Integer productoId) {
        logger.debug("Solicitud de download de imagen - ProductoID: {}", productoId);
        try {
            // Obtener producto
            Producto producto = productoService.obtener(productoId);

            if (producto.getImagenId() == null) {
                logger.warn("Producto sin imagen - ProductoID: {}", productoId);
                return ResponseEntity.notFound().build();
            }

            // Descargar imagen del sistema distribuido
            byte[] imageData = dfsService.downloadImagen(producto.getImagenId());

            // Determinar tipo de contenido basado en extensión
            String contentType = MediaType.IMAGE_JPEG_VALUE;
            if (producto.getImagenNombre() != null) {
                if (producto.getImagenNombre().toLowerCase().endsWith(".png")) {
                    contentType = MediaType.IMAGE_PNG_VALUE;
                } else if (producto.getImagenNombre().toLowerCase().endsWith(".gif")) {
                    contentType = MediaType.IMAGE_GIF_VALUE;
                }
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.parseMediaType(contentType));
            headers.setContentLength(imageData.length);

            logger.info("Imagen descargada exitosamente - ProductoID: {}, Size: {} bytes",
                    productoId, imageData.length);
            return new ResponseEntity<>(imageData, headers, HttpStatus.OK);

        } catch (RuntimeException e) {
            logger.error("Error en download de imagen - ProductoID: {}, Error: {}", productoId, e.getMessage());
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            logger.error("Error general en download de imagen - ProductoID: {}, Error: {}",
                    productoId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Eliminar imagen de un producto
     */
    @DeleteMapping("/{productoId}")
    public ResponseEntity<Map<String, String>> deleteImagen(@PathVariable Integer productoId) {
        logger.info("Solicitud de eliminacion de imagen - ProductoID: {}", productoId);
        try {
            Producto producto = productoService.obtener(productoId);

            if (producto.getImagenId() == null) {
                logger.warn("Intento de eliminar imagen inexistente - ProductoID: {}", productoId);
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "El producto no tiene imagen");
                return ResponseEntity.badRequest().body(error);
            }

            // Eliminar del sistema distribuido
            dfsService.deleteImagen(producto.getImagenId());

            // Actualizar producto
            producto.setImagenId(null);
            producto.setImagenNombre(null);
            productoService.actualizar(producto);
            logger.info("Imagen eliminada exitosamente - ProductoID: {}", productoId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Imagen eliminada correctamente");

            return ResponseEntity.ok(response);

        } catch (RuntimeException e) {
            logger.error("Producto no encontrado al eliminar imagen - ProductoID: {}", productoId);
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Producto no encontrado");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        } catch (Exception e) {
            logger.error("Error al eliminar imagen - ProductoID: {}, Error: {}",
                    productoId, e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error al eliminar imagen: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}