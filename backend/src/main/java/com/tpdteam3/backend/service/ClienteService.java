package com.tpdteam3.backend.service;



import com.tpdteam3.backend.entity.Cliente;
import com.tpdteam3.backend.repository.ClienteRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ClienteService {
    private final ClienteRepository clienteRepository;

    public ClienteService(ClienteRepository clienteRepository) {
        this.clienteRepository = clienteRepository;
    }

    public List<Cliente> listar() {
        return clienteRepository.findAll();
    }

    public Cliente obtener(Integer id) {
        return clienteRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Cliente no encontrado"));
    }

    public Cliente crear(Cliente cliente) {
        return clienteRepository.save(cliente);
    }

    public Cliente actualizar(Cliente cliente) {
        return clienteRepository.save(cliente);
    }

    public void eliminar(Integer id) {
        clienteRepository.deleteById(id);
    }
}
