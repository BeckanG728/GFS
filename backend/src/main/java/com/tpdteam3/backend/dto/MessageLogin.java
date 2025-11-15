package com.tpdteam3.backend.dto;

public class MessageLogin {

    private String resultado;
    private String mensaje;

    public MessageLogin(String resultado, String mensaje) {
        this.resultado = resultado;
        this.mensaje = mensaje;
    }

    public String getResultado() {
        return resultado;
    }

    public String getMensaje() {
        return mensaje;
    }
}
