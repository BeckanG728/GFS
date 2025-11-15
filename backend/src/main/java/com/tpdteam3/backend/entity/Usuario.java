package com.tpdteam3.backend.entity;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "usuario")
public class Usuario {
    @Id
    @Column(name = "codiUsua")
    private Integer codiUsua;

    @Column(name = "logiUsua")
    private String logiUsua;

    @Column(name = "passUsua")
    private String passUsua;

    public Integer getCodiUsua() {
        return codiUsua;
    }

    public void setCodiUsua(Integer codiUsua) {
        this.codiUsua = codiUsua;
    }

    public String getLogiUsua() {
        return logiUsua;
    }

    public void setLogiUsua(String logiUsua) {
        this.logiUsua = logiUsua;
    }

    public String getPassUsua() {
        return passUsua;
    }

    public void setPassUsua(String passUsua) {
        this.passUsua = passUsua;
    }
}
