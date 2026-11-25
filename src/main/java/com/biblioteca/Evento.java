package com.biblioteca;

import java.time.Instant;
import java.util.Map;

/** Contrato JSON: eventId (unico), tipo (= routing key), ocorridoEm e dados do dominio. */
public record Evento(String eventId, String tipo, Instant ocorridoEm, Map<String, Object> dados) { }
