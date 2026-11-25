package com.biblioteca;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Consulta dos efeitos gravados (evidencia para a demonstracao). Disponivel nos consumidores. */
@RestController
@Profile("!produtor")
public class ConsultaController {
    private static final Set<String> TABELAS = Set.of("circulacao_saidas", "avisos", "multas",
        "estatisticas", "estatisticas_log", "eventos_processados");
    private final JdbcTemplate jdbc;

    public ConsultaController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @GetMapping("/consulta/{tabela}")
    public List<Map<String, Object>> tabela(@PathVariable String tabela) {
        if (!TABELAS.contains(tabela)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Use: " + TABELAS);
        return jdbc.queryForList("SELECT * FROM " + tabela + " ORDER BY 1 DESC LIMIT 200");
    }

    // /consulta/auditoria?eventId=...&tipo=...  (historico por identificador, tipo e horario)
    @GetMapping("/consulta/auditoria")
    public List<Map<String, Object>> auditoria(@RequestParam(required = false) String eventId,
                                               @RequestParam(required = false) String tipo) {
        return jdbc.queryForList("SELECT event_id,tipo,ocorrido_em,payload FROM auditoria " +
            "WHERE (?::text IS NULL OR event_id = ?) AND (?::text IS NULL OR tipo = ?) ORDER BY ocorrido_em DESC",
            eventId, eventId, tipo, tipo);
    }
}
