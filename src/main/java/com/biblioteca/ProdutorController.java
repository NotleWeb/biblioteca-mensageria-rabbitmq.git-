package com.biblioteca;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Dispara os 4 eventos com dados ficticios. Para testar duplicidade, repita o mesmo eventId. */
@RestController
@Profile("produtor")
public class ProdutorController {
    private static final Set<String> TIPOS = Set.of(
        "emprestimo.registrado", "exemplar.devolvido", "multa.aplicada", "multa.cancelada");
    private static final String[] CATEGORIAS = {"Literatura", "Tecnologia", "Ciencias", "Historia"};
    private final RabbitTemplate rabbit;

    public ProdutorController(RabbitTemplate rabbit) { this.rabbit = rabbit; }

    // POST /eventos/emprestimo-registrado?eventId=abc   (corpo JSON opcional com os "dados")
    @PostMapping("/eventos/{tipo}")
    public Evento publicar(@PathVariable String tipo,
                           @RequestParam(required = false) String eventId,
                           @RequestBody(required = false) Map<String, Object> dados) {
        String chave = tipo.replace('-', '.');
        if (!TIPOS.contains(chave))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Tipo invalido: " + TIPOS);
        Evento ev = new Evento(eventId != null ? eventId : UUID.randomUUID().toString(), chave,
                               Instant.now(), dados != null ? dados : dadosPadrao(chave));
        rabbit.convertAndSend(Topologia.EXCHANGE, chave, ev);
        return ev;
    }

    private Map<String, Object> dadosPadrao(String tipo) {
        Random r = new Random();
        String leitor = "L" + (100 + r.nextInt(900));
        String categoria = CATEGORIAS[r.nextInt(CATEGORIAS.length)];
        Map<String, Object> d = new LinkedHashMap<>();
        switch (tipo) {
            case "emprestimo.registrado" -> {
                d.put("emprestimoId", "E" + (1000 + r.nextInt(9000)));
                d.put("exemplarId", "X" + (100 + r.nextInt(900)));
                d.put("titulo", "Livro de " + categoria);
                d.put("categoria", categoria);
                d.put("leitorId", leitor);
                d.put("dataPrevistaDevolucao", LocalDate.now().plusDays(14).toString());
            }
            case "exemplar.devolvido" -> {
                d.put("emprestimoId", "E" + (1000 + r.nextInt(9000)));
                d.put("exemplarId", "X" + (100 + r.nextInt(900)));
                d.put("leitorId", leitor);
                d.put("dataDevolucao", LocalDate.now().toString());
            }
            default -> { // multa.aplicada / multa.cancelada
                d.put("multaId", "M" + (100 + r.nextInt(900)));
                d.put("leitorId", leitor);
                d.put("valor", 5 + r.nextInt(30));
                d.put("motivo", "Atraso na devolucao");
            }
        }
        return d;
    }
}
