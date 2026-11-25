package com.biblioteca;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Regra central de todos os consumidores:
 *  1) numa MESMA transacao: registra (consumidor, eventId) e executa o efeito;
 *  2) so depois do commit envia o ACK;
 *  3) se falhar: rollback + NACK sem requeue -> dead-letter (evita loop infinito).
 * Idempotencia: a chave primaria (consumidor, event_id) impede o efeito duplicado.
 */
@Component
public class Processador {
    private static final Logger log = LoggerFactory.getLogger(Processador.class);
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String instancia;

    public Processador(JdbcTemplate jdbc, TransactionTemplate tx, @Value("${instancia:unica}") String instancia) {
        this.jdbc = jdbc; this.tx = tx; this.instancia = instancia;
    }

    public String instancia() { return instancia; }

    public static String s(Evento e, String campo) { return String.valueOf(e.dados().get(campo)); }

    public void processar(String consumidor, Evento ev, Channel ch, long tag, Consumer<Evento> efeito) throws IOException {
        boolean novo;
        try {
            novo = Boolean.TRUE.equals(tx.execute(st -> {
                int n = jdbc.update("INSERT INTO eventos_processados(consumidor,event_id,instancia) VALUES (?,?,?) ON CONFLICT DO NOTHING",
                                    consumidor, ev.eventId(), instancia);
                if (n == 0) return false;      // ja processado antes: duplicata
                efeito.accept(ev);
                return true;
            }));
        } catch (Exception e) {
            log.error("[{}/{}] FALHA no evento {} -> NACK (dead-letter): {}", consumidor, instancia, ev.eventId(), e.getMessage());
            ch.basicNack(tag, false, false);
            return;
        }
        ch.basicAck(tag, false);               // ACK somente apos efeito gravado
        log.info("[{}/{}] {} evento {} ({}) -> ACK", consumidor, instancia,
                 novo ? "PROCESSOU" : "IGNOROU DUPLICADO", ev.eventId(), ev.tipo());
    }
}
