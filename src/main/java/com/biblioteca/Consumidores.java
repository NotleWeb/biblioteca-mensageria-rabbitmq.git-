package com.biblioteca;

import static com.biblioteca.Processador.s;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/** Os 5 consumidores. Cada um so existe no processo iniciado com o seu perfil. */
public class Consumidores {

    @Component @Profile("circulacao")
    static class Circulacao {
        private final Processador p; private final JdbcTemplate jdbc;
        Circulacao(Processador p, JdbcTemplate jdbc) { this.p = p; this.jdbc = jdbc; }

        @RabbitListener(queues = Topologia.Q_CIRCULACAO)
        public void on(@Payload Evento ev, Channel ch, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
            p.processar("circulacao", ev, ch, tag, e -> jdbc.update(
                "INSERT INTO circulacao_saidas(event_id,exemplar_id,leitor_id,data_prevista_devolucao) VALUES (?,?,?,?)",
                e.eventId(), s(e, "exemplarId"), s(e, "leitorId"), Date.valueOf(LocalDate.parse(s(e, "dataPrevistaDevolucao")))));
        }
    }

    @Component @Profile("comunicacao")
    static class Comunicacao {
        private final Processador p; private final JdbcTemplate jdbc;
        Comunicacao(Processador p, JdbcTemplate jdbc) { this.p = p; this.jdbc = jdbc; }

        @RabbitListener(queues = Topologia.Q_COMUNICACAO)
        public void on(@Payload Evento ev, Channel ch, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
            p.processar("comunicacao", ev, ch, tag, e -> {
                String tipo, msg;
                switch (e.tipo()) {
                    case "emprestimo.registrado" -> { tipo = "EMPRESTIMO";
                        msg = "Emprestimo do exemplar " + s(e, "exemplarId") + " registrado. Devolver ate " + s(e, "dataPrevistaDevolucao"); }
                    case "exemplar.devolvido" -> { tipo = "DEVOLUCAO";
                        msg = "Devolucao do exemplar " + s(e, "exemplarId") + " confirmada"; }
                    default -> { tipo = "MULTA";   // multa.cancelada
                        msg = "Sua multa " + s(e, "multaId") + " (R$ " + s(e, "valor") + ") foi cancelada"; }
                }
                jdbc.update("INSERT INTO avisos(event_id,leitor_id,tipo_aviso,mensagem) VALUES (?,?,?,?)",
                            e.eventId(), s(e, "leitorId"), tipo, msg);   // "envio" simulado = registro
            });
        }
    }

    @Component @Profile("multas")
    static class Multas {
        private final Processador p; private final JdbcTemplate jdbc;
        Multas(Processador p, JdbcTemplate jdbc) { this.p = p; this.jdbc = jdbc; }

        @RabbitListener(queues = Topologia.Q_MULTAS)
        public void on(@Payload Evento ev, Channel ch, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
            p.processar("multas", ev, ch, tag, e -> jdbc.update(
                "INSERT INTO multas(event_id,multa_id,leitor_id,valor,status) VALUES (?,?,?,?,?)",
                e.eventId(), s(e, "multaId"), s(e, "leitorId"), new BigDecimal(s(e, "valor")),
                e.tipo().endsWith("aplicada") ? "APLICADA" : e.tipo().endsWith("cancelada") ? "CANCELADA" : e.tipo()));
        }
    }

    @Component @Profile("auditoria")
    static class Auditoria {
        private final Processador p; private final JdbcTemplate jdbc; private final ObjectMapper om;
        Auditoria(Processador p, JdbcTemplate jdbc, ObjectMapper om) { this.p = p; this.jdbc = jdbc; this.om = om; }

        @RabbitListener(queues = Topologia.Q_AUDITORIA)
        public void on(@Payload Evento ev, Channel ch, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
            p.processar("auditoria", ev, ch, tag, e -> {
                try {
                    jdbc.update("INSERT INTO auditoria(event_id,tipo,ocorrido_em,payload) VALUES (?,?,?,?)",
                                e.eventId(), e.tipo(), Timestamp.from(e.ocorridoEm()), om.writeValueAsString(e));
                } catch (JsonProcessingException ex) { throw new RuntimeException(ex); }
            });
        }
    }

    @Component @Profile("estatisticas")
    static class Estatisticas {
        private final Processador p; private final JdbcTemplate jdbc;
        @Value("${demo.atraso-ms:0}") long atraso;
        Estatisticas(Processador p, JdbcTemplate jdbc) { this.p = p; this.jdbc = jdbc; }

        @RabbitListener(queues = Topologia.Q_ESTATISTICAS)
        public void on(@Payload Evento ev, Channel ch, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
            p.processar("estatisticas", ev, ch, tag, e -> {
                try { Thread.sleep(atraso); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                String cat = s(e, "categoria");
                jdbc.update("INSERT INTO estatisticas(categoria,total) VALUES (?,1) ON CONFLICT (categoria) DO UPDATE SET total = estatisticas.total + 1", cat);
                jdbc.update("INSERT INTO estatisticas_log(event_id,instancia,categoria) VALUES (?,?,?)", e.eventId(), p.instancia(), cat);
            });
        }
    }
}
