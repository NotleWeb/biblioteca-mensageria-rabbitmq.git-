package com.biblioteca;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topologia reproduzivel: declarada por TODOS os processos (idempotente). Assim as filas
 * duraveis existem mesmo com um consumidor desligado e acumulam mensagens.
 */
@Configuration
public class Topologia {
    public static final String EXCHANGE = "biblioteca.eventos";
    public static final String DLX = "biblioteca.eventos.dlx";
    public static final String DLQ = "biblioteca.dlq";

    public static final String Q_CIRCULACAO = "biblioteca.circulacao";
    public static final String Q_COMUNICACAO = "biblioteca.comunicacao";
    public static final String Q_MULTAS = "biblioteca.multas";
    public static final String Q_AUDITORIA = "biblioteca.auditoria";
    public static final String Q_ESTATISTICAS = "biblioteca.estatisticas";

    @Bean
    MessageConverter jsonConverter(ObjectMapper mapper) {
        return new Jackson2JsonMessageConverter(mapper);
    }

    private static Queue fila(String nome) {
        return QueueBuilder.durable(nome).deadLetterExchange(DLX).build();
    }

    @Bean
    Declarables declaracoesRabbitMq() {
        TopicExchange ex = ExchangeBuilder.topicExchange(EXCHANGE).durable(true).build();
        FanoutExchange dlx = new FanoutExchange(DLX, true, false);
        Queue dlq = QueueBuilder.durable(DLQ).build();

        Queue circulacao = fila(Q_CIRCULACAO), comunicacao = fila(Q_COMUNICACAO), multas = fila(Q_MULTAS),
              auditoria = fila(Q_AUDITORIA), estatisticas = fila(Q_ESTATISTICAS);

        return new Declarables(ex, dlx, dlq, circulacao, comunicacao, multas, auditoria, estatisticas,
            BindingBuilder.bind(dlq).to(dlx),
            // Circulacao: somente emprestimo
            BindingBuilder.bind(circulacao).to(ex).with("emprestimo.registrado"),
            // Comunicacao: 3 eventos especificos (selecao por evento)
            BindingBuilder.bind(comunicacao).to(ex).with("emprestimo.registrado"),
            BindingBuilder.bind(comunicacao).to(ex).with("exemplar.devolvido"),
            BindingBuilder.bind(comunicacao).to(ex).with("multa.cancelada"),
            // Multas: toda a familia multa.* (inclui eventos futuros, ex.: multa.paga)
            BindingBuilder.bind(multas).to(ex).with("multa.*"),
            // Auditoria: tudo
            BindingBuilder.bind(auditoria).to(ex).with("#"),
            // Estatisticas: somente emprestimo (fila unica compartilhada por 2 instancias)
            BindingBuilder.bind(estatisticas).to(ex).with("emprestimo.registrado"));
    }
}
