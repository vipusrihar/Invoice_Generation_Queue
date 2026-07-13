package com.invoicequeue.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQConfig — declares every RabbitMQ resource the app needs.
 *
 * Spring Boot reads these @Bean definitions on startup and automatically
 * creates the queues, exchanges, and bindings in RabbitMQ if they don't
 * already exist. You never need to log in to the RabbitMQ UI to create them.
 *
 * WHAT IS DECLARED HERE:
 *  1. Main Queue          — where invoice jobs wait for a worker
 *  2. Dead Letter Queue   — where failed jobs go after max retries
 *  3. Main Exchange       — routes messages to the main queue
 *  4. Dead Letter Exchange— routes failed messages to the DLQ
 *  5. Bindings            — connects queues to exchanges via routing keys
 *  6. JSON Message Converter — auto-converts InvoiceRequest ↔ JSON
 *  7. RabbitTemplate      — the tool used to SEND messages from Java code
 */
@Configuration
public class RabbitMQConfig {

    @Value("${rabbitmq.queue.invoice}")
    private String invoiceQueue;

    @Value("${rabbitmq.queue.dead-letter}")
    private String deadLetterQueue;

    @Value("${rabbitmq.exchange.invoice}")
    private String invoiceExchange;

    @Value("${rabbitmq.exchange.dead-letter}")
    private String deadLetterExchange;

    @Value("${rabbitmq.routing.key.invoice}")
    private String invoiceRoutingKey;

    @Value("${rabbitmq.routing.key.dead-letter}")
    private String deadLetterRoutingKey;

    // =========================================================
    //  DEAD LETTER EXCHANGE
    //  Must be declared FIRST because the main queue references it
    // =========================================================

    /**
     * Dead Letter Exchange (DLE):
     * When a message is rejected (basicNack with requeue=false) after
     * max retry attempts, RabbitMQ forwards it here instead of dropping it.
     *
     * Think of it as a "failed jobs" inbox — you can inspect these later,
     * alert your team, or replay them manually once the root cause is fixed.
     */
    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(deadLetterExchange);
    }

    /**
     * Dead Letter Queue (DLQ):
     * Durable queue that permanently stores failed messages.
     * Messages here are safe from broker restarts and won't be lost.
     */
    @Bean
    public Queue deadLetterQueueBean() {
        return QueueBuilder
                .durable(deadLetterQueue)
                .build();
    }

    /**
     * DLQ Binding: links the dead letter queue to the dead letter exchange.
     */
    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder
                .bind(deadLetterQueueBean())
                .to(deadLetterExchange())
                .with(deadLetterRoutingKey);
    }

    // =========================================================
    //  MAIN INVOICE EXCHANGE
    // =========================================================

    /**
     * Direct Exchange:
     * Routes messages to a queue based on an exact routing key match.
     * Message with key "invoice.generate" → goes to invoice_generation_queue.
     *
     * Other exchange types exist (Topic, Fanout, Headers) but Direct is
     * the right fit for a Work Queue — one routing key, one destination queue.
     */
    @Bean
    public DirectExchange invoiceExchange() {
        return new DirectExchange(invoiceExchange);
    }

    // =========================================================
    //  MAIN INVOICE QUEUE
    // =========================================================

    /**
     * Main Invoice Queue:
     *
     * durable(true)    → survives RabbitMQ restart (written to disk)
     * withArgument DLX → on rejection, route to dead letter exchange
     * withArgument DLK → with this routing key so the DLQ binding picks it up
     * withArgument TTL → if a message sits unprocessed for 30min, auto-expire it to DLQ
     *
     * This is a "production-grade" queue configuration.
     * A basic queue would just be QueueBuilder.durable(name).build().
     */
    @Bean
    public Queue invoiceQueueBean() {
        Map<String, Object> args = new HashMap<>();

        // Where rejected messages go
        args.put("x-dead-letter-exchange", deadLetterExchange);
        args.put("x-dead-letter-routing-key", deadLetterRoutingKey);

        // Message TTL: auto-expire after 30 minutes if nobody processes it
        args.put("x-message-ttl", 1800000);

        return QueueBuilder
                .durable(invoiceQueue)
                .withArguments(args)
                .build();
    }

    /**
     * Main Binding:
     * Messages published to invoiceExchange with key "invoice.generate"
     * are delivered to invoice_generation_queue.
     */
    @Bean
    public Binding invoiceBinding() {
        return BindingBuilder
                .bind(invoiceQueueBean())
                .to(invoiceExchange())
                .with(invoiceRoutingKey);
    }

    // =========================================================
    //  MESSAGE CONVERTER
    // =========================================================

    /**
     * Jackson2JsonMessageConverter:
     * Automatically serializes InvoiceRequest → JSON when publishing,
     * and deserializes JSON → InvoiceRequest when consuming.
     *
     * Without this, Spring AMQP uses Java binary serialization — which
     * is brittle, unreadable in the RabbitMQ UI, and breaks if you change
     * class names or field names without versioning.
     *
     * With this, messages look like:
     * {"invoiceId":"INV-2024-1001","customerName":"Lakehouse Retail",...}
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // =========================================================
    //  RABBIT TEMPLATE
    // =========================================================

    /**
     * RabbitTemplate:
     * The Spring way to SEND messages programmatically.
     * Inject this into any class that needs to publish.
     *
     * Key methods:
     *   convertAndSend(exchange, routingKey, object) — serialize + publish
     *   receiveAndConvert(queueName) — manual pull (we use @RabbitListener instead)
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter());
        return template;
    }
}
