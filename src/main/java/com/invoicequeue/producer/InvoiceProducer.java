package com.invoicequeue.producer;

import com.invoicequeue.model.InvoiceRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * InvoiceProducer — responsible for publishing invoice jobs to RabbitMQ.
 *
 * ROLE IN THE SYSTEM:
 *   This is the "sender" side. It knows nothing about who will process
 *   the message or when. It just hands the job to RabbitMQ and trusts
 *   that a worker will pick it up.
 *
 * SPRING AMQP vs RAW CLIENT:
 *   Raw amqp-client required: ConnectionFactory → Connection → Channel →
 *   channel.basicPublish(exchange, routingKey, props, body.getBytes())
 *
 *   Spring AMQP replaces all of that with:
 *   rabbitTemplate.convertAndSend(exchange, routingKey, object)
 *   Jackson handles serialization. Connection pooling is automatic.
 *
 * SUCCESS SCENARIO:
 *   RabbitMQ is running → message published → logged → returns normally.
 *
 * FAILURE SCENARIO:
 *   RabbitMQ is DOWN → AmqpException is thrown → caught here → wrapped
 *   in RuntimeException so the REST controller can return a 503.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InvoiceProducer {

    private final RabbitTemplate rabbitTemplate;

    @Value("${rabbitmq.exchange.invoice}")
    private String invoiceExchange;

    @Value("${rabbitmq.routing.key.invoice}")
    private String invoiceRoutingKey;

    /**
     * Publishes a single invoice job to the RabbitMQ exchange.
     *
     * What convertAndSend does internally:
     *   1. Jackson serializes InvoiceRequest → JSON bytes
     *   2. Sets Content-Type header: application/json
     *   3. Sets delivery mode: PERSISTENT (survives broker restart)
     *   4. Publishes to invoiceExchange with routing key "invoice.generate"
     *   5. RabbitMQ routes it to invoice_generation_queue
     *   6. Returns immediately — does NOT wait for a worker to process it
     *
     * @param request the invoice job to queue
     * @throws RuntimeException if RabbitMQ is unreachable
     */
    public void publishInvoiceJob(InvoiceRequest request) {
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("[PRODUCER] 📤 Publishing invoice job to RabbitMQ...");
        log.info("[PRODUCER] Invoice ID   : {}", request.getInvoiceId());
        log.info("[PRODUCER] Customer     : {}", request.getCustomerName());
        log.info("[PRODUCER] Type         : {}", request.getInvoiceType());
        log.info("[PRODUCER] Complexity   : '{}' ({} seconds estimated)",
                request.getComplexityDots(),
                request.getComplexityDots().replace(".", "").length() == 0
                        ? request.getComplexityDots().length()
                        : request.getComplexityDots().length());
        log.info("[PRODUCER] Amount       : {} {}", request.getCurrency(), request.getTotalAmount());

        try {
            // This is where the magic happens — serialize + publish in one call
            rabbitTemplate.convertAndSend(invoiceExchange, invoiceRoutingKey, request);

            log.info("[PRODUCER] ✅ SUCCESS — Job queued. Exchange: '{}', Key: '{}'",
                    invoiceExchange, invoiceRoutingKey);
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        } catch (AmqpException e) {
            // FAILURE SCENARIO: RabbitMQ is down or connection refused
            log.error("[PRODUCER] ❌ FAILED — Could not publish {} to RabbitMQ", request.getInvoiceId());
            log.error("[PRODUCER] Reason: {}", e.getMessage());
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            // Re-throw so the REST controller can return an appropriate HTTP error
            throw new RuntimeException(
                    "Failed to queue invoice job " + request.getInvoiceId() +
                    ". RabbitMQ may be unavailable.", e);
        }
    }
}
