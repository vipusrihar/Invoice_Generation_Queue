package com.invoicequeue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * InvoiceQueueApplication — Spring Boot entry point.
 *
 * STARTUP SEQUENCE:
 *   1. Spring connects to RabbitMQ at localhost:5672
 *   2. RabbitMQConfig @Beans declare queue, exchange, binding, DLQ → created in RabbitMQ
 *   3. InvoiceWorker starts 2 listener threads on invoice_generation_queue
 *   4. DeadLetterWorker starts 1 listener thread on invoice_dead_letter_queue
 *   5. InvoiceController exposes REST endpoints on port 8080
 *   6. Actuator exposes health check at /actuator/health
 *
 * WHAT TO WATCH ON STARTUP:
 *   Look for these log lines to confirm everything connected:
 *   - "Started InvoiceQueueApplication in X seconds"
 *   - "SimpleMessageListenerContainer: started" (means workers are listening)
 *   - No "Connection refused" errors (means RabbitMQ is reachable)
 */
@Slf4j
@SpringBootApplication
public class InvoiceQueueApplication {

    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(InvoiceQueueApplication.class, args);

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║         Invoice Worker Queue — Application Started           ║");
        log.info("╠══════════════════════════════════════════════════════════════╣");
        log.info("║  REST API       →  http://localhost:8080/api/invoices        ║");
        log.info("║  Health Check   →  http://localhost:8080/actuator/health     ║");
        log.info("║  RabbitMQ UI    →  http://localhost:15672  (guest/guest)     ║");
        log.info("╠══════════════════════════════════════════════════════════════╣");
        log.info("║  ENDPOINTS:                                                  ║");
        log.info("║  POST /api/invoices/submit        → Single job               ║");
        log.info("║  POST /api/invoices/submit-batch  → 5 demo jobs              ║");
        log.info("║  GET  /api/invoices/health        → API health               ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");
        log.info("");
    }
}
