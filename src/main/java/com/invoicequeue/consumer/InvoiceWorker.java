package com.invoicequeue.consumer;

import com.invoicequeue.model.InvoiceRequest;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * InvoiceWorker — the message consumer. The "worker" in our Work Queue pattern.
 *
 * HOW IT WORKS:
 *   @RabbitListener tells Spring to:
 *   1. Connect to RabbitMQ on startup
 *   2. Spawn listener threads (quantity = spring.rabbitmq.listener.simple.concurrency)
 *   3. Each thread continuously polls invoice_generation_queue
 *   4. When a message arrives, deserialize it and call processInvoice()
 *   5. Wait for basicAck before fetching the next message (because prefetch=1)
 *
 * SCENARIOS HANDLED:
 *   ✅ SUCCESS           — PDF rendered → basicAck → message removed from queue
 *   ⚠️  TRANSIENT FAILURE — temporary error → re-publish with incremented retryCount
 *   ❌ PERMANENT FAILURE  — poison message → basicNack(requeue=false) → goes to DLQ
 *   💀 WORKER CRASH       — app killed mid-job → no ack sent → RabbitMQ re-queues automatically
 *
 * ─────────────────────────────────────────────────────────────────────
 * WHY WE RE-PUBLISH INSTEAD OF basicNack(requeue=true) FOR RETRIES:
 * ─────────────────────────────────────────────────────────────────────
 * When basicNack(requeue=true) is called, RabbitMQ re-queues the ORIGINAL
 * message bytes — exactly as they arrived. Any changes made to the Java
 * object (like incrementing retryCount) are completely discarded.
 *
 * So the retry scenario was broken like this:
 *   Attempt 1 → retryCount=0 in JSON → worker sets retryCount=1 on Java object
 *               → basicNack(requeue=true) → RabbitMQ re-queues original bytes
 *   Attempt 2 → retryCount=0 in JSON again (original bytes!) → worker sees 0
 *               → thinks it's the first attempt → loops forever, never hits max
 *
 * The correct approach for tracked retries:
 *   1. basicAck the original message (remove it from the queue)
 *   2. Re-publish a NEW message with retryCount incremented in the JSON body
 *   3. On next delivery, the worker reads the correct retryCount from the JSON
 *
 * This way the retry counter is embedded in the message itself and survives
 * each round-trip through RabbitMQ correctly.
 * ─────────────────────────────────────────────────────────────────────
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InvoiceWorker {

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    // Injected so we can re-publish the updated message for transient retries
    private final RabbitTemplate rabbitTemplate;

    @Value("${rabbitmq.exchange.invoice}")
    private String invoiceExchange;

    @Value("${rabbitmq.routing.key.invoice}")
    private String invoiceRoutingKey;

    /**
     * Main message handler — called by Spring AMQP for each message.
     *
     * Parameters:
     *   InvoiceRequest request — Jackson deserializes the JSON body into this object
     *   Channel channel        — raw AMQP channel needed to send ack/nack manually
     *   long deliveryTag       — unique ID for this specific delivery (used in ack/nack)
     */
    @RabbitListener(queues = "${rabbitmq.queue.invoice}")
    public void processInvoice(
            InvoiceRequest request,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {

        String workerName = Thread.currentThread().getName();
        String startTime  = LocalDateTime.now().format(TIME_FMT);

        log.info("");
        log.info("╔══════════════════════════════════════════════════════╗");
        log.info("║  [{}] NEW JOB RECEIVED at {}  ║", workerName.substring(workerName.lastIndexOf('-') + 1), startTime);
        log.info("╠══════════════════════════════════════════════════════╣");
        log.info("║  Invoice ID  : {}", request.getInvoiceId());
        log.info("║  Customer    : {}", request.getCustomerName());
        log.info("║  Email       : {}", request.getCustomerEmail());
        log.info("║  Type        : {}", request.getInvoiceType());
        log.info("║  Amount      : {} {}", request.getCurrency(), request.getTotalAmount());
        log.info("║  Complexity  : '{}' ({} sec)", request.getComplexityDots(), request.getComplexityDots().length());
        log.info("║  Retry Count : {}", request.getRetryCount());
        log.info("╚══════════════════════════════════════════════════════╝");

        try {

            // ────────────────────────────────────────────────────────
            //  SCENARIO 1: SIMULATED PERMANENT FAILURE
            //  If invoiceId contains "FAIL", treat as a poison message.
            // ────────────────────────────────────────────────────────
            if (request.getInvoiceId().toUpperCase().contains("FAIL")) {
                throw new IllegalArgumentException(
                        "PERMANENT FAILURE: Invoice ID '" + request.getInvoiceId() +
                                "' is flagged as invalid. Routing to Dead Letter Queue."
                );
            }

            // ────────────────────────────────────────────────────────
            //  SCENARIO 2: SIMULATED TRANSIENT FAILURE
            //  If invoiceId contains "RETRY" and we haven't exceeded max attempts,
            //  simulate a temporary failure (e.g. database timeout).
            // ────────────────────────────────────────────────────────
            if (request.getInvoiceId().toUpperCase().contains("RETRY")
                    && request.getRetryCount() < MAX_RETRY_ATTEMPTS) {

                int nextRetryCount = request.getRetryCount() + 1;

                log.warn("");
                log.warn("⚠️  ⚠️  ⚠️  TRANSIENT FAILURE — attempt {}/{} for {}",
                        nextRetryCount, MAX_RETRY_ATTEMPTS, request.getInvoiceId());
                log.warn("   Reason: Simulating DB timeout");
                log.warn("   Strategy: ACK original → re-publish with retryCount={}", nextRetryCount);
                log.warn("");

                // ── Step 1: ACK the original message ─────────────────
                // Remove the original (retryCount=N) message from the queue.
                // We are NOT using basicNack(requeue=true) because that
                // re-queues the original bytes — our retryCount change is lost.
                channel.basicAck(deliveryTag, false);

                // ── Step 2: Re-publish with incremented retryCount ────
                // Build a new InvoiceRequest with retryCount incremented,
                // then publish it as a brand-new message. RabbitMQ stores
                // the updated JSON, so the next worker reads the correct count.
                InvoiceRequest retryRequest = InvoiceRequest.builder()
                        .invoiceId(request.getInvoiceId())
                        .customerName(request.getCustomerName())
                        .customerEmail(request.getCustomerEmail())
                        .invoiceType(request.getInvoiceType())
                        .complexityDots(request.getComplexityDots())
                        .totalAmount(request.getTotalAmount())
                        .currency(request.getCurrency())
                        .submittedAt(request.getSubmittedAt())
                        .retryCount(nextRetryCount)   // ← incremented value in the JSON
                        .build();

                rabbitTemplate.convertAndSend(invoiceExchange, invoiceRoutingKey, retryRequest);

                log.warn("   ✉️  Re-published {} with retryCount={} — will be picked up shortly.",
                        request.getInvoiceId(), nextRetryCount);
                return; // exit — do not fall through to the success ack below
            }

            // ────────────────────────────────────────────────────────
            //  SCENARIO 3: NORMAL SUCCESS PATH (includes RETRY after max attempts)
            //  If retryCount >= MAX_RETRY_ATTEMPTS, we stop retrying and
            //  process normally — the "fault" clears after enough attempts.
            // ────────────────────────────────────────────────────────
            if (request.getInvoiceId().toUpperCase().contains("RETRY")
                    && request.getRetryCount() >= MAX_RETRY_ATTEMPTS) {
                log.info("✅ RETRY RESOLVED — {} succeeded after {} attempt(s). Processing now.",
                        request.getInvoiceId(), request.getRetryCount());
            }

            generateInvoicePdf(request, workerName);

            // ── SUCCESS ACK ──────────────────────────────────────────
            // Tell RabbitMQ: "I finished this job. Remove it from the queue."
            // Only reached if generateInvoicePdf() completed without throwing.
            // If the worker is killed before this line, no ack is sent and
            // RabbitMQ automatically re-queues the message.
            channel.basicAck(deliveryTag, false);

            log.info("");
            log.info("✅ ✅ ✅  JOB COMPLETE — {} acknowledged and removed from queue",
                    request.getInvoiceId());
            log.info("   Customer {} will receive their PDF at {}",
                    request.getCustomerName(), request.getCustomerEmail());
            log.info("");

        } catch (IllegalArgumentException e) {
            // ────────────────────────────────────────────────────────
            //  PERMANENT FAILURE → DEAD LETTER QUEUE
            //  Bad data that no amount of retrying will fix.
            //  basicNack(requeue=false) → Dead Letter Exchange → DLQ
            // ────────────────────────────────────────────────────────
            log.error("");
            log.error("❌ ❌ ❌  PERMANENT FAILURE for {} — routing to Dead Letter Queue",
                    request.getInvoiceId());
            log.error("   Reason: {}", e.getMessage());
            log.error("   This message will NOT be retried.");
            log.error("");

            channel.basicNack(deliveryTag, false, false);

        } catch (InterruptedException e) {
            // ────────────────────────────────────────────────────────
            //  WORKER INTERRUPTED during Thread.sleep (JVM shutdown)
            //  Restore the interrupt flag and nack so it is re-queued.
            // ────────────────────────────────────────────────────────
            Thread.currentThread().interrupt();
            log.error("⚠️  Worker interrupted while processing {}. Re-queuing.",
                    request.getInvoiceId());
            channel.basicNack(deliveryTag, false, true);
        }
    }

    /**
     * Simulates PDF generation work.
     * Each '.' in complexityDots = 1 second of rendering.
     */
    private void generateInvoicePdf(InvoiceRequest request, String workerName)
            throws InterruptedException {

        String dots = request.getComplexityDots();
        int totalSeconds = (int) dots.chars().filter(c -> c == '.').count();

        log.info("🔄 [{}] Starting PDF render for {} — estimated {} second(s)",
                workerName.substring(workerName.lastIndexOf('-') + 1),
                request.getInvoiceId(),
                totalSeconds);

        for (int i = 1; i <= totalSeconds; i++) {
            Thread.sleep(1000);
            log.info("   ⏳ [{}] Rendering {}: {}/{} sec  |  Step: {}",
                    workerName.substring(workerName.lastIndexOf('-') + 1),
                    request.getInvoiceId(),
                    i,
                    totalSeconds,
                    getRenderingStep(i, totalSeconds));
        }

        log.info("   📄 [{}] PDF rendered for {} — {} {}",
                workerName.substring(workerName.lastIndexOf('-') + 1),
                request.getCustomerName(),
                request.getCurrency(),
                request.getTotalAmount());
    }

    private String getRenderingStep(int current, int total) {
        if (total == 1) return "Generating single-page receipt";
        double progress = (double) current / total;
        if (progress <= 0.25) return "Fetching customer & order data";
        if (progress <= 0.50) return "Rendering line items and tax breakdown";
        if (progress <= 0.75) return "Applying watermark and company branding";
        return "Uploading to storage & preparing email";
    }
}