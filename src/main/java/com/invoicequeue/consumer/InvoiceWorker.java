package com.invoicequeue.consumer;

import com.invoicequeue.model.InvoiceRequest;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
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
 *   ⚠️  TRANSIENT FAILURE — temporary error (e.g. DB down) → basicNack(requeue=true) → retried
 *   ❌ PERMANENT FAILURE  — poison message (e.g. invalid data) → basicNack(requeue=false) → goes to DLQ
 *   💀 WORKER CRASH       — app killed mid-job → no ack sent → RabbitMQ re-queues automatically
 *
 * FAIR DISPATCH:
 *   prefetch=1 (set in application.properties) means this worker will NOT
 *   receive a second message until it calls basicAck on the current one.
 *   This prevents a slow worker from hoarding messages while a fast worker sits idle.
 *
 * COMPETING CONSUMERS:
 *   Multiple threads (or multiple app instances) all call @RabbitListener on
 *   the same queue. RabbitMQ guarantees each message goes to exactly ONE worker.
 */
@Slf4j
@Component
public class InvoiceWorker {

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Main message handler — called by Spring AMQP for each message.
     *
     * Parameters explained:
     *   InvoiceRequest request — Jackson auto-deserializes the JSON body into this object
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

            // --------------------------------------------------------
            //  SCENARIO 1: SIMULATED PERMANENT FAILURE
            //  If invoiceId contains "FAIL", treat it as a poison message.
            //  This tests the DLQ path.
            // --------------------------------------------------------
            if (request.getInvoiceId().toUpperCase().contains("FAIL")) {
                throw new IllegalArgumentException(
                    "PERMANENT FAILURE: Invoice ID '" + request.getInvoiceId() +
                    "' is flagged as invalid. Routing to Dead Letter Queue."
                );
            }

            // --------------------------------------------------------
            //  SCENARIO 2: SIMULATED TRANSIENT FAILURE
            //  If invoiceId contains "RETRY" and we haven't retried 3x yet,
            //  simulate a temporary failure (e.g. database timeout).
            // --------------------------------------------------------
            if (request.getInvoiceId().toUpperCase().contains("RETRY")
                    && request.getRetryCount() < MAX_RETRY_ATTEMPTS) {
                request.setRetryCount(request.getRetryCount() + 1);
                throw new RuntimeException(
                    "TRANSIENT FAILURE (attempt " + request.getRetryCount() + "/" + MAX_RETRY_ATTEMPTS +
                    "): Simulating DB timeout for " + request.getInvoiceId()
                );
            }

            // --------------------------------------------------------
            //  SCENARIO 3: NORMAL SUCCESS PATH
            //  Simulate actual PDF generation work
            // --------------------------------------------------------
            generateInvoicePdf(request, workerName);

            // --------------------------------------------------------
            //  SUCCESS ACK
            //  This tells RabbitMQ: "I finished this job. Remove it from the queue."
            //
            //  basicAck(deliveryTag, multiple):
            //    deliveryTag — which message we're acknowledging
            //    multiple=false — only ack THIS message, not all pending ones
            //
            //  IMPORTANT: This only runs if generateInvoicePdf() completes
            //  without throwing. If the worker is killed (Ctrl+C) before this
            //  line, RabbitMQ never receives the ack and re-queues the message.
            // --------------------------------------------------------
            channel.basicAck(deliveryTag, false);

            log.info("");
            log.info("✅ ✅ ✅  JOB COMPLETE — {} acknowledged and removed from queue", request.getInvoiceId());
            log.info("   Customer {} will receive their PDF at {}", request.getCustomerName(), request.getCustomerEmail());
            log.info("");

        } catch (IllegalArgumentException e) {
            // --------------------------------------------------------
            //  PERMANENT FAILURE → DEAD LETTER QUEUE
            //  This message has bad data that no amount of retrying will fix.
            //  basicNack with requeue=false sends it to the DLQ.
            // --------------------------------------------------------
            log.error("");
            log.error("❌ ❌ ❌  PERMANENT FAILURE for {} — routing to Dead Letter Queue", request.getInvoiceId());
            log.error("   Reason: {}", e.getMessage());
            log.error("   This message will NOT be retried. Check DLQ: invoice_dead_letter_queue");
            log.error("");

            // requeue=false → goes to Dead Letter Exchange → lands in DLQ
            channel.basicNack(deliveryTag, false, false);

        } catch (RuntimeException e) {
            // --------------------------------------------------------
            //  TRANSIENT FAILURE → RE-QUEUE FOR RETRY
            //  Something went wrong but it might succeed on a retry
            //  (e.g., DB was temporarily down, external service timed out).
            //  basicNack with requeue=true puts it back in the queue.
            // --------------------------------------------------------
            log.warn("");
            log.warn("⚠️  ⚠️  ⚠️   TRANSIENT FAILURE for {} (retry {})", request.getInvoiceId(), request.getRetryCount());
            log.warn("   Reason: {}", e.getMessage());
            log.warn("   Message will be RE-QUEUED for another worker to attempt.");
            log.warn("");

            // requeue=true → message goes back to invoice_generation_queue
            channel.basicNack(deliveryTag, false, true);

        } catch (InterruptedException e) {
            // --------------------------------------------------------
            //  WORKER INTERRUPTED (e.g. JVM shutdown during Thread.sleep)
            //  Restore the interrupt flag and nack so the message is re-queued.
            // --------------------------------------------------------
            Thread.currentThread().interrupt();
            log.error("⚠️  Worker thread interrupted while processing {}. Re-queuing.", request.getInvoiceId());
            channel.basicNack(deliveryTag, false, true);
        }
    }

    /**
     * Simulates PDF generation work.
     *
     * Each '.' in complexityDots = 1 second of rendering.
     * Logs progress second-by-second so you can see Fair Dispatch in action
     * in real time across multiple worker threads.
     *
     * Real-world equivalent of this method:
     *   - Fetch customer data from DB
     *   - Fetch line items from order service
     *   - Render PDF using iText or Apache PDFBox
     *   - Apply company watermark and digital signature
     *   - Upload PDF to S3 / Azure Blob Storage
     *   - Send email with PDF link via SendGrid / SES
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

    /** Returns a human-readable description of what the PDF renderer is doing */
    private String getRenderingStep(int current, int total) {
        if (total == 1) return "Generating single-page receipt";
        double progress = (double) current / total;
        if (progress <= 0.25) return "Fetching customer & order data";
        if (progress <= 0.50) return "Rendering line items and tax breakdown";
        if (progress <= 0.75) return "Applying watermark and company branding";
        return "Uploading to storage & preparing email";
    }
}
