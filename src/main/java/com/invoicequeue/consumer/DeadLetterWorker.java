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

/**
 * DeadLetterWorker — listens to the Dead Letter Queue (DLQ).
 *
 * WHAT IS A DLQ?
 *   When a message is rejected with basicNack(requeue=false), RabbitMQ
 *   doesn't discard it. Instead, it routes it to the Dead Letter Exchange,
 *   which delivers it to this queue: invoice_dead_letter_queue.
 *
 *   A DLQ is your safety net. No job is silently lost — even permanently
 *   failed ones end up here where they can be:
 *     - Logged and alerted on (Slack, PagerDuty, email)
 *     - Inspected manually in the RabbitMQ Management UI
 *     - Replayed after fixing the root cause
 *     - Stored to a database for audit trail
 *
 * WHAT THIS WORKER DOES:
 *   In this demo it just logs the failed message clearly so you can see
 *   exactly what landed in the DLQ and why.
 *
 *   In production you would:
 *     - Write the failed job to a "failed_invoices" database table
 *     - Trigger a Slack/email/PagerDuty alert
 *     - Store enough context to manually re-trigger the job later
 */
@Slf4j
@Component
public class DeadLetterWorker {

    @RabbitListener(queues = "${rabbitmq.queue.dead-letter}")
    public void handleDeadLetter(
            InvoiceRequest request,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {

        log.error("");
        log.error("☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️");
        log.error("  DEAD LETTER QUEUE — Failed Invoice Job Received");
        log.error("☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️");
        log.error("  Invoice ID   : {}", request.getInvoiceId());
        log.error("  Customer     : {}", request.getCustomerName());
        log.error("  Email        : {}", request.getCustomerEmail());
        log.error("  Type         : {}", request.getInvoiceType());
        log.error("  Retry Count  : {}", request.getRetryCount());
        log.error("  Submitted At : {}", request.getSubmittedAt());
        log.error("  DLQ Time     : {}", LocalDateTime.now());
        log.error("");
        log.error("  ⚡ ACTION REQUIRED: This invoice was NOT generated.");
        log.error("  ⚡ In production: alert the ops team + save to failed_invoices table.");
        log.error("  ⚡ Inspect in UI: http://localhost:15672 → Queues → invoice_dead_letter_queue");
        log.error("☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️  ☠️");
        log.error("");

        // Acknowledge the DLQ message — we've "handled" it by logging/alerting.
        // In production you'd ack only after writing to the DB.
        channel.basicAck(deliveryTag, false);
    }
}
