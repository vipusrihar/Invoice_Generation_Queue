package com.invoicequeue.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * InvoiceRequest — the message payload that travels through the RabbitMQ queue.
 *
 * WHY Serializable?
 *   This object is converted to JSON by Jackson2JsonMessageConverter before
 *   being published to RabbitMQ. Serializable is also kept as a safety net
 *   for any fallback serialization.
 *
 * ANALOGY:
 *   Think of this as the @RequestBody of an async operation.
 *   In a REST call, you send JSON in the HTTP body.
 *   In RabbitMQ, you send JSON as a message body. Same idea, different channel.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InvoiceRequest implements Serializable {

    /** Unique invoice ID — e.g. INV-2024-1001 */
    private String invoiceId;

    /** Full name of the customer */
    private String customerName;

    /** Customer's email — in a real app we'd email the finished PDF here */
    private String customerEmail;

    /**
     * Invoice type controls rendering complexity.
     * Each dot '.' in complexityDots represents 1 second of PDF work.
     */
    private InvoiceType invoiceType;

    /**
     * Dots simulate rendering time: "." = 1s, "....." = 5s.
     * In a real system this would be page count, image count, etc.
     */
    private String complexityDots;

    /** Total invoice amount (shown in the generated PDF) */
    private double totalAmount;

    /** ISO currency code — e.g. USD, EUR, LKR */
    private String currency;

    /** When the REST API accepted this job — set by the controller */
    private LocalDateTime submittedAt;

    /** How many times this message has been retried (incremented on failure) */
    @Builder.Default
    private int retryCount = 0;

    // ------------------------------------------------------------------
    //  Invoice Types
    //  Each type maps to a real-world scenario with different complexity
    // ------------------------------------------------------------------
    public enum InvoiceType {
        SIMPLE_RECEIPT,         // Single item, no tax — 1 second
        STANDARD_INVOICE,       // Multi-item with tax breakdown — 2 seconds
        ITEMIZED_WITH_TAX,      // Detailed breakdown + currency conversion — 3 seconds
        ANNUAL_REPORT_INVOICE   // Charts, logos, multi-page watermark — 5 seconds
    }
}
