package com.invoicequeue.model;

import lombok.*;

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
 *
 * WHY @Getter/@Setter instead of @Data?
 *   @Data generates equals/hashCode based on all fields — which can cause subtle
 *   bugs when objects are used in Sets or as Map keys. We only need getters,
 *   setters, and constructors here, so we're explicit about what we want.
 *
 * IMPORTANT — retryCount field:
 *   We do NOT use @Builder.Default here. Instead we initialise retryCount
 *   inside the no-args constructor explicitly. This is because:
 *
 *   @Builder.Default requires @Builder to be present, and when Jackson
 *   deserializes the JSON from RabbitMQ it uses the NO-ARGS constructor —
 *   not the builder. So @Builder.Default has zero effect on deserialization.
 *
 *   The correct fix is to set the default in the no-args constructor directly,
 *   so Jackson always gets retryCount = 0 on a fresh deserialization.
 *
 *   The worker then increments it on the Java object and re-publishes the
 *   updated message back to the queue — so RabbitMQ's copy also has the
 *   incremented count on the next delivery.
 */
@Getter
@Setter
@Builder
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

    /**
     * How many times this message has been retried.
     *
     * DEFAULT = 0 set in the no-args constructor below.
     *
     * DO NOT use @Builder.Default here. Jackson deserializes via the
     * no-args constructor — @Builder.Default only affects the builder path
     * and is completely ignored during JSON deserialization from RabbitMQ.
     * Setting it here in the no-args constructor is the correct approach.
     */
    private int retryCount;

    /**
     * No-args constructor used by Jackson when deserializing messages
     * from RabbitMQ. retryCount is explicitly set to 0 here so every
     * fresh deserialization starts from zero correctly.
     *
     * NOTE: We cannot use @NoArgsConstructor alongside @Builder when we
     * need to set field defaults, because Lombok's generated no-args
     * constructor does NOT apply @Builder.Default values. Writing this
     * constructor manually gives us full control.
     */
    public InvoiceRequest() {
        this.retryCount = 0;
    }

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