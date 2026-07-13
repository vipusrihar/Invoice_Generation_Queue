package com.invoicequeue.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * InvoiceResponse — what the REST API returns to the caller.
 *
 * This is returned IMMEDIATELY (within milliseconds) when a job is queued —
 * long before the actual PDF is generated. The caller gets status=ACCEPTED
 * and a message saying "we're on it."
 *
 * In a real system, you'd add a status polling endpoint:
 * GET /api/invoices/{invoiceId}/status → QUEUED | PROCESSING | DONE | FAILED
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InvoiceResponse {

    private String invoiceId;
    private String status;           // ACCEPTED, REJECTED
    private String message;
    private LocalDateTime acceptedAt;
    private String estimatedProcessingTime;
}
