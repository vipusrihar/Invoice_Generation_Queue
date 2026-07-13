package com.invoicequeue.producer;

import com.invoicequeue.model.InvoiceRequest;
import com.invoicequeue.model.InvoiceResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * InvoiceController — the REST API layer (entry point for all clients).
 *
 * ENDPOINTS:
 *   POST /api/invoices/submit         — submit a single custom invoice job
 *   POST /api/invoices/submit-batch   — submit 5 pre-built demo jobs
 *   GET  /api/invoices/health         — check if the API is responsive
 *
 * KEY DESIGN DECISION — 202 vs 200:
 *   We return 202 Accepted, NOT 200 OK.
 *   200 OK means "here is your result."
 *   202 Accepted means "I got your request, work is starting in the background."
 *   This is the correct HTTP contract for any asynchronous operation.
 *
 * SUCCESS SCENARIO:
 *   RabbitMQ is running → job published → 202 returned within ~5ms
 *   regardless of whether the job takes 1 second or 5 minutes to complete.
 *
 * FAILURE SCENARIO — RabbitMQ is DOWN:
 *   InvoiceProducer throws RuntimeException → caught here → 503 Service Unavailable
 *   The client knows the job was NOT queued and should retry.
 *
 * FAILURE SCENARIO — Invalid request body:
 *   Missing required fields → 400 Bad Request with an explanatory message.
 */
@Slf4j
@RestController
@RequestMapping("/api/invoices")
@RequiredArgsConstructor
public class InvoiceController {

    private final InvoiceProducer invoiceProducer;

    // ================================================================
    //  POST /api/invoices/submit
    //  Submit a single invoice generation job
    // ================================================================
    @PostMapping("/submit")
    public ResponseEntity<?> submitInvoice(@RequestBody InvoiceRequest request) {

        // Basic validation — in production, use @Valid + Bean Validation
        if (request.getInvoiceId() == null || request.getInvoiceId().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "REJECTED",
                    "reason", "invoiceId is required and cannot be blank"
            ));
        }
        if (request.getCustomerName() == null || request.getCustomerName().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "REJECTED",
                    "reason", "customerName is required and cannot be blank"
            ));
        }
        if (request.getComplexityDots() == null || request.getComplexityDots().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "REJECTED",
                    "reason", "complexityDots is required (e.g. '...' for 3 seconds of processing)"
            ));
        }

        request.setSubmittedAt(LocalDateTime.now());

        try {
            invoiceProducer.publishInvoiceJob(request);

            int estimatedSeconds = (int) request.getComplexityDots()
                    .chars().filter(c -> c == '.').count();

            InvoiceResponse response = InvoiceResponse.builder()
                    .invoiceId(request.getInvoiceId())
                    .status("ACCEPTED")
                    .message("Invoice job successfully queued. A worker will process it shortly.")
                    .acceptedAt(LocalDateTime.now())
                    .estimatedProcessingTime(estimatedSeconds + " second(s)")
                    .build();

            // 202 Accepted: job received and queued, processing is in progress
            return ResponseEntity.accepted().body(response);

        } catch (RuntimeException e) {
            // FAILURE SCENARIO: RabbitMQ is unreachable
            log.error("[CONTROLLER] ❌ Failed to queue job {} — {}", request.getInvoiceId(), e.getMessage());

            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                    "status", "FAILED",
                    "invoiceId", request.getInvoiceId(),
                    "reason", "Message broker is currently unavailable. Please retry in a moment.",
                    "timestamp", LocalDateTime.now().toString()
            ));
        }
    }

    // ================================================================
    //  POST /api/invoices/submit-batch
    //  Submit 5 pre-configured demo invoice jobs in one call.
    //  Useful for testing Fair Dispatch and Competing Consumers.
    // ================================================================
    @PostMapping("/submit-batch")
    public ResponseEntity<Map<String, Object>> submitBatch() {

        List<InvoiceRequest> batch = List.of(

            InvoiceRequest.builder()
                .invoiceId("INV-2024-1001")
                .customerName("Lakehouse Retail Ltd")
                .customerEmail("billing@lakehouse.com")
                .invoiceType(InvoiceRequest.InvoiceType.SIMPLE_RECEIPT)
                .complexityDots(".")                  // 1 second
                .totalAmount(250.00)
                .currency("USD")
                .submittedAt(LocalDateTime.now())
                .build(),

            InvoiceRequest.builder()
                .invoiceId("INV-2024-1002")
                .customerName("BluePeak Exports")
                .customerEmail("accounts@bluepeak.com")
                .invoiceType(InvoiceRequest.InvoiceType.ITEMIZED_WITH_TAX)
                .complexityDots("...")                 // 3 seconds
                .totalAmount(18750.50)
                .currency("USD")
                .submittedAt(LocalDateTime.now())
                .build(),

            InvoiceRequest.builder()
                .invoiceId("INV-2024-1003")
                .customerName("Fernridge Catering Co.")
                .customerEmail("finance@fernridge.com")
                .invoiceType(InvoiceRequest.InvoiceType.SIMPLE_RECEIPT)
                .complexityDots(".")                  // 1 second
                .totalAmount(3400.00)
                .currency("LKR")
                .submittedAt(LocalDateTime.now())
                .build(),

            InvoiceRequest.builder()
                .invoiceId("INV-2024-1004")
                .customerName("Montara Global Finance")
                .customerEmail("invoices@montara.com")
                .invoiceType(InvoiceRequest.InvoiceType.ANNUAL_REPORT_INVOICE)
                .complexityDots(".....")               // 5 seconds
                .totalAmount(475000.00)
                .currency("EUR")
                .submittedAt(LocalDateTime.now())
                .build(),

            InvoiceRequest.builder()
                .invoiceId("INV-2024-1005")
                .customerName("Coastline Marketing Agency")
                .customerEmail("billing@coastline.com")
                .invoiceType(InvoiceRequest.InvoiceType.STANDARD_INVOICE)
                .complexityDots("..")                  // 2 seconds
                .totalAmount(9200.00)
                .currency("USD")
                .submittedAt(LocalDateTime.now())
                .build()
        );

        int queued = 0;
        int failed = 0;

        for (InvoiceRequest req : batch) {
            try {
                invoiceProducer.publishInvoiceJob(req);
                queued++;
            } catch (RuntimeException e) {
                log.error("[CONTROLLER] Batch: failed to queue {}", req.getInvoiceId());
                failed++;
            }
        }

        return ResponseEntity.accepted().body(Map.of(
                "status", failed == 0 ? "ALL_ACCEPTED" : "PARTIAL_FAILURE",
                "totalSubmitted", batch.size(),
                "jobsQueued", queued,
                "jobsFailed", failed,
                "message", queued + " invoice job(s) queued. Watch worker logs for processing details."
        ));
    }

    // ================================================================
    //  GET /api/invoices/health
    //  Quick sanity check — confirms the REST layer is up
    // ================================================================
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Invoice Queue API",
                "timestamp", LocalDateTime.now().toString()
        ));
    }
}
