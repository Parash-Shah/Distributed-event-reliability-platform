package com.paras.eventplatform;

import com.paras.eventplatform.dlq.DeadLetterQueue;
import com.paras.eventplatform.event.EventIngestionService;
import com.paras.eventplatform.event.EventRepository;
import com.paras.eventplatform.event.EventRequest;
import com.paras.eventplatform.event.EventResponse;
import com.paras.eventplatform.event.EventStatus;
import com.paras.eventplatform.queue.EventQueue;
import com.paras.eventplatform.reconciliation.ReconciliationJob;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
@SpringBootTest(properties = {
        "platform.retry.initial-backoff-ms=10",
        "platform.retry.max-attempts=3",
        "platform.reconciliation.stale-after=25ms",
        "platform.reconciliation.interval-ms=60000"
})
class EventPlatformIntegrationTest {
    @Autowired EventIngestionService ingestion;
    @Autowired EventRepository repository;
    @Autowired EventQueue queue;
    @Autowired DeadLetterQueue dlq;
    @Autowired ReconciliationJob reconciliation;
    @Autowired MockMvc mockMvc;

    @BeforeEach
    void resetState() {
        queue.clear();
        repository.clear();
        dlq.clear();
    }

    @Test
    void processesAnEventExactlyOnceWhenSubmittedTwice() {
        EventRequest request = new EventRequest(null, "ORDER_CREATED", Map.of("orderId", "123"));

        EventResponse first = ingestion.ingest(request, "event-123", "correlation-123");
        EventResponse duplicate = ingestion.ingest(request, "event-123", "correlation-123");

        assertThat(first.duplicate()).isFalse();
        assertThat(duplicate.duplicate()).isTrue();
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(repository.find("event-123").orElseThrow().status()).isEqualTo(EventStatus.PROCESSED));
        assertThat(repository.findAll()).hasSize(1);
    }

    @Test
    void eventApiReturnsAcceptedThenDuplicateForSameIdempotencyKey() throws Exception {
        String body = """
                {"event_type":"ORDER_CREATED","payload":{"order_id":"api-1"}}
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "api-event-1")
                        .header("X-Correlation-Id", "api-correlation-1")
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.event_id").value("api-event-1"))
                .andExpect(jsonPath("$.correlation_id").value("api-correlation-1"))
                .andExpect(jsonPath("$.duplicate").value(false));

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "api-event-1")
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true));
    }

    @Test
    void retriesTransientFailureAndEventuallyProcessesIt() {
        ingestion.ingest(new EventRequest("transient-1", "TRANSIENT_FAILURE",
                Map.of("failUntilAttempt", 2)), null, null);

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            var event = repository.find("transient-1").orElseThrow();
            assertThat(event.status()).isEqualTo(EventStatus.PROCESSED);
            assertThat(event.attempts()).isEqualTo(3);
        });
        assertThat(dlq.findAll()).isEmpty();
    }

    @Test
    void movesPoisonMessageToDlq() {
        ingestion.ingest(new EventRequest("poison-1", "POISON", Map.of()), null, null);

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(dlq.findAll()).hasSize(1));
        assertThat(repository.find("poison-1").orElseThrow().status()).isEqualTo(EventStatus.FAILED);
    }

    @Test
    void reconciliationFindsSilentDrop() throws InterruptedException {
        ingestion.ingest(new EventRequest("dropped-1", "SILENT_DROP", Map.of()), null, null);
        Thread.sleep(75);

        assertThat(reconciliation.reconcile())
                .extracting(event -> event.eventId())
                .contains("dropped-1");
    }
}
