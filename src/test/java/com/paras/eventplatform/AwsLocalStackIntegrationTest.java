package com.paras.eventplatform;

import com.paras.eventplatform.event.EventIngestionService;
import com.paras.eventplatform.event.EventRecord;
import com.paras.eventplatform.event.EventRepository;
import com.paras.eventplatform.event.EventRequest;
import com.paras.eventplatform.event.EventStatus;
import com.paras.eventplatform.event.StaleEventClaimException;
import com.paras.eventplatform.queue.EventQueue;
import com.paras.eventplatform.queue.QueuedEvent;
import com.paras.eventplatform.reconciliation.ReconciliationJob;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ActiveProfiles("aws")
@EnabledIfEnvironmentVariable(named = "RUN_LOCALSTACK_TESTS", matches = "true")
@SpringBootTest(properties = {
        "platform.aws.region=us-east-1",
        "platform.aws.endpoint=http://localhost:4566",
        "platform.aws.dynamodb.event-table=event-platform-events",
        "platform.aws.sqs.event-queue-url=http://localhost:4566/queue/us-east-1/000000000000/event-platform-events",
        "platform.aws.sqs.dlq-url=http://localhost:4566/queue/us-east-1/000000000000/event-platform-dlq",
        "platform.aws.sqs.wait-time-seconds=1",
        "platform.workers.count=1",
        "platform.retry.max-attempts=3",
        "platform.retry.initial-backoff-ms=10",
        "platform.processing.claim-timeout=1s",
        "platform.reconciliation.stale-after=25ms",
        "platform.reconciliation.interval-ms=60000"
})
class AwsLocalStackIntegrationTest {
    @Autowired EventIngestionService ingestion;
    @Autowired EventRepository repository;
    @Autowired EventQueue queue;
    @Autowired ReconciliationJob reconciliation;
    @Autowired SqsClient sqs;

    @Test
    void persistsQueuesAndProcessesThroughAwsAdapters() {
        String eventId = "localstack-" + UUID.randomUUID();
        ingestion.ingest(new EventRequest(eventId, "ORDER_CREATED", Map.of("orderId", "aws-1")), null, null);

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var event = repository.find(eventId).orElseThrow();
            assertThat(event.status()).isEqualTo(EventStatus.PROCESSED);
            assertThat(event.attempts()).isEqualTo(1);
        });
    }

    @Test
    void rejectsDuplicateSubmissionAndAcknowledgesDuplicateDelivery() {
        String eventId = id("duplicate");
        EventRequest request = new EventRequest(eventId, "ORDER_CREATED", Map.of("orderId", eventId));

        assertThat(ingestion.ingest(request, null, null).duplicate()).isFalse();
        assertThat(ingestion.ingest(request, null, null).duplicate()).isTrue();
        EventRecord processed = awaitStatus(eventId, EventStatus.PROCESSED);

        queue.publish(QueuedEvent.availableNow(processed));
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(queue.size()).isZero());

        EventRecord afterRedelivery = repository.find(eventId).orElseThrow();
        assertThat(afterRedelivery.status()).isEqualTo(EventStatus.PROCESSED);
        assertThat(afterRedelivery.attempts()).isEqualTo(1);
    }

    @Test
    void retriesTransientFailureUntilItSucceeds() {
        String eventId = id("transient");
        ingestion.ingest(new EventRequest(eventId, "TRANSIENT_FAILURE",
                Map.of("failUntilAttempt", 2)), null, null);

        EventRecord processed = awaitStatus(eventId, EventStatus.PROCESSED);
        assertThat(processed.attempts()).isEqualTo(3);
    }

    @Test
    void routesPoisonMessageToSqsDeadLetterQueue() {
        int depthBefore = dlqDepth();
        String eventId = id("poison");
        ingestion.ingest(new EventRequest(eventId, "POISON", Map.of()), null, null);

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            EventRecord failed = repository.find(eventId).orElseThrow();
            assertThat(failed.status()).isEqualTo(EventStatus.FAILED);
            assertThat(failed.attempts()).isEqualTo(3);
            assertThat(dlqDepth()).isGreaterThan(depthBefore);
        });
    }

    @Test
    void reconciliationDetectsSilentlyLostEvent() throws InterruptedException {
        String eventId = id("silent");
        ingestion.ingest(new EventRequest(eventId, "SILENT_DROP", Map.of()), null, null);
        awaitStatus(eventId, EventStatus.PROCESSING);
        Thread.sleep(75);

        assertThat(reconciliation.reconcile())
                .extracting(EventRecord::eventId)
                .contains(eventId);
    }

    @Test
    void expiredDynamoDbLeaseCanBeReclaimedAndRejectsStaleOwner() {
        String eventId = id("lease");
        repository.createIfAbsent(new EventRecord(
                eventId, id("correlation"), "ORDER_CREATED", Map.of(), Instant.now(), null,
                EventStatus.RECEIVED, 0, null, null));
        Instant now = Instant.now();

        assertThat(repository.claimForProcessing(eventId, 1, now.minusSeconds(1))).isTrue();
        assertThat(repository.claimForProcessing(eventId, 2, now.minusSeconds(1))).isFalse();
        assertThat(repository.claimForProcessing(eventId, 2, now.plusSeconds(1))).isTrue();
        assertThatThrownBy(() -> repository.update(eventId, EventStatus.PROCESSED, 1, null))
                .isInstanceOf(StaleEventClaimException.class);
    }

    private EventRecord awaitStatus(String eventId, EventStatus status) {
        final EventRecord[] result = new EventRecord[1];
        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            result[0] = repository.find(eventId).orElseThrow();
            assertThat(result[0].status()).isEqualTo(status);
        });
        return result[0];
    }

    private int dlqDepth() {
        Map<QueueAttributeName, String> attributes = sqs.getQueueAttributes(request -> request
                        .queueUrl("http://localhost:4566/queue/us-east-1/000000000000/event-platform-dlq")
                        .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
                .attributes();
        return parse(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                + parse(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
    }

    private int parse(String value) {
        return value == null ? 0 : Integer.parseInt(value);
    }

    private String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
