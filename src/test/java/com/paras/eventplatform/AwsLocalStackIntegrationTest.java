package com.paras.eventplatform;

import com.paras.eventplatform.event.EventIngestionService;
import com.paras.eventplatform.event.EventRepository;
import com.paras.eventplatform.event.EventRequest;
import com.paras.eventplatform.event.EventStatus;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("aws")
@EnabledIfEnvironmentVariable(named = "RUN_LOCALSTACK_TESTS", matches = "true")
@SpringBootTest(properties = {
        "platform.aws.region=us-east-1",
        "platform.aws.endpoint=http://localhost:4566",
        "platform.aws.dynamodb.event-table=event-platform-events",
        "platform.aws.sqs.event-queue-url=http://localhost:4566/queue/us-east-1/000000000000/event-platform-events",
        "platform.aws.sqs.dlq-url=http://localhost:4566/queue/us-east-1/000000000000/event-platform-dlq",
        "platform.aws.sqs.wait-time-seconds=1",
        "platform.workers.count=2"
})
class AwsLocalStackIntegrationTest {
    @Autowired EventIngestionService ingestion;
    @Autowired EventRepository repository;

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
}
