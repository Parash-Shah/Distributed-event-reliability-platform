package com.paras.eventplatform.event;

import com.paras.eventplatform.queue.EventQueue;
import com.paras.eventplatform.queue.QueuedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@ConditionalOnExpression("'${platform.runtime.mode:all}' == 'all' || '${platform.runtime.mode:all}' == 'api'")
public class EventIngestionService {
    private final EventRepository repository;
    private final EventQueue queue;
    private final Counter receivedCounter;
    private final Counter duplicateCounter;

    public EventIngestionService(EventRepository repository, EventQueue queue, MeterRegistry registry) {
        this.repository = repository;
        this.queue = queue;
        this.receivedCounter = registry.counter("events.received");
        this.duplicateCounter = registry.counter("events.duplicates");
        registry.gauge("events.queue.depth", queue, EventQueue::size);
    }

    public EventResponse ingest(EventRequest request, String idempotencyKey, String requestedCorrelationId) {
        String eventId = firstNonBlank(idempotencyKey, request.eventId(), UUID.randomUUID().toString());
        String correlationId = firstNonBlank(requestedCorrelationId, UUID.randomUUID().toString());
        Instant now = Instant.now();
        EventRecord event = new EventRecord(eventId, correlationId, request.eventType(),
                request.payload(), now, null, EventStatus.RECEIVED, 0, null, null);

        if (!repository.createIfAbsent(event)) {
            duplicateCounter.increment();
            EventRecord existing = repository.find(eventId).orElseThrow();
            return response(existing, true);
        }

        receivedCounter.increment();
        queue.publish(QueuedEvent.availableNow(event));
        return response(event, false);
    }

    private EventResponse response(EventRecord event, boolean duplicate) {
        return new EventResponse(event.eventId(), event.correlationId(), event.status(), event.receivedAt(), duplicate);
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        throw new IllegalArgumentException("At least one value is required");
    }
}
