package com.paras.eventplatform.event;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Repository
@Profile("!aws")
public class InMemoryEventRepository implements EventRepository {
    private final ConcurrentHashMap<String, EventRecord> events = new ConcurrentHashMap<>();

    @Override
    public boolean createIfAbsent(EventRecord event) {
        return events.putIfAbsent(event.eventId(), event) == null;
    }

    @Override
    public Optional<EventRecord> find(String eventId) {
        return Optional.ofNullable(events.get(eventId));
    }

    @Override
    public EventRecord update(String eventId, EventStatus status, int attempts, String error) {
        return events.compute(eventId, (ignored, current) -> {
            if (current == null) {
                throw new IllegalArgumentException("Unknown event " + eventId);
            }
            if (current.status() != EventStatus.PROCESSING || current.attempts() != attempts) {
                throw new StaleEventClaimException(eventId, attempts);
            }
            return current.withStatus(status, attempts, error);
        });
    }

    @Override
    public boolean claimForProcessing(String eventId, int attempt, Instant staleBefore) {
        AtomicBoolean claimed = new AtomicBoolean(false);
        events.compute(eventId, (ignored, current) -> {
            if (current == null) {
                throw new IllegalArgumentException("Unknown event " + eventId);
            }
            boolean activeLease = current.status() == EventStatus.PROCESSING
                    && current.processingStartedAt() != null
                    && !current.processingStartedAt().isBefore(staleBefore);
            if (current.status() == EventStatus.PROCESSED || activeLease) {
                return current;
            }
            claimed.set(true);
            return current.withStatus(EventStatus.PROCESSING, attempt, null);
        });
        return claimed.get();
    }

    @Override
    public List<EventRecord> findUnprocessedBefore(Instant cutoff) {
        return events.values().stream()
                .filter(event -> event.status() != EventStatus.PROCESSED)
                .filter(event -> event.receivedAt().isBefore(cutoff))
                .toList();
    }

    @Override
    public List<EventRecord> findAll() {
        return List.copyOf(events.values());
    }

    @Override
    public void clear() {
        events.clear();
    }
}
