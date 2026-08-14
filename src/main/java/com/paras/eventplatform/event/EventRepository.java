package com.paras.eventplatform.event;

import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class EventRepository {
    private final ConcurrentHashMap<String, EventRecord> events = new ConcurrentHashMap<>();

    public boolean createIfAbsent(EventRecord event) {
        return events.putIfAbsent(event.eventId(), event) == null;
    }

    public Optional<EventRecord> find(String eventId) {
        return Optional.ofNullable(events.get(eventId));
    }

    public EventRecord update(String eventId, EventStatus status, int attempts, String error) {
        return events.compute(eventId, (ignored, current) -> {
            if (current == null) {
                throw new IllegalArgumentException("Unknown event " + eventId);
            }
            return current.withStatus(status, attempts, error);
        });
    }

    public boolean claimForProcessing(String eventId, int attempt) {
        var claimed = new java.util.concurrent.atomic.AtomicBoolean(false);
        events.compute(eventId, (ignored, current) -> {
            if (current == null) {
                throw new IllegalArgumentException("Unknown event " + eventId);
            }
            if (current.status() == EventStatus.PROCESSED || current.status() == EventStatus.PROCESSING) {
                return current;
            }
            claimed.set(true);
            return current.withStatus(EventStatus.PROCESSING, attempt, null);
        });
        return claimed.get();
    }

    public List<EventRecord> findUnprocessedBefore(Instant cutoff) {
        return events.values().stream()
                .filter(event -> event.status() != EventStatus.PROCESSED)
                .filter(event -> event.receivedAt().isBefore(cutoff))
                .toList();
    }

    public List<EventRecord> findAll() {
        return List.copyOf(events.values());
    }

    public void clear() {
        events.clear();
    }
}
