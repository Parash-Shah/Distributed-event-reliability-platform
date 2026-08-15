package com.paras.eventplatform.event;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface EventRepository {
    boolean createIfAbsent(EventRecord event);

    Optional<EventRecord> find(String eventId);

    EventRecord update(String eventId, EventStatus status, int attempts, String error);

    boolean claimForProcessing(String eventId, int attempt, Instant staleBefore);

    List<EventRecord> findUnprocessedBefore(Instant cutoff);

    List<EventRecord> findAll();

    void clear();
}
