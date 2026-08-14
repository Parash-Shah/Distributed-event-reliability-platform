package com.paras.eventplatform.event;

import java.time.Instant;
import java.util.Map;

public record EventRecord(
        String eventId,
        String correlationId,
        String eventType,
        Map<String, Object> payload,
        Instant receivedAt,
        Instant processedAt,
        EventStatus status,
        int attempts,
        String lastError
) {
    public EventRecord withStatus(EventStatus newStatus, int newAttempts, String error) {
        return new EventRecord(eventId, correlationId, eventType, payload, receivedAt,
                newStatus == EventStatus.PROCESSED ? Instant.now() : processedAt,
                newStatus, newAttempts, error);
    }
}
