package com.paras.eventplatform.event;

import java.time.Instant;

public record EventResponse(
        String eventId,
        String correlationId,
        EventStatus status,
        Instant receivedAt,
        boolean duplicate
) {
}
