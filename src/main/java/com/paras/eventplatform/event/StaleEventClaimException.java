package com.paras.eventplatform.event;

public class StaleEventClaimException extends RuntimeException {
    public StaleEventClaimException(String eventId, int attempt) {
        super("Worker no longer owns event %s for attempt %d".formatted(eventId, attempt));
    }
}
