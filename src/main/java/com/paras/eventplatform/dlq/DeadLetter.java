package com.paras.eventplatform.dlq;

import com.paras.eventplatform.event.EventRecord;

import java.time.Instant;

public record DeadLetter(EventRecord event, int attempts, String error, Instant failedAt) {
}
