package com.paras.eventplatform.queue;

import com.paras.eventplatform.event.EventRecord;

import java.time.Duration;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public record QueuedEvent(
        EventRecord event,
        int attempt,
        long availableAtNanos,
        String messageId,
        String receiptHandle
) implements Delayed {
    public static QueuedEvent availableNow(EventRecord event) {
        return new QueuedEvent(event, 1, System.nanoTime(), null, null);
    }

    public QueuedEvent retryAfter(Duration delay) {
        return new QueuedEvent(event, attempt + 1, System.nanoTime() + delay.toNanos(), messageId, receiptHandle);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(availableAtNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other instanceof QueuedEvent queuedEvent) {
            return Long.compare(availableAtNanos, queuedEvent.availableAtNanos);
        }
        return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }
}
