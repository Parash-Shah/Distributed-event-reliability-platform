package com.paras.eventplatform.dlq;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class DeadLetterQueue {
    private final CopyOnWriteArrayList<DeadLetter> messages = new CopyOnWriteArrayList<>();
    private final Counter dlqCounter;

    public DeadLetterQueue(MeterRegistry registry) {
        this.dlqCounter = registry.counter("events.dlq");
        registry.gauge("events.dlq.depth", messages, List::size);
    }

    public void add(com.paras.eventplatform.event.EventRecord event, int attempts, String error) {
        messages.add(new DeadLetter(event, attempts, error, Instant.now()));
        dlqCounter.increment();
    }

    public List<DeadLetter> findAll() {
        return List.copyOf(messages);
    }

    public void clear() {
        messages.clear();
    }
}
