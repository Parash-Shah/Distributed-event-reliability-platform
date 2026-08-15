package com.paras.eventplatform.dlq;

import com.paras.eventplatform.event.EventRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
@Profile("!aws")
public class InMemoryDeadLetterQueue implements DeadLetterQueue {
    private final CopyOnWriteArrayList<DeadLetter> messages = new CopyOnWriteArrayList<>();
    private final Counter dlqCounter;

    public InMemoryDeadLetterQueue(MeterRegistry registry) {
        this.dlqCounter = registry.counter("events.dlq");
        registry.gauge("events.dlq.depth", messages, List::size);
    }

    @Override
    public void add(EventRecord event, int attempts, String error) {
        messages.add(new DeadLetter(event, attempts, error, Instant.now()));
        dlqCounter.increment();
    }

    @Override
    public List<DeadLetter> findAll() {
        return List.copyOf(messages);
    }

    @Override
    public void clear() {
        messages.clear();
    }
}
