package com.paras.eventplatform.queue;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.DelayQueue;

@Component
@Profile("!aws")
public class InMemoryEventQueue implements EventQueue {
    private final DelayQueue<QueuedEvent> queue = new DelayQueue<>();

    @Override
    public void publish(QueuedEvent event) {
        queue.put(event);
    }

    @Override
    public QueuedEvent take() throws InterruptedException {
        return queue.take();
    }

    @Override
    public void acknowledge(QueuedEvent event) {
        // Removing an item from DelayQueue is the local equivalent of acknowledgement.
    }

    @Override
    public void retry(QueuedEvent event, Duration delay) {
        queue.put(event.retryAfter(delay));
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public void clear() {
        queue.clear();
    }
}
