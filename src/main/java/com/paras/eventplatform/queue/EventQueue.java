package com.paras.eventplatform.queue;

import org.springframework.stereotype.Component;

import java.util.concurrent.DelayQueue;

@Component
public class EventQueue {
    private final DelayQueue<QueuedEvent> queue = new DelayQueue<>();

    public void publish(QueuedEvent event) {
        queue.put(event);
    }

    public QueuedEvent take() throws InterruptedException {
        return queue.take();
    }

    public int size() {
        return queue.size();
    }

    public void clear() {
        queue.clear();
    }
}
