package com.paras.eventplatform.queue;

import java.time.Duration;

public interface EventQueue {
    void publish(QueuedEvent event);

    QueuedEvent take() throws InterruptedException;

    void acknowledge(QueuedEvent event);

    void retry(QueuedEvent event, Duration delay);

    int size();

    void clear();
}
