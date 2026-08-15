package com.paras.eventplatform.worker;

import com.paras.eventplatform.dlq.DeadLetterQueue;
import com.paras.eventplatform.event.EventRecord;
import com.paras.eventplatform.event.EventRepository;
import com.paras.eventplatform.event.EventStatus;
import com.paras.eventplatform.event.StaleEventClaimException;
import com.paras.eventplatform.queue.EventQueue;
import com.paras.eventplatform.queue.QueuedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class EventProcessor {
    private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);

    private final EventQueue queue;
    private final EventRepository repository;
    private final DeadLetterQueue deadLetterQueue;
    private final FailureInjector failureInjector;
    private final int workerCount;
    private final int maxAttempts;
    private final long initialBackoffMs;
    private final Duration claimTimeout;
    private final Counter processedCounter;
    private final Counter failedCounter;
    private final Counter retriedCounter;
    private final Counter duplicateCounter;
    private final Timer processingLatency;
    private ExecutorService executor;

    public EventProcessor(EventQueue queue, EventRepository repository, DeadLetterQueue deadLetterQueue,
                          FailureInjector failureInjector, MeterRegistry registry,
                          @Value("${platform.workers.count:4}") int workerCount,
                          @Value("${platform.retry.max-attempts:4}") int maxAttempts,
                          @Value("${platform.retry.initial-backoff-ms:250}") long initialBackoffMs,
                          @Value("${platform.processing.claim-timeout:55s}") Duration claimTimeout) {
        this.queue = queue;
        this.repository = repository;
        this.deadLetterQueue = deadLetterQueue;
        this.failureInjector = failureInjector;
        this.workerCount = workerCount;
        this.maxAttempts = maxAttempts;
        this.initialBackoffMs = initialBackoffMs;
        this.claimTimeout = claimTimeout;
        this.processedCounter = registry.counter("events.processed");
        this.failedCounter = registry.counter("events.failed");
        this.retriedCounter = registry.counter("events.retried");
        this.duplicateCounter = registry.counter("events.worker.duplicates");
        this.processingLatency = Timer.builder("events.processing.latency")
                .publishPercentiles(0.5, 0.95, 0.99).register(registry);
    }

    @PostConstruct
    void start() {
        executor = Executors.newFixedThreadPool(workerCount);
        for (int index = 0; index < workerCount; index++) {
            executor.submit(this::workLoop);
        }
    }

    @PreDestroy
    void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private void workLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                process(queue.take());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception unexpected) {
                log.error("worker_loop_failure", unexpected);
                pauseAfterInfrastructureFailure();
            }
        }
    }

    private void pauseAfterInfrastructureFailure() {
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public void process(QueuedEvent queued) {
        EventRecord current = repository.find(queued.event().eventId()).orElseThrow();
        if (current.status() == EventStatus.PROCESSED) {
            duplicateCounter.increment();
            queue.acknowledge(queued);
            log.info("event_duplicate event_id={} correlation_id={}", current.eventId(), current.correlationId());
            return;
        }
        if (!repository.claimForProcessing(current.eventId(), queued.attempt(),
                java.time.Instant.now().minus(claimTimeout))) {
            queue.retry(queued, Duration.ofMillis(initialBackoffMs));
            log.info("event_claim_deferred event_id={} correlation_id={} status={}",
                    current.eventId(), current.correlationId(), current.status());
            return;
        }

        try {
            FailureInjector.ProcessingDecision decision = failureInjector.evaluate(current, queued.attempt());
            if (decision == FailureInjector.ProcessingDecision.DROP) {
                queue.acknowledge(queued);
                log.warn("event_silently_dropped event_id={} correlation_id={}", current.eventId(), current.correlationId());
                return;
            }
            if (decision == FailureInjector.ProcessingDecision.FAIL) {
                throw new IllegalStateException("Injected failure for " + current.eventType());
            }

            repository.update(current.eventId(), EventStatus.PROCESSED, queued.attempt(), null);
        } catch (StaleEventClaimException staleClaim) {
            log.warn("event_claim_lost event_id={} correlation_id={} attempt={}",
                    current.eventId(), current.correlationId(), queued.attempt());
            return;
        } catch (RuntimeException failure) {
            handleFailure(current, queued, failure);
            return;
        }

        queue.acknowledge(queued);
        processedCounter.increment();
        processingLatency.record(Duration.between(current.receivedAt(), java.time.Instant.now()));
        log.info("event_processed event_id={} correlation_id={} attempt={}",
                current.eventId(), current.correlationId(), queued.attempt());
    }

    private void handleFailure(EventRecord event, QueuedEvent queued, RuntimeException failure) {
        failedCounter.increment();
        EventRecord failed = repository.update(event.eventId(), EventStatus.FAILED,
                queued.attempt(), failure.getMessage());
        if (queued.attempt() >= maxAttempts) {
            deadLetterQueue.add(failed, queued.attempt(), failure.getMessage());
            queue.acknowledge(queued);
            log.error("event_moved_to_dlq event_id={} correlation_id={} attempts={}",
                    event.eventId(), event.correlationId(), queued.attempt());
            return;
        }

        long delay = initialBackoffMs * (1L << (queued.attempt() - 1));
        retriedCounter.increment();
        queue.retry(queued, Duration.ofMillis(delay));
        log.warn("event_retry_scheduled event_id={} correlation_id={} attempt={} delay_ms={}",
                event.eventId(), event.correlationId(), queued.attempt() + 1, delay);
    }
}
