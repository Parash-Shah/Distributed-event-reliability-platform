package com.paras.eventplatform.reconciliation;

import com.paras.eventplatform.event.EventRecord;
import com.paras.eventplatform.event.EventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@ConditionalOnExpression("'${platform.runtime.mode:all}' == 'all' || '${platform.runtime.mode:all}' == 'reconciliation'")
public class ReconciliationJob {
    private static final Logger log = LoggerFactory.getLogger(ReconciliationJob.class);
    private final EventRepository repository;
    private final Duration staleAfter;
    private final AtomicInteger missingEvents = new AtomicInteger();

    public ReconciliationJob(EventRepository repository, MeterRegistry registry,
                             @Value("${platform.reconciliation.stale-after:5s}") Duration staleAfter) {
        this.repository = repository;
        this.staleAfter = staleAfter;
        registry.gauge("events.reconciliation.missing", missingEvents);
    }

    @Scheduled(fixedDelayString = "${platform.reconciliation.interval-ms:5000}")
    public List<EventRecord> reconcile() {
        List<EventRecord> missing = repository.findUnprocessedBefore(Instant.now().minus(staleAfter));
        missingEvents.set(missing.size());
        if (!missing.isEmpty()) {
            log.error("reconciliation_discrepancy missing_count={} event_ids={}", missing.size(),
                    missing.stream().map(EventRecord::eventId).toList());
        }
        return missing;
    }
}
