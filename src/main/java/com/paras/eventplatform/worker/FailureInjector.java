package com.paras.eventplatform.worker;

import com.paras.eventplatform.event.EventRecord;
import org.springframework.stereotype.Component;

@Component
public class FailureInjector {
    private static final long MAX_PROCESSING_DELAY_MS = 300_000;

    public void delayIfRequested(EventRecord event) throws InterruptedException {
        if (!event.eventType().equalsIgnoreCase("SLOW_PROCESSING")) {
            return;
        }
        long delayMs = longPayload(event, "processingDelayMs", 30_000);
        Thread.sleep(Math.clamp(delayMs, 0, MAX_PROCESSING_DELAY_MS));
    }

    public ProcessingDecision evaluate(EventRecord event, int attempt) {
        return switch (event.eventType().toUpperCase()) {
            case "POISON" -> ProcessingDecision.FAIL;
            case "TRANSIENT_FAILURE" -> attempt <= intPayload(event, "failUntilAttempt", 2)
                    ? ProcessingDecision.FAIL : ProcessingDecision.PROCESS;
            case "SILENT_DROP" -> ProcessingDecision.DROP;
            default -> ProcessingDecision.PROCESS;
        };
    }

    private int intPayload(EventRecord event, String key, int defaultValue) {
        Object value = event.payload().get(key);
        return value instanceof Number number ? number.intValue() : defaultValue;
    }

    private long longPayload(EventRecord event, String key, long defaultValue) {
        Object value = event.payload().get(key);
        return value instanceof Number number ? number.longValue() : defaultValue;
    }

    public enum ProcessingDecision { PROCESS, FAIL, DROP }
}
