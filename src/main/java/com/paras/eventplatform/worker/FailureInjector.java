package com.paras.eventplatform.worker;

import com.paras.eventplatform.event.EventRecord;
import org.springframework.stereotype.Component;

@Component
public class FailureInjector {
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

    public enum ProcessingDecision { PROCESS, FAIL, DROP }
}
