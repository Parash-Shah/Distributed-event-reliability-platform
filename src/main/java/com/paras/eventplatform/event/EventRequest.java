package com.paras.eventplatform.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public record EventRequest(
        String eventId,
        @NotBlank String eventType,
        @NotNull Map<String, Object> payload
) {
}
