package com.paras.eventplatform.event;

import com.paras.eventplatform.dlq.DeadLetter;
import com.paras.eventplatform.dlq.DeadLetterQueue;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class EventController {
    private final EventIngestionService ingestionService;
    private final EventRepository repository;
    private final DeadLetterQueue deadLetterQueue;

    public EventController(EventIngestionService ingestionService, EventRepository repository,
                           DeadLetterQueue deadLetterQueue) {
        this.ingestionService = ingestionService;
        this.repository = repository;
        this.deadLetterQueue = deadLetterQueue;
    }

    @PostMapping("/events")
    public ResponseEntity<EventResponse> createEvent(
            @Valid @RequestBody EventRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        EventResponse response = ingestionService.ingest(request, idempotencyKey, correlationId);
        return ResponseEntity.status(response.duplicate() ? HttpStatus.OK : HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/events/{eventId}")
    public ResponseEntity<EventRecord> getEvent(@PathVariable String eventId) {
        return repository.find(eventId).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/events")
    public List<EventRecord> getEvents() {
        return repository.findAll();
    }

    @GetMapping("/dlq")
    public List<DeadLetter> getDeadLetters() {
        return deadLetterQueue.findAll();
    }
}
