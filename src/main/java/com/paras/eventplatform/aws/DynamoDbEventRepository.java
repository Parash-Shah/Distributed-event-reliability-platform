package com.paras.eventplatform.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paras.eventplatform.event.EventRecord;
import com.paras.eventplatform.event.EventRepository;
import com.paras.eventplatform.event.EventStatus;
import com.paras.eventplatform.event.StaleEventClaimException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
@Profile("aws")
public class DynamoDbEventRepository implements EventRepository {
    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {};

    private final DynamoDbClient dynamoDb;
    private final ObjectMapper objectMapper;
    private final String tableName;

    public DynamoDbEventRepository(DynamoDbClient dynamoDb, ObjectMapper objectMapper,
                                   @Value("${platform.aws.dynamodb.event-table}") String tableName) {
        this.dynamoDb = dynamoDb;
        this.objectMapper = objectMapper;
        this.tableName = tableName;
    }

    @Override
    public boolean createIfAbsent(EventRecord event) {
        try {
            dynamoDb.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(toItem(event))
                    .conditionExpression("attribute_not_exists(event_id)")
                    .build());
            return true;
        } catch (ConditionalCheckFailedException duplicate) {
            return false;
        }
    }

    @Override
    public Optional<EventRecord> find(String eventId) {
        var response = dynamoDb.getItem(request -> request
                .tableName(tableName)
                .consistentRead(true)
                .key(key(eventId)));
        return response.hasItem() && !response.item().isEmpty()
                ? Optional.of(fromItem(response.item()))
                : Optional.empty();
    }

    @Override
    public EventRecord update(String eventId, EventStatus status, int attempts, String error) {
        Map<String, String> names = Map.of("#status", "status");
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":status", string(status.name()));
        values.put(":attempts", number(attempts));
        values.put(":processing", string(EventStatus.PROCESSING.name()));

        StringBuilder expression = new StringBuilder("SET #status = :status, attempts = :attempts");
        if (status == EventStatus.PROCESSED) {
            expression.append(", processed_at = :processedAt");
            values.put(":processedAt", string(Instant.now().toString()));
        }
        if (error != null) {
            expression.append(", last_error = :error");
            values.put(":error", string(error));
        }
        expression.append(" REMOVE processing_started_at");
        if (error == null) expression.append(", last_error");

        try {
            var response = dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key(eventId))
                    .updateExpression(expression.toString())
                    .conditionExpression("#status = :processing AND attempts = :attempts")
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .returnValues(ReturnValue.ALL_NEW)
                    .build());
            return fromItem(response.attributes());
        } catch (ConditionalCheckFailedException staleClaim) {
            throw new StaleEventClaimException(eventId, attempts);
        }
    }

    @Override
    public boolean claimForProcessing(String eventId, int attempt, Instant staleBefore) {
        try {
            dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key(eventId))
                    .updateExpression("SET #status = :processing, attempts = :attempt, processing_started_at = :started REMOVE last_error")
                    .conditionExpression("#status = :received OR #status = :failed OR "
                            + "(#status = :processing AND processing_started_at < :staleBefore)")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":processing", string(EventStatus.PROCESSING.name()),
                            ":received", string(EventStatus.RECEIVED.name()),
                            ":failed", string(EventStatus.FAILED.name()),
                            ":attempt", number(attempt),
                            ":started", string(Instant.now().toString()),
                            ":staleBefore", string(staleBefore.toString())))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException alreadyClaimed) {
            return false;
        }
    }

    @Override
    public List<EventRecord> findUnprocessedBefore(Instant cutoff) {
        return findAll().stream()
                .filter(event -> event.status() != EventStatus.PROCESSED)
                .filter(event -> event.receivedAt().isBefore(cutoff))
                .toList();
    }

    @Override
    public List<EventRecord> findAll() {
        List<EventRecord> events = new ArrayList<>();
        dynamoDb.scanPaginator(ScanRequest.builder().tableName(tableName).build())
                .items().stream().map(this::fromItem).forEach(events::add);
        return List.copyOf(events);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Refusing to clear the AWS event table through the application");
    }

    private Map<String, AttributeValue> toItem(EventRecord event) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("event_id", string(event.eventId()));
        item.put("correlation_id", string(event.correlationId()));
        item.put("event_type", string(event.eventType()));
        item.put("payload_json", string(writePayload(event.payload())));
        item.put("received_at", string(event.receivedAt().toString()));
        item.put("status", string(event.status().name()));
        item.put("attempts", number(event.attempts()));
        if (event.processedAt() != null) item.put("processed_at", string(event.processedAt().toString()));
        if (event.lastError() != null) item.put("last_error", string(event.lastError()));
        if (event.processingStartedAt() != null) {
            item.put("processing_started_at", string(event.processingStartedAt().toString()));
        }
        return item;
    }

    private EventRecord fromItem(Map<String, AttributeValue> item) {
        return new EventRecord(
                item.get("event_id").s(),
                item.get("correlation_id").s(),
                item.get("event_type").s(),
                readPayload(item.get("payload_json").s()),
                Instant.parse(item.get("received_at").s()),
                item.containsKey("processed_at") ? Instant.parse(item.get("processed_at").s()) : null,
                EventStatus.valueOf(item.get("status").s()),
                Integer.parseInt(item.get("attempts").n()),
                item.containsKey("last_error") ? item.get("last_error").s() : null,
                item.containsKey("processing_started_at")
                        ? Instant.parse(item.get("processing_started_at").s()) : null);
    }

    private Map<String, AttributeValue> key(String eventId) {
        return Map.of("event_id", string(eventId));
    }

    private AttributeValue string(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private AttributeValue number(int value) {
        return AttributeValue.builder().n(Integer.toString(value)).build();
    }

    private String writePayload(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Event payload cannot be serialized", failure);
        }
    }

    private Map<String, Object> readPayload(String payload) {
        try {
            return objectMapper.readValue(payload, PAYLOAD_TYPE);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Stored event payload cannot be deserialized", failure);
        }
    }
}
