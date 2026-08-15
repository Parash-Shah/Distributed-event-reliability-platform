package com.paras.eventplatform.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paras.eventplatform.dlq.DeadLetter;
import com.paras.eventplatform.dlq.DeadLetterQueue;
import com.paras.eventplatform.event.EventRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Instant;
import java.util.List;

@Component
@Profile("aws")
public class SqsDeadLetterQueue implements DeadLetterQueue {
    private final SqsClient sqs;
    private final ObjectMapper objectMapper;
    private final String queueUrl;
    private final Counter dlqCounter;

    public SqsDeadLetterQueue(SqsClient sqs, ObjectMapper objectMapper,
                              MeterRegistry registry,
                              @Value("${platform.aws.sqs.dlq-url}") String queueUrl) {
        this.sqs = sqs;
        this.objectMapper = objectMapper;
        this.queueUrl = queueUrl;
        this.dlqCounter = registry.counter("events.dlq");
    }

    @Override
    public void add(EventRecord event, int attempts, String error) {
        DeadLetter deadLetter = new DeadLetter(event, attempts, error, Instant.now());
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody(write(deadLetter)));
        dlqCounter.increment();
    }

    @Override
    public List<DeadLetter> findAll() {
        return List.of();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Refusing to purge the AWS DLQ through the application");
    }

    private String write(DeadLetter deadLetter) {
        try {
            return objectMapper.writeValueAsString(deadLetter);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Dead letter cannot be serialized for SQS", failure);
        }
    }
}
