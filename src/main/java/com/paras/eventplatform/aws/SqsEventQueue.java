package com.paras.eventplatform.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paras.eventplatform.event.EventRecord;
import com.paras.eventplatform.queue.EventQueue;
import com.paras.eventplatform.queue.QueuedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.time.Duration;
import java.util.Map;

@Component
@Profile("aws")
public class SqsEventQueue implements EventQueue {
    private final SqsClient sqs;
    private final ObjectMapper objectMapper;
    private final String queueUrl;
    private final int waitTimeSeconds;

    public SqsEventQueue(SqsClient sqs, ObjectMapper objectMapper,
                         @Value("${platform.aws.sqs.event-queue-url}") String queueUrl,
                         @Value("${platform.aws.sqs.wait-time-seconds:10}") int waitTimeSeconds) {
        this.sqs = sqs;
        this.objectMapper = objectMapper;
        this.queueUrl = queueUrl;
        this.waitTimeSeconds = waitTimeSeconds;
    }

    @Override
    public void publish(QueuedEvent event) {
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody(write(event.event())));
    }

    @Override
    public QueuedEvent take() throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            var response = sqs.receiveMessage(request -> request
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(waitTimeSeconds)
                    .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT));
            if (!response.messages().isEmpty()) {
                var message = response.messages().getFirst();
                String receiveCount = message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
                int attempt = receiveCount == null ? 1 : Integer.parseInt(receiveCount);
                return new QueuedEvent(read(message.body()), attempt, System.nanoTime(),
                        message.messageId(), message.receiptHandle());
            }
        }
        throw new InterruptedException("SQS worker interrupted");
    }

    @Override
    public void acknowledge(QueuedEvent event) {
        requireReceiptHandle(event);
        sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(event.receiptHandle()));
    }

    @Override
    public void retry(QueuedEvent event, Duration delay) {
        requireReceiptHandle(event);
        long roundedUpSeconds = Math.max(1, (delay.toMillis() + 999) / 1000);
        int visibilitySeconds = Math.toIntExact(Math.min(43_200, roundedUpSeconds));
        sqs.changeMessageVisibility(request -> request
                .queueUrl(queueUrl)
                .receiptHandle(event.receiptHandle())
                .visibilityTimeout(visibilitySeconds));
    }

    @Override
    public int size() {
        Map<QueueAttributeName, String> attributes = sqs.getQueueAttributes(request -> request
                        .queueUrl(queueUrl)
                        .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
                .attributes();
        return parse(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                + parse(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Refusing to purge the AWS event queue through the application");
    }

    private void requireReceiptHandle(QueuedEvent event) {
        if (event.receiptHandle() == null || event.receiptHandle().isBlank()) {
            throw new IllegalArgumentException("An SQS receipt handle is required");
        }
    }

    private int parse(String value) {
        return value == null ? 0 : Integer.parseInt(value);
    }

    private String write(EventRecord event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Event cannot be serialized for SQS", failure);
        }
    }

    private EventRecord read(String body) {
        try {
            return objectMapper.readValue(body, EventRecord.class);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("SQS message is not a valid event", failure);
        }
    }
}
