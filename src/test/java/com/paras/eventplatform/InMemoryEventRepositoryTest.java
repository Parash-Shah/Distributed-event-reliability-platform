package com.paras.eventplatform;

import com.paras.eventplatform.event.EventRecord;
import com.paras.eventplatform.event.EventStatus;
import com.paras.eventplatform.event.InMemoryEventRepository;
import com.paras.eventplatform.event.StaleEventClaimException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryEventRepositoryTest {
    @Test
    void activeProcessingLeaseCannotBeClaimedTwice() {
        InMemoryEventRepository repository = repositoryWithReceivedEvent();
        Instant now = Instant.now();

        assertThat(repository.claimForProcessing("lease-event", 1, now.minusSeconds(55))).isTrue();
        assertThat(repository.claimForProcessing("lease-event", 2, now.minusSeconds(55))).isFalse();
        assertThat(repository.find("lease-event").orElseThrow().attempts()).isEqualTo(1);
    }

    @Test
    void expiredProcessingLeaseCanBeRecoveredByAnotherWorker() {
        InMemoryEventRepository repository = repositoryWithReceivedEvent();
        Instant now = Instant.now();
        repository.claimForProcessing("lease-event", 1, now.minusSeconds(55));

        assertThat(repository.claimForProcessing("lease-event", 2, now.plusSeconds(1))).isTrue();
        assertThat(repository.find("lease-event").orElseThrow().attempts()).isEqualTo(2);
        assertThatThrownBy(() -> repository.update("lease-event", EventStatus.PROCESSED, 1, null))
                .isInstanceOf(StaleEventClaimException.class);
    }

    private InMemoryEventRepository repositoryWithReceivedEvent() {
        InMemoryEventRepository repository = new InMemoryEventRepository();
        repository.createIfAbsent(new EventRecord(
                "lease-event", "correlation-1", "ORDER_CREATED", Map.of(), Instant.now(), null,
                EventStatus.RECEIVED, 0, null, null));
        return repository;
    }
}
