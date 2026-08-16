package com.paras.eventplatform;

import com.paras.eventplatform.event.EventController;
import com.paras.eventplatform.event.EventIngestionService;
import com.paras.eventplatform.reconciliation.ReconciliationJob;
import com.paras.eventplatform.worker.EventProcessor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeModeConfigurationTest {
    @Test
    void apiModeStartsOnlyApiComponents() {
        try (ConfigurableApplicationContext context = contextFor("api")) {
            assertThat(context.getBeansOfType(EventController.class)).hasSize(1);
            assertThat(context.getBeansOfType(EventIngestionService.class)).hasSize(1);
            assertThat(context.getBeansOfType(EventProcessor.class)).isEmpty();
            assertThat(context.getBeansOfType(ReconciliationJob.class)).isEmpty();
        }
    }

    @Test
    void workerModeStartsOnlyWorkerComponents() {
        try (ConfigurableApplicationContext context = contextFor("worker")) {
            assertThat(context.getBeansOfType(EventController.class)).isEmpty();
            assertThat(context.getBeansOfType(EventIngestionService.class)).isEmpty();
            assertThat(context.getBeansOfType(EventProcessor.class)).hasSize(1);
            assertThat(context.getBeansOfType(ReconciliationJob.class)).isEmpty();
        }
    }

    @Test
    void reconciliationModeStartsOnlyReconciliationComponents() {
        try (ConfigurableApplicationContext context = contextFor("reconciliation")) {
            assertThat(context.getBeansOfType(EventController.class)).isEmpty();
            assertThat(context.getBeansOfType(EventIngestionService.class)).isEmpty();
            assertThat(context.getBeansOfType(EventProcessor.class)).isEmpty();
            assertThat(context.getBeansOfType(ReconciliationJob.class)).hasSize(1);
        }
    }

    private ConfigurableApplicationContext contextFor(String mode) {
        return new SpringApplicationBuilder(EventPlatformApplication.class)
                .web(WebApplicationType.NONE)
                .run("--platform.runtime.mode=" + mode);
    }
}
