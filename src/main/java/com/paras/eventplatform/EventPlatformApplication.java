package com.paras.eventplatform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class EventPlatformApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventPlatformApplication.class, args);
    }
}
