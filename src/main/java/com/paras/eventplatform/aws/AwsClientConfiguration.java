package com.paras.eventplatform.aws;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.util.function.Consumer;

@Configuration
@Profile("aws")
public class AwsClientConfiguration {
    @Bean
    SqsClient sqsClient(@Value("${platform.aws.region}") String region,
                        @Value("${platform.aws.endpoint:}") String endpoint) {
        var builder = SqsClient.builder().region(Region.of(region));
        applyEndpoint(endpoint, builder::endpointOverride);
        return builder.build();
    }

    @Bean
    DynamoDbClient dynamoDbClient(@Value("${platform.aws.region}") String region,
                                   @Value("${platform.aws.endpoint:}") String endpoint) {
        var builder = DynamoDbClient.builder().region(Region.of(region));
        applyEndpoint(endpoint, builder::endpointOverride);
        return builder.build();
    }

    private void applyEndpoint(String endpoint, Consumer<URI> endpointSetter) {
        if (endpoint != null && !endpoint.isBlank()) {
            endpointSetter.accept(URI.create(endpoint));
        }
    }
}
