package com.example.saga.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic paymentRequestTopic() {
        return TopicBuilder.name("payment-requests").build();
    }
    @Bean
    public NewTopic paymentCancelTopic() {
        return TopicBuilder.name("payment-cancels").build();
    }
    @Bean
    public NewTopic inventoryRequestTopic() {
        return TopicBuilder.name("inventory-requests").build();
    }
    @Bean
    public NewTopic inventoryCancelTopic() {
        return TopicBuilder.name("inventory-cancels").build();
    }
    @Bean
    public NewTopic deliveryRequestTopic() {
        return TopicBuilder.name("delivery-requests").build();
    }
}
