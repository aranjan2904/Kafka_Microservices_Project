package com.ecommerce.deliveryservice.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic deliveryCreatedTopic() {
        return new NewTopic("delivery-created", 3, (short) 1);
    }
}