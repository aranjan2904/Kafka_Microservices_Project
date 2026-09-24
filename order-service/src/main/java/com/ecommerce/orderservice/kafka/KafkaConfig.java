package com.ecommerce.orderservice.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
	
	@Bean
	public NewTopic orderCreatedTopic() {
		
		return new NewTopic("order-created", 3, (short) 1);
	}

}
