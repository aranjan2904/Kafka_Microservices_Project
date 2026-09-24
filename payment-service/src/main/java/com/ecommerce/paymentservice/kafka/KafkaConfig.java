package com.ecommerce.paymentservice.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
	
	@Bean
	public NewTopic paymentSuccessTopic( ) {
		
		return new NewTopic("payment-success",1, (short)1);
	}
	
	@Bean
	public NewTopic paymentFailedTopic() {
		
		return new NewTopic("payment-failed", 3, (short)1);
	}

}