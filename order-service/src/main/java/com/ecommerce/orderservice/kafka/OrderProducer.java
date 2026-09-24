package com.ecommerce.orderservice.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public class OrderProducer {
	
	private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

	public OrderProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
		super();
		this.kafkaTemplate = kafkaTemplate;
	}
	
	public void sendOrderCreatedEvent(OrderCreatedEvent event) {
		
		kafkaTemplate.send("order-created", event.getOrderId().toString(), event);
	}
}
