package com.ecommerce.deliveryservice.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DeliveryProducer {

	private final KafkaTemplate<String, Object> kafkaTemplate;

	public DeliveryProducer(KafkaTemplate<String, Object> kafkaTemplate) {
		super();
		this.kafkaTemplate = kafkaTemplate;
	}

	public void sendDeliveryCreated(DeliveryCreatedEvent event) {

		kafkaTemplate.send("delivery-created", event.getOrderId().toString(), event);

	}
}
