package com.ecommerce.deliveryservice.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.ecommerce.deliveryservice.service.DeliveryService;

@Component
public class DeliveryConsumer {
	
	private final DeliveryService deliveryService;
	
	
	public DeliveryConsumer(DeliveryService deliveryService) {
		super();
		this.deliveryService = deliveryService;
	}

    

	@KafkaListener(topics = "payment-success", groupId = "delivery-service-group")
	public void consumePaymentSuccess(PaymentSuccessEvent event) {
		
		deliveryService.createDelivery(event);
		
	}

}
