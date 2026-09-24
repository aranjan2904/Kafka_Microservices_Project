package com.ecommerce.paymentservice.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.ecommerce.paymentservice.service.PaymentService;

@Component
public class PaymentConsumer {
	
	private final PaymentService paymentService;
	
	
	public PaymentConsumer(PaymentService paymentService) {
		super();
		this.paymentService = paymentService;
	}


	@KafkaListener(topics = "order-created", groupId = "payment-service-group")
	public void consumeOrderCreated(OrderCreatedEvent event) {
		
		paymentService.processPayment(event);
		
	}

}
