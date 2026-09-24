package com.ecommerce.paymentservice.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentProducer {
	
	private final KafkaTemplate<String, Object> kafkaTemplate;

	public PaymentProducer(KafkaTemplate<String, Object> kafkaTemplate) {
		super();
		this.kafkaTemplate = kafkaTemplate;
	}
	
	public void sendPaymentSuccess(PaymentSuccessEvent event) {
		
		kafkaTemplate.send("payment-success", event.getOrderId().toString(),event);
	}
	
	public void sendPaymentFailed(PaymentFailedEvent event) {
		
		kafkaTemplate.send("payment-failed", event.getOrderId().toString(), event);
	}
	
	

}
