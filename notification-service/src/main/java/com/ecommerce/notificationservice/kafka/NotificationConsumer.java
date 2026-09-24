package com.ecommerce.notificationservice.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    @KafkaListener(
            topics = "order-created",
            groupId = "notification-service-group"
    )
    public void consumeOrderCreated(OrderCreatedEvent event) {

        System.out.println("======================================");
        System.out.println("NOTIFICATION SERVICE");
        System.out.println("ORDER CREATED");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Customer ID: " + event.getCustomerId());
        System.out.println("Amount: " + event.getAmount());
        System.out.println("Message: Order created successfully.");
        System.out.println("======================================");
    }


    @KafkaListener(
            topics = "payment-success",
            groupId = "notification-service-group"
    )
    public void consumePaymentSuccess(PaymentSuccessEvent event) {

        System.out.println("======================================");
        System.out.println("NOTIFICATION SERVICE");
        System.out.println("PAYMENT SUCCESS");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Payment ID: " + event.getPaymentId());
        System.out.println("Amount: " + event.getAmount());
        System.out.println("Payment Method: " + event.getPaymentMethod());
        System.out.println("Message: Payment successful.");
        System.out.println("======================================");
    }


    @KafkaListener(
            topics = "payment-failed",
            groupId = "notification-service-group"
    )
    public void consumePaymentFailed(PaymentFailedEvent event) {

        System.out.println("======================================");
        System.out.println("NOTIFICATION SERVICE");
        System.out.println("PAYMENT FAILED");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Payment ID: " + event.getPaymentId());
        System.out.println("Amount: " + event.getAmount());
        System.out.println("Reason: " + event.getReason());
        System.out.println("Message: Payment failed.");
        System.out.println("======================================");
    }


    @KafkaListener(
            topics = "delivery-created",
            groupId = "notification-service-group"
    )
    public void consumeDeliveryCreated(DeliveryCreatedEvent event) {

        System.out.println("======================================");
        System.out.println("NOTIFICATION SERVICE");
        System.out.println("DELIVERY CREATED");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Tracking ID: " + event.getTrackingId());
        System.out.println("Delivery Status: " + event.getDeliveryStatus());
        System.out.println("Message: Your order has been handed over for delivery.");
        System.out.println("======================================");
    }
}