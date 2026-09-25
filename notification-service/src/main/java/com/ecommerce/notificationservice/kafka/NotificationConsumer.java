package com.ecommerce.notificationservice.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    // ==========================================
    // ORDER CREATED
    // ==========================================

    @KafkaListener(
            topics = "order-created",
            groupId = "notification-service-group"
    )
    public void consumeOrderCreated(OrderCreatedEvent event) {

        System.out.println("======================================");
        System.out.println("NOTIFICATION SERVICE");
        System.out.println("ORDER CREATED");
        System.out.println("Customer ID: " + event.getCustomerId());
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Amount: Rs." + event.getAmount());
        System.out.println("Message: Your order has been created successfully.");
        System.out.println("======================================");
    }


    // ==========================================
    // PAYMENT SUCCESS
    // ==========================================

    @KafkaListener(
            topics = "payment-success",
            groupId = "notification-service-group"
    )
    public void consumePaymentSuccess(PaymentSuccessEvent event) {

        System.out.println("======================================");
        System.out.println("NOTIFICATION SERVICE");
        System.out.println("PAYMENT SUCCESS");
        System.out.println("Customer ID: " + event.getCustomerId());
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Payment ID: " + event.getPaymentId());
        System.out.println("Amount: Rs." + event.getAmount());
        System.out.println("Payment Method: " + event.getPaymentMethod());
        System.out.println("Message: Your payment of Rs."
                + event.getAmount()
                + " was successful.");
        System.out.println("======================================");
    }


    // ==========================================
    // PAYMENT FAILED
    // ==========================================

    @KafkaListener(
            topics = "payment-failed",
            groupId = "notification-service-group"
    )
    public void consumePaymentFailed(PaymentFailedEvent event) {

        System.out.println("======================================");
        System.out.println("NOTIFICATION SERVICE");
        System.out.println("PAYMENT FAILED");
        System.out.println("Customer ID: " + event.getCustomerId());
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Payment ID: " + event.getPaymentId());
        System.out.println("Amount: Rs." + event.getAmount());
        System.out.println("Reason: " + event.getReason());
        System.out.println("Message: Your payment could not be processed.");
        System.out.println("======================================");
    }


    // ==========================================
    // DELIVERY CREATED
    // ==========================================

    @KafkaListener(
            topics = "delivery-created",
            groupId = "notification-service-group"
    )
    public void consumeDeliveryCreated(DeliveryCreatedEvent event) {

        System.out.println("======================================");
        System.out.println("NOTIFICATION SERVICE");
        System.out.println("DELIVERY CREATED");
        System.out.println("Customer ID: " + event.getCustomerId());
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Tracking Number: " + event.getTrackingNumber());
        System.out.println("Delivery Address: " + event.getDeliveryAddress());
        System.out.println("Delivery Status: " + event.getDeliveryStatus());
        System.out.println("Message: Your order has been handed over for delivery.");
        System.out.println("======================================");
    }
}