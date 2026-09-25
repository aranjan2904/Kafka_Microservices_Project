package com.ecommerce.paymentservice.service;

import java.math.BigDecimal;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.ecommerce.paymentservice.entity.Payment;
import com.ecommerce.paymentservice.kafka.OrderCreatedEvent;
import com.ecommerce.paymentservice.kafka.PaymentFailedEvent;
import com.ecommerce.paymentservice.kafka.PaymentProducer;
import com.ecommerce.paymentservice.kafka.PaymentSuccessEvent;
import com.ecommerce.paymentservice.repository.PaymentRepository;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentProducer paymentProducer;

    public PaymentService(PaymentRepository paymentRepository,
                          PaymentProducer paymentProducer) {
        super();
        this.paymentRepository = paymentRepository;
        this.paymentProducer = paymentProducer;
    }

    public void processPayment(OrderCreatedEvent event) {

        // Idempotency check
        if (paymentRepository.existsByEventId(event.getEventId())) {
            System.out.println("Duplicate event ignored: " + event.getEventId());
            return;
        }

        Payment payment = new Payment();

        payment.setEventId(event.getEventId());
        payment.setOrderId(event.getOrderId());
        payment.setCustomerId(event.getCustomerId());
        payment.setAmount(event.getAmount());
        payment.setPaymentId("TXN-" + UUID.randomUUID());
        payment.setPaymentMethod("UPI");

        if (event.getAmount().compareTo(new BigDecimal("50000")) > 0) {
            payment.setPaymentStatus("FAILED");
        } else {
            payment.setPaymentStatus("SUCCESS");
        }

        Payment savedPayment = paymentRepository.save(payment);

        if ("SUCCESS".equals(savedPayment.getPaymentStatus())) {

            PaymentSuccessEvent successEvent = new PaymentSuccessEvent();

            successEvent.setEventId("PAY-" + UUID.randomUUID());
            successEvent.setEventType("PAYMENT_SUCCESS");
            successEvent.setOrderId(savedPayment.getOrderId());
            successEvent.setCustomerId(savedPayment.getCustomerId());
            successEvent.setAmount(savedPayment.getAmount());
            successEvent.setPaymentId(savedPayment.getPaymentId());
            successEvent.setPaymentMethod(savedPayment.getPaymentMethod());
            successEvent.setPaymentStatus(savedPayment.getPaymentStatus());

            paymentProducer.sendPaymentSuccess(successEvent);

            System.out.println("PaymentSuccess event sent");

        } else {

            PaymentFailedEvent failedEvent = new PaymentFailedEvent();

            failedEvent.setEventId("PAY-" + UUID.randomUUID());
            failedEvent.setEventType("PAYMENT_FAILED");
            failedEvent.setOrderId(savedPayment.getOrderId());
            failedEvent.setCustomerId(savedPayment.getCustomerId());
            failedEvent.setAmount(savedPayment.getAmount());
            failedEvent.setPaymentId(savedPayment.getPaymentId());
            failedEvent.setPaymentStatus(savedPayment.getPaymentStatus());
            failedEvent.setReason("INSUFFICIENT_FUNDS");

            paymentProducer.sendPaymentFailed(failedEvent);

            System.out.println("PaymentFailed event sent");
        }
    }
}