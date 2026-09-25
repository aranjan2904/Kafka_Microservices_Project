package com.ecommerce.deliveryservice.service;

import java.util.UUID;

import org.springframework.stereotype.Service;

import com.ecommerce.deliveryservice.entity.Delivery;
import com.ecommerce.deliveryservice.kafka.DeliveryCreatedEvent;
import com.ecommerce.deliveryservice.kafka.DeliveryProducer;
import com.ecommerce.deliveryservice.kafka.PaymentSuccessEvent;
import com.ecommerce.deliveryservice.repository.DeliveryRepository;

@Service
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryProducer deliveryProducer;

    public DeliveryService(
            DeliveryRepository deliveryRepository,
            DeliveryProducer deliveryProducer) {

        this.deliveryRepository = deliveryRepository;
        this.deliveryProducer = deliveryProducer;
    }

    public void createDelivery(PaymentSuccessEvent event) {

       
        Delivery delivery = new Delivery();

        delivery.setOrderId(event.getOrderId());
        delivery.setCustomerId(event.getCustomerId());
        delivery.setDeliveryAddress("Bangalore");
        delivery.setTrackingNumber("TRK-" + UUID.randomUUID());

        
        delivery.setDeliveryStatus("CREATED");

    
        Delivery savedDelivery = deliveryRepository.save(delivery);

        
        DeliveryCreatedEvent deliveryEvent = new DeliveryCreatedEvent();
        
        deliveryEvent.setEventId("DEL-" + UUID.randomUUID());

        deliveryEvent.setEventType("DELIVERY_CREATED");

        deliveryEvent.setOrderId(savedDelivery.getOrderId());

        deliveryEvent.setCustomerId(
                savedDelivery.getCustomerId()
        );

        deliveryEvent.setTrackingNumber(savedDelivery.getTrackingNumber());

        deliveryEvent.setDeliveryAddress(savedDelivery.getDeliveryAddress());

        deliveryEvent.setDeliveryStatus(
                savedDelivery.getDeliveryStatus()
        );

        // 6. Publish event to Kafka
        deliveryProducer.sendDeliveryCreated(deliveryEvent);

        System.out.println("DeliveryCreated event sent");
    }
}