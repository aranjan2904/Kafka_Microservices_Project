package com.ecommerce.orderservice.service;

import java.time.LocalDateTime;
import java.util.UUID;

import org.apache.kafka.common.Uuid;
import org.springframework.stereotype.Service;

import com.ecommerce.orderservice.dto.OrderRequest;
import com.ecommerce.orderservice.dto.OrderResponse;
import com.ecommerce.orderservice.entity.Order;
import com.ecommerce.orderservice.kafka.OrderCreatedEvent;
import com.ecommerce.orderservice.kafka.OrderProducer;
import com.ecommerce.orderservice.repository.OrderRepository;

@Service
public class OrderService {
	
	private final OrderRepository orderRepository;
	private final OrderProducer orderProducer;

	public OrderService(OrderRepository orderRepository, OrderProducer orderProducer) {
		super();
		this.orderRepository = orderRepository;
		this.orderProducer = orderProducer;
	}
	
	
	public OrderResponse createOrder(OrderRequest request) {
		
		Order order = new Order();
		
		order.setCustomerId(request.getCustomerId());
		order.setCustomerName(request.getCustomerName());
		order.setProductId(request.getProductId());
		order.setProductName(request.getProductName());
		order.setQuantity(request.getQuantity());
		order.setAmount(request.getAmount());
		order.setDeliveryAddress(request.getDeliveryAddress());
		order.setStatus("CREATED");
		
		Order savedOrder = orderRepository.save(order);
		
		//create kafka event
		OrderCreatedEvent event = new OrderCreatedEvent();
		
		event.setEventId(UUID.randomUUID().toString());
		event.setEventType("ORDER_CREATED");
		event.setOrderId(savedOrder.getId());
		event.setCustomerId(savedOrder.getCustomerId());
		event.setAmount(savedOrder.getAmount());
		event.setDeliveryAddress(savedOrder.getDeliveryAddress());
		event.setEventTime(LocalDateTime.now());
		
		
		//publish event to kafka
		orderProducer.sendOrderCreatedEvent(event);
		
		
		OrderResponse response = new OrderResponse();
		
		response.setOrderId(order.getId());
		response.setStatus(order.getStatus());
		
		return response;
	}
}
