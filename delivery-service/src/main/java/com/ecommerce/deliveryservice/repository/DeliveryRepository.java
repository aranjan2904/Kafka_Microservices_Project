package com.ecommerce.deliveryservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.ecommerce.deliveryservice.entity.Delivery;

public interface DeliveryRepository extends JpaRepository<Delivery, Long>{

	
}
