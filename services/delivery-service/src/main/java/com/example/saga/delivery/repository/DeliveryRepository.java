package com.example.saga.delivery.repository;

import com.example.saga.delivery.entity.Delivery;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface DeliveryRepository extends JpaRepository<Delivery, UUID> {
    Delivery findByOrderId(UUID orderId);
}
