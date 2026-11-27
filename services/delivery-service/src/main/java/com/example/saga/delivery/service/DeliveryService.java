package com.example.saga.delivery.service;

import com.example.saga.delivery.entity.Delivery;
import com.example.saga.delivery.repository.DeliveryRepository;
import com.example.saga.enums.DeliveryStatus;
import com.example.saga.event.DeliveryRequestEvent;
import com.example.saga.event.DeliveryResponseEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DeliveryService {
    private final DeliveryRepository deliveryRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "delivery-requests", groupId = "delivery-group")
    public void arrangeDelivery(DeliveryRequestEvent event) {
        boolean isSuccess = event.getAddress() != null && !event.getAddress().contains("NOWHERE");

        Delivery delivery = Delivery.builder()
                .orderId(event.getOrderId())
                .address(event.getAddress())
                .status(isSuccess ? "SCHEDULED" : "FAILED")
                .build();
        deliveryRepository.save(delivery);

        DeliveryStatus status = isSuccess ? DeliveryStatus.SUCCESS : DeliveryStatus.FAILED;
        DeliveryResponseEvent responseEvent = new DeliveryResponseEvent(event.getOrderId(), status);

        kafkaTemplate.send("delivery-responses", event.getOrderId().toString(), responseEvent);
    }
}
