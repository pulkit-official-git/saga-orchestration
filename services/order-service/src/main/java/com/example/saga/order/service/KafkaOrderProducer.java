package com.example.saga.order.service;

import com.example.saga.event.*;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaOrderProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendPaymentRequest(PaymentRequestEvent event) {
        kafkaTemplate.send("payment-requests", event.getOrderId().toString(), event);
    }

    public void sendPaymentCancel(PaymentCancelEvent event) {
        kafkaTemplate.send("payment-cancels", event.getOrderId().toString(), event);
    }

    public void sendInventoryRequest(InventoryRequestEvent event) {
        kafkaTemplate.send("inventory-requests", event.getOrderId().toString(), event);
    }

    public void sendInventoryCancel(InventoryCancelEvent event) {
        kafkaTemplate.send("inventory-cancels", event.getOrderId().toString(), event);
    }

    public void sendDeliveryRequest(DeliveryRequestEvent event) {
        kafkaTemplate.send("delivery-requests", event.getOrderId().toString(), event);
    }
}
