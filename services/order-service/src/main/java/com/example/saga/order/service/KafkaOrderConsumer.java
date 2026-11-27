package com.example.saga.order.service;

import com.example.saga.enums.*;
import com.example.saga.event.*;
import com.example.saga.order.entity.Order;
import com.example.saga.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class KafkaOrderConsumer {
    private final OrderRepository orderRepository;
    private final KafkaOrderProducer kafkaProducer;

    @KafkaListener(topics = "payment-responses", groupId = "order-group")
    public void handlePaymentResponse(PaymentResponseEvent event) {
        Optional<Order> orderOpt = orderRepository.findById(event.getOrderId());
        if (orderOpt.isPresent()) {
            Order order = orderOpt.get();
            if (event.getStatus() == PaymentStatus.SUCCESS) {
                order.setMessage("Payment successful. Requesting inventory...");
                orderRepository.save(order);
                kafkaProducer.sendInventoryRequest(new InventoryRequestEvent(order.getId(), order.getProductId(), order.getQuantity()));
            } else {
                order.setStatus(OrderStatus.CANCELLED);
                order.setMessage("Payment failed. Order cancelled.");
                orderRepository.save(order);
            }
        }
    }

    @KafkaListener(topics = "inventory-responses", groupId = "order-group")
    public void handleInventoryResponse(InventoryResponseEvent event) {
        Optional<Order> orderOpt = orderRepository.findById(event.getOrderId());
        if (orderOpt.isPresent()) {
            Order order = orderOpt.get();
            if (event.getStatus() == InventoryStatus.SUCCESS) {
                order.setMessage("Inventory reserved. Arranging delivery...");
                orderRepository.save(order);
                kafkaProducer.sendDeliveryRequest(new DeliveryRequestEvent(order.getId(), order.getAddress()));
            } else {
                kafkaProducer.sendPaymentCancel(new PaymentCancelEvent(order.getId()));
                order.setStatus(OrderStatus.CANCELLED);
                order.setMessage("Inventory reservation failed. Refunding payment and cancelling order.");
                orderRepository.save(order);
            }
        }
    }

    @KafkaListener(topics = "delivery-responses", groupId = "order-group")
    public void handleDeliveryResponse(DeliveryResponseEvent event) {
        Optional<Order> orderOpt = orderRepository.findById(event.getOrderId());
        if (orderOpt.isPresent()) {
            Order order = orderOpt.get();
            if (event.getStatus() == DeliveryStatus.SUCCESS) {
                order.setStatus(OrderStatus.COMPLETED);
                order.setMessage("Order completed successfully.");
                orderRepository.save(order);
            } else {
                kafkaProducer.sendInventoryCancel(new InventoryCancelEvent(order.getId()));
                kafkaProducer.sendPaymentCancel(new PaymentCancelEvent(order.getId()));
                order.setStatus(OrderStatus.CANCELLED);
                order.setMessage("Delivery failed. Restocking, refunding, and cancelling order.");
                orderRepository.save(order);
            }
        }
    }
}
