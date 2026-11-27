package com.example.saga.order.service;

import com.example.saga.dto.OrderRequestDto;
import com.example.saga.dto.OrderResponseDto;
import com.example.saga.enums.OrderStatus;
import com.example.saga.event.PaymentRequestEvent;
import com.example.saga.order.entity.Order;
import com.example.saga.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final KafkaOrderProducer kafkaProducer;

    public OrderResponseDto createOrder(OrderRequestDto request) {
        Order order = Order.builder()
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .amount(request.getAmount())
                .address(request.getAddress())
                .status(OrderStatus.PENDING)
                .message("Order created, initiating payment...")
                .build();
        
        order = orderRepository.save(order);

        kafkaProducer.sendPaymentRequest(new PaymentRequestEvent(order.getId(), order.getAmount()));

        return new OrderResponseDto(order.getId(), order.getStatus(), order.getMessage());
    }
}
