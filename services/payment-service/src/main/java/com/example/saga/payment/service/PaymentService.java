package com.example.saga.payment.service;

import com.example.saga.enums.PaymentStatus;
import com.example.saga.event.PaymentCancelEvent;
import com.example.saga.event.PaymentRequestEvent;
import com.example.saga.event.PaymentResponseEvent;
import com.example.saga.payment.entity.Payment;
import com.example.saga.payment.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "payment-requests", groupId = "payment-group")
    public void processPayment(PaymentRequestEvent event) {
        boolean isSuccess = event.getAmount() <= 1000;
        
        Payment payment = Payment.builder()
                .orderId(event.getOrderId())
                .amount(event.getAmount())
                .status(isSuccess ? "APPROVED" : "REJECTED")
                .build();
        paymentRepository.save(payment);

        PaymentStatus status = isSuccess ? PaymentStatus.SUCCESS : PaymentStatus.FAILED;
        PaymentResponseEvent responseEvent = new PaymentResponseEvent(event.getOrderId(), status);
        
        kafkaTemplate.send("payment-responses", event.getOrderId().toString(), responseEvent);
    }

    @KafkaListener(topics = "payment-cancels", groupId = "payment-group")
    public void cancelPayment(PaymentCancelEvent event) {
        Payment payment = paymentRepository.findByOrderId(event.getOrderId());
        if (payment != null) {
            payment.setStatus("REFUNDED");
            paymentRepository.save(payment);
            System.out.println("Payment refunded for order: " + event.getOrderId());
        }
    }
}
