package com.example.saga.payment.repository;

import com.example.saga.payment.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<Payment, UUID> {
    Payment findByOrderId(UUID orderId);
}
