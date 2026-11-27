package com.example.saga.order.entity;

import com.example.saga.enums.OrderStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID id;
    private String productId;
    private Integer quantity;
    private Double amount;
    private String address;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;
    private String message;
}
