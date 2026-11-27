package com.example.saga.inventory.repository;

import com.example.saga.inventory.entity.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface InventoryRepository extends JpaRepository<Inventory, UUID> {
    Inventory findByOrderId(UUID orderId);
}
