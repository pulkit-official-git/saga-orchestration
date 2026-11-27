package com.example.saga.inventory.service;

import com.example.saga.enums.InventoryStatus;
import com.example.saga.event.InventoryCancelEvent;
import com.example.saga.event.InventoryRequestEvent;
import com.example.saga.event.InventoryResponseEvent;
import com.example.saga.inventory.entity.Inventory;
import com.example.saga.inventory.repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class InventoryService {
    private final InventoryRepository inventoryRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "inventory-requests", groupId = "inventory-group")
    public void reserveInventory(InventoryRequestEvent event) {
        boolean isSuccess = event.getProductId() != null && !event.getProductId().startsWith("OUT_OF_STOCK");

        Inventory inventory = Inventory.builder()
                .orderId(event.getOrderId())
                .productId(event.getProductId())
                .allocatedQuantity(event.getQuantity())
                .status(isSuccess ? "ALLOCATED" : "FAILED")
                .build();
        inventoryRepository.save(inventory);

        InventoryStatus status = isSuccess ? InventoryStatus.SUCCESS : InventoryStatus.FAILED;
        InventoryResponseEvent responseEvent = new InventoryResponseEvent(event.getOrderId(), status);

        kafkaTemplate.send("inventory-responses", event.getOrderId().toString(), responseEvent);
    }

    @KafkaListener(topics = "inventory-cancels", groupId = "inventory-group")
    public void cancelInventory(InventoryCancelEvent event) {
        Inventory inventory = inventoryRepository.findByOrderId(event.getOrderId());
        if (inventory != null) {
            inventory.setStatus("RESTOCKED");
            inventoryRepository.save(inventory);
            System.out.println("Inventory restocked for order: " + event.getOrderId());
        }
    }
}
