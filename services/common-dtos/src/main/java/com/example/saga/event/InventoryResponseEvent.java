package com.example.saga.event;

import com.example.saga.enums.InventoryStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryResponseEvent {
    private UUID orderId;
    private InventoryStatus status;
}
