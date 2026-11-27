package com.example.saga.event;

import com.example.saga.enums.DeliveryStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeliveryResponseEvent {
    private UUID orderId;
    private DeliveryStatus status;
}
