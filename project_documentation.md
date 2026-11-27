# Saga Orchestration Project Documentation

## 1. Project Overview
This project implements the **Saga Pattern (Orchestration)** using **Apache Kafka** and **Spring Boot** to handle distributed transactions across multiple microservices. 

The system simulates an e-commerce order workflow involving four primary services:
1. **Order Service** (The Central Orchestrator)
2. **Payment Service**
3. **Inventory Service**
4. **Delivery Service**
5. **Common DTOs** (Shared Library)

## 2. Requirements & Business Logic
The system must guarantee data consistency across independent databases without using 2-Phase Commit (2PC) or distributed locks. It uses eventual consistency via compensation transactions.

### Workflow Requirements:
1. When a user creates an order, a payment request is initiated.
2. If payment succeeds, inventory allocation is requested.
3. If inventory allocation succeeds, delivery scheduling is requested.
4. If delivery is scheduled, the order is marked as `COMPLETED`.

### Compensation Requirements (Rollbacks):
1. If **Inventory Allocation fails**: The orchestrator must cancel the order and trigger a `PaymentCancelEvent` to refund the customer.
2. If **Delivery Scheduling fails**: The orchestrator must cancel the order, trigger an `InventoryCancelEvent` to restock the item, and trigger a `PaymentCancelEvent` to refund the customer.

---

## 3. Architecture & Complete Structure

```mermaid
graph TD
    UI[Client] -->|Create Order| OS[Order Service - 8081]
    
    subgraph Saga Orchestrator
        OS
    end
    
    subgraph Microservices
        PS[Payment Service - 8082]
        IS[Inventory Service - 8083]
        DS[Delivery Service - 8084]
    end
    
    subgraph Kafka Message Broker
        T1[(payment-requests)]
        T2[(payment-responses)]
        T3[(payment-cancels)]
        T4[(inventory-requests)]
        T5[(inventory-responses)]
        T6[(inventory-cancels)]
        T7[(delivery-requests)]
        T8[(delivery-responses)]
    end

    OS -->|Produces| T1
    T1 -->|Consumes| PS
    PS -->|Produces| T2
    T2 -->|Consumes| OS
    
    OS -->|Produces| T4
    T4 -->|Consumes| IS
    IS -->|Produces| T5
    T5 -->|Consumes| OS
    
    OS -->|Produces| T7
    T7 -->|Consumes| DS
    DS -->|Produces| T8
    T8 -->|Consumes| OS
```

---

## 4. Sequence Diagram (Saga Flow)

```mermaid
sequenceDiagram
    participant C as Client
    participant O as Order Service (Orchestrator)
    participant P as Payment Service
    participant I as Inventory Service
    participant D as Delivery Service

    C->>O: POST /orders
    O->>O: Save Order (PENDING)
    O->>P: 1. PaymentRequestEvent
    
    alt Payment Success
        P-->>O: PaymentResponseEvent (SUCCESS)
        O->>I: 2. InventoryRequestEvent
        
        alt Inventory Success
            I-->>O: InventoryResponseEvent (SUCCESS)
            O->>D: 3. DeliveryRequestEvent
            
            alt Delivery Success
                D-->>O: DeliveryResponseEvent (SUCCESS)
                O->>O: Update Order (COMPLETED)
                O-->>C: Order Completed
            else Delivery Fails
                D-->>O: DeliveryResponseEvent (FAILED)
                O->>I: InventoryCancelEvent (Rollback)
                O->>P: PaymentCancelEvent (Rollback)
                O->>O: Update Order (CANCELLED)
            end
            
        else Inventory Fails
            I-->>O: InventoryResponseEvent (FAILED)
            O->>P: PaymentCancelEvent (Rollback)
            O->>O: Update Order (CANCELLED)
        end
        
    else Payment Fails
        P-->>O: PaymentResponseEvent (FAILED)
        O->>O: Update Order (CANCELLED)
    end
```

---

## 5. Schema Diagram (Database Tables)

*Note: Each microservice has its own isolated database/schema.*

```mermaid
erDiagram
    orders {
        UUID id PK
        String productId
        Integer quantity
        Double amount
        String address
        String status "PENDING, COMPLETED, CANCELLED"
        String message
    }
    
    payments {
        UUID id PK
        UUID orderId FK
        Double amount
        String status "APPROVED, REJECTED, REFUNDED"
    }
    
    inventory_allocations {
        UUID id PK
        UUID orderId FK
        String productId
        Integer allocatedQuantity
        String status
    }
    
    deliveries {
        UUID id PK
        UUID orderId FK
        String address
        String status
    }

    orders ||--o{ payments : "triggers"
    orders ||--o{ inventory_allocations : "triggers"
    orders ||--o{ deliveries : "triggers"
```

---

## 6. Class Diagram (Core Entities)

```mermaid
classDiagram
    class Order {
        -UUID id
        -String productId
        -Integer quantity
        -Double amount
        -String address
        -OrderStatus status
        -String message
    }
    
    class Payment {
        -UUID id
        -UUID orderId
        -Double amount
        -String status
    }
    
    class Inventory {
        -UUID id
        -UUID orderId
        -String productId
        -Integer allocatedQuantity
        -String status
    }
    
    class Delivery {
        -UUID id
        -UUID orderId
        -String address
        -String status
    }
    
    class KafkaOrderConsumer {
        +handlePaymentResponse(event)
        +handleInventoryResponse(event)
        +handleDeliveryResponse(event)
    }
    
    class KafkaOrderProducer {
        +sendInventoryRequest(event)
        +sendDeliveryRequest(event)
        +sendPaymentCancel(event)
        +sendInventoryCancel(event)
    }
    
    Order "1" --> "1" KafkaOrderConsumer : Managed By
    KafkaOrderConsumer --> KafkaOrderProducer : Triggers Next Step
```
