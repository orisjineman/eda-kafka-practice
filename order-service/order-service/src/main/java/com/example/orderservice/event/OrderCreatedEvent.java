package com.example.orderservice.event;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String orderId,
        String productName,
        int quantity,
        BigDecimal price,
        Instant occurredAt
) implements Serializable {
}
