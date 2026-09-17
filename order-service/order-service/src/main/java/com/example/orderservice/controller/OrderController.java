package com.example.orderservice.controller;

import com.example.orderservice.event.OrderCreatedEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final String TOPIC = "order-events";
    private final KafkaTemplate<String, OrderCreatedEvent>  kafkaTemplate;

    public OrderController(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody OrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId, request.productName(), request.quantity(), request.price(), Instant.now()
        );
        kafkaTemplate.send(TOPIC, orderId, event);
        return  ResponseEntity.ok().body("주문 생성 완료, orderId=" + orderId);
    }

    record OrderRequest(String productName, int quantity, BigDecimal price) {
    }
}
