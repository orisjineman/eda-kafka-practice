package com.example.notificationservice.listener;

import com.example.notificationservice.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    @KafkaListener(topics = "order-events", groupId = "notification-service")
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("이벤트 수신 - 주문번호: {}", event.orderId());
        log.info("상품: {} x {}개, 금액: {}", event.productName(), event.quantity(), event.price());
    }
}
