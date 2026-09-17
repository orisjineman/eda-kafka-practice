# EDA 실습: Spring Boot + Kafka

주문(order-service)이 이벤트를 발행하면, 알림(notification-service)이 그걸 구독해서 반응하는
가장 단순한 이벤트 기반 아키텍처(EDA) 학습용 사이드 프로젝트.

실무에서 EDA/Kafka를 써본 적이 없어서, 직접 손으로 타이핑하면서 개념을 익히는 걸 목표로 함.

## 진행 상황

- [x] Kafka 로컬 인프라 (Docker Compose)
- [x] order-service: 주문 생성 시 `OrderCreatedEvent` 발행
- [ ] notification-service: 이벤트 구독 후 알림 처리 (다음 단계)
- [ ] 컨슈머 그룹 다중 인스턴스로 파티션 분배 관찰
- [ ] Dead Letter Topic

## 아키텍처

```
[클라이언트]
    | POST /orders
    v
[order-service] --(OrderCreatedEvent 발행)--> [Kafka: order-events 토픽]
                                                        |
                                                        v (구독 예정)
                                                [notification-service]
```

order-service는 notification-service의 존재를 전혀 모른다. Kafka 토픽에 이벤트만 던질 뿐.
나중에 다른 서비스가 같은 토픽을 구독해도 order-service 코드는 안 바뀐다는 게 핵심.

## 프로젝트 구조

```
eda-kafka-practice/
├── docker-compose.yml       # Kafka + Kafka UI
├── order-service/           # 이벤트 발행자 (완료)
└── notification-service/    # 이벤트 구독자 (예정)
```

## 사전 준비

- Docker (또는 OrbStack) 실행 중이어야 함
- Java 17, Gradle

## 실행 방법

**1. Kafka 인프라 띄우기**

```bash
docker-compose up -d
```

- Kafka: `localhost:9094` (호스트에서 접속용)
- Kafka UI: http://localhost:8090

**2. order-service 실행**

IntelliJ에서 `OrderServiceApplication` 실행 (포트 8080)

**3. 주문 생성 → 이벤트 발행 테스트**

```bash
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"productName":"기계식 키보드","quantity":1,"price":89000}'
```

Kafka UI(http://localhost:8090) → Topics → `order-events` 에서 실제로 메시지가 쌓였는지 확인.
컨슈머가 아직 없어도(notification-service 미구현) 토픽에 이벤트가 쌓이는 걸 볼 수 있음
— Kafka는 컨슈머 유무와 상관없이 메시지를 보관해두기 때문.

## 코드에서 눈여겨볼 부분

- **`kafkaTemplate.send(TOPIC, orderId, event)`** — key를 orderId로 준 이유: 같은 주문 관련
  이벤트가 여러 개 생기더라도 항상 같은 파티션으로 가서 순서가 보장되게 하기 위함.
- **`JacksonJsonSerializer`** 사용 (Spring Boot 4 / Jackson 3 기준). 예전 `JsonSerializer`는
  Jackson 2 기반이라 deprecated 상태 — Boot 4 프로젝트에서는 새 클래스로 가는 게 맞음.
- **`spring.json.add.type.headers: false`** — producer가 자기 패키지 경로를 헤더에 안 심게 함.
  이게 켜져있으면 consumer가 producer의 클래스 경로까지 알아야 해서 결합도가 생김.

## 트러블슈팅 노트

**`ClassNotFoundException: com.fasterxml.jackson.core.type.TypeReference`**
Spring Boot 4는 기본 JSON 라이브러리가 Jackson 3(`tools.jackson.*`)로 바뀜. 옛날 `JsonSerializer`/
`JsonDeserializer`는 Jackson 2(`com.fasterxml.jackson.*`) 기반이라 클래스패스에 없어서 발생.
→ `JacksonJsonSerializer`/`JacksonJsonDeserializer`(Jackson 3 기반, spring-kafka 4.0+)로 교체.

**Kafka UI에서 클러스터 "오프라인"으로 뜸**
`KAFKA_ADVERTISED_LISTENERS`를 `localhost`로만 등록해두면, 호스트(맥)에서 접속하는
order-service는 되지만 도커 네트워크 안에 있는 kafka-ui는 `localhost`를 자기 자신으로
착각해서 못 붙음. → 리스너를 내부용(`kafka:9092`, 컨테이너용)과 외부용
(`localhost:9094`, 호스트용)으로 분리해서 해결.

## 다음 단계

- notification-service 만들어서 `order-events` 토픽 구독
- 컨슈머 인스턴스 2개 띄워서 파티션이 어떻게 나눠 처리되는지 관찰
- 결제 서비스 추가해서 동일 이벤트를 여러 서비스가 구독하는 구조 실습
- Dead Letter Topic으로 처리 실패 메시지 격리