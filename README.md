# EDA 실습: Spring Boot + Kafka

주문(order-service)이 이벤트를 발행하면, 알림(notification-service)이 그걸 구독해서 반응하는
가장 단순한 이벤트 기반 아키텍처(EDA) 학습용 사이드 프로젝트.

실무에서 EDA/Kafka를 써본 적이 없어서, 직접 손으로 타이핑하면서 개념을 익히는 걸 목표로 함.

## 진행 상황

- [x] Kafka 로컬 인프라 (Docker Compose)
- [x] order-service: 주문 생성 시 `OrderCreatedEvent` 발행
- [x] notification-service: 이벤트 구독 후 로그로 알림 처리
- [x] order-service → Kafka → notification-service 전체 파이프라인 동작 확인
- [ ] 컨슈머 그룹 다중 인스턴스로 파티션 분배 관찰
- [ ] 결제 서비스 추가해서 동일 이벤트를 여러 서비스가 구독하는 구조 실습
- [ ] Dead Letter Topic

## 아키텍처

```
[클라이언트]
    | POST /orders (order-service, :8080)
    v
[order-service] --(OrderCreatedEvent 발행)--> [Kafka: order-events 토픽] --(구독)--> [notification-service, :8081]
```

order-service는 notification-service의 존재를 전혀 모른다. Kafka 토픽에 이벤트만 던질 뿐.
나중에 다른 서비스가 같은 토픽을 구독해도 order-service 코드는 안 바뀐다는 게 핵심.

## 프로젝트 구조

```
eda-kafka-practice/
├── docker-compose.yml       # Kafka + Kafka UI
├── order-service/           # 이벤트 발행자 (완료)
└── notification-service/    # 이벤트 구독자 (완료)
```

## 사전 준비

- Docker (또는 OrbStack) 실행 중이어야 함
- Java 17, Gradle

## 실행 방법

**1. Kafka 인프라 띄우기**

```bash
docker-compose up -d
```

- Kafka: `localhost:9094` (호스트에서 접속용, 도커 네트워크 내부용은 `kafka:9092`)
- Kafka UI: http://localhost:8090

**2. order-service, notification-service 둘 다 실행**

IntelliJ에서 `OrderServiceApplication`(:8080), `NotificationServiceApplication`(:8081) 각각 실행.
둘 다 떠있어야 전체 흐름이 동작함 — order-service만 켜져있으면 이벤트는 발행되지만 아무도 안 받고,
notification-service만 켜져있으면 받을 이벤트 자체가 없음.

**3. 주문 생성 → 이벤트 발행 → 구독 확인**

```bash
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"productName":"기계식 키보드","quantity":1,"price":89000}'
```

- order-service 콘솔: 응답 리턴
- notification-service 콘솔: "이벤트 수신" 로그 실시간으로 찍힘
- http://localhost:8090 → Topics → `order-events` 에서 실제 메시지 확인 가능

notification-service를 껐다 켜보면, 꺼져있던 동안 못 받은 이벤트도 다시 켜지자마자 받아서
처리하는 걸 확인할 수 있음 (Kafka가 컨슈머 그룹의 offset을 기억하기 때문).

## 코드에서 눈여겨볼 부분

- **`kafkaTemplate.send(TOPIC, orderId, event)`** — key를 orderId로 준 이유: 같은 주문 관련
  이벤트가 여러 개 생기더라도 항상 같은 파티션으로 가서 순서가 보장되게 하기 위함.
- **`JacksonJsonSerializer` / `JacksonJsonDeserializer`** 사용 (Spring Boot 4 / Jackson 3 기준).
  예전 `JsonSerializer`/`JsonDeserializer`는 Jackson 2 기반이라 deprecated 상태.
- **`OrderCreatedEvent`를 두 서비스에 각각 따로 정의** — 실수가 아니라 의도적 선택. 서비스 간에
  클래스를 공유하면 결합도가 생겨서, EDA에서는 보통 이벤트 스키마(필드 구조)만 약속으로 공유하고
  각자 자기 코드베이스 안에 따로 정의함.
- **`spring.json.add.type.headers: false`** — producer가 자기 패키지 경로를 헤더에 안 심게 함.
  이게 켜져있으면 consumer가 producer의 클래스 경로까지 알아야 해서 결합도가 생김.
- **`@KafkaListener`** 하나로 "이 토픽 구독합니다" 선언 끝. poll 루프, 역직렬화, 메서드 호출까지
  스프링이 다 알아서 처리해줌.

## 트러블슈팅 노트

**`ClassNotFoundException: com.fasterxml.jackson.core.type.TypeReference` (order-service)**
Spring Boot 4는 기본 JSON 라이브러리가 Jackson 3(`tools.jackson.*`)로 바뀜. 옛날 `JsonSerializer`는
Jackson 2(`com.fasterxml.jackson.*`) 기반이라 클래스패스에 없어서 발생.
→ `JacksonJsonSerializer`(Jackson 3 기반, spring-kafka 4.0+)로 교체.

**`NoClassDefFoundError: tools/jackson/core/type/TypeReference` (notification-service)**
위와 반대 케이스. order-service는 Web 스타터가 Jackson 3를 자동으로 딸려오지만,
notification-service는 Kafka 의존성만 있어서 Jackson 자체가 클래스패스에 없었음.
→ `build.gradle`에 `implementation 'tools.jackson.core:jackson-databind'` 직접 추가.

**Kafka UI에서 클러스터 "오프라인"으로 뜸**
`KAFKA_ADVERTISED_LISTENERS`를 `localhost`로만 등록해두면, 호스트(맥)에서 접속하는
order-service는 되지만 도커 네트워크 안에 있는 kafka-ui는 `localhost`를 자기 자신으로
착각해서 못 붙음. → 리스너를 내부용(`kafka:9092`, 컨테이너용)과 외부용
(`localhost:9094`, 호스트용)으로 분리해서 해결.

**컨슈머 실행 직후 `NOT_COORDINATOR` 로그가 반복됨**
에러 아니고 정상 과정. 새로 생기는 컨슈머 그룹은 코디네이터가 정해지고 안정화되는 데 짧은 시간이
걸려서 그 사이에 나오는 과도기 로그. `Successfully joined group`으로 끝나면 정상 동작 중인 것.

**`.idea` 내부 설정 파일(`compiler.xml` 등)이 계속 Changes에 잡힘**
`.gitignore`에 `.idea/`를 넣었어도, 이미 한 번 커밋된 적 있는 파일은 소급 적용 안 됨.
→ `git rm -r --cached .idea` 로 추적만 해제 (로컬 파일은 그대로 유지).

## 다음 단계

- notification-service 컨슈머 인스턴스 2개 띄워서 파티션 분배 관찰 (지금은 파티션 1개라 토픽
  파티션 수부터 늘려야 함)
- 결제 서비스 추가해서 같은 `order-events` 토픽을 여러 서비스가 구독하는 구조 실습
- Dead Letter Topic으로 처리 실패 메시지 격리
- 컨슈머 인스턴스를 여러 개 띄운 상태에서 하나를 강제로 끄고 리밸런싱 관찰