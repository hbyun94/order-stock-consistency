# 설계 결정 기록

프로젝트를 진행하며 내린 설계 결정을 한 건씩 기록한다.
각 항목은 **결정 / 선택지 / 이유**로 구성하고, 나중에 바뀌면 지우지 않고 새 항목으로 덮어쓴 이유를 남긴다.

---

## 1단계: 프로젝트 뼈대

### D-001. 버전 정책: "패치가 한 번 이상 나온 최신 마이너"를 쓴다 (2026-11-25)

- **결정**: 각 제품의 최신 마이너 중 패치가 한 번 이상 나온 버전을 쓴다. 단, Java는 LTS를 쓴다.
- **선택지**
  - A. 무조건 최신 (Spring Boot 4.2.0, PostgreSQL 19.0, Redis 8.12.0, Kafka 4.4.0)
  - B. 패치가 한 번 이상 나온 최신 마이너 ← 선택
- **이유**
  - Spring Boot, PostgreSQL, Redis, Kafka는 OSS에서 LTS 개념이 없다. 그래서 "LTS"를 고를 수 없고 안정성 기준이 따로 필요했다.
  - `.0` 릴리스는 초기 버그 위험이 있고, 주변 라이브러리(Redisson 등)가 아직 따라오지 못했을 수 있다.
  - 원칙이 하나라서 버전마다 따로 설명할 필요가 없다.
- **확정 버전** (2026-11-25 Maven Central / Docker Hub 기준)

  | 항목 | 버전 | 비고 |
  |---|---|---|
  | Java | 25 (LTS) | Gradle 툴체인으로 고정 |
  | Spring Boot | 4.1.3 | 최신 4.2.0은 출시 6일째라 제외 |
  | Gradle | 9.9.2 | wrapper |
  | Testcontainers, Kafka 클라이언트, JDBC 드라이버 등 | Spring Boot BOM 관리 버전 | 직접 덮어쓰지 않는다 (Boot가 검증한 조합) |
  | Kafka (브로커) | apache/kafka:4.3.2 | 클라이언트(Boot 관리 4.2.x)보다 브로커가 새 버전인 것은 Kafka 호환 정책상 문제없음 |
  | PostgreSQL | 18.7 | |
  | Redis | 8.10.3 | |
  | Kafka UI | kafbat/kafka-ui:v1.5.1 | 기존 provectuslabs/kafka-ui는 2024년 이후 업데이트 없음 → 유지되는 포크 사용 |

### D-002. 빌드 도구: Gradle Kotlin DSL + 버전 카탈로그 (2026-11-25)

- **결정**: Gradle Kotlin DSL(`*.gradle.kts`)과 `gradle/libs.versions.toml`을 쓴다.
- **선택지**: Maven / Gradle Groovy DSL / Gradle Kotlin DSL ← 선택
- **이유**
  - 빌드 스크립트도 타입 체크와 IDE 자동완성을 받는다.
  - 멀티 모듈 공통 설정(Java 버전, 테스트 설정)을 루트 `subprojects {}` 한 곳에 둘 수 있다.
  - 버전 카탈로그에는 Spring Boot 버전만 고정하고, 나머지 라이브러리 버전은 Boot BOM(`platform(...)`)에 맡긴다.

### D-003. 실행 방식: Compose에는 인프라만, 서비스는 로컬 `bootRun` (2026-11-25)

- **결정**: `docker-compose.yml`에는 Kafka, Kafka UI, Redis, PostgreSQL 2개만 둔다. order-service와 stock-service는 `./gradlew :<모듈>:bootRun`으로 로컬에서 실행한다.
- **선택지**
  - A. 인프라만 Compose, 서비스는 로컬 실행 ← 선택
  - B. 서비스도 Docker 이미지로 만들어 Compose에 포함
- **이유**
  - 코드를 고친 뒤 이미지를 다시 빌드할 필요 없이 바로 다시 실행할 수 있다.
  - 동시성 실험 중에 디버거를 붙이거나 로그를 보기 쉽다.
  - 테스트는 어차피 Testcontainers로 인프라를 따로 띄우므로 Compose는 로컬 수동 확인용이다.
- **결과**: `bootRun`이 루트 `.env`를 읽어 환경 변수로 넘기도록 설정했다. Compose와 애플리케이션이 같은 `.env`를 공유한다.

### D-004. 기본 패키지: `io.github.hbyun94` (2026-11-25)

- **결정**: `io.github.hbyun94.order`, `io.github.hbyun94.stock`
- **이유**: 개인 도메인이 없을 때 GitHub 계정을 역도메인으로 쓰는 관례(`io.github.<계정>`)를 따랐다. 전역에서 이름이 겹치지 않는다.

### D-005. 서비스별 DB 분리: PostgreSQL 컨테이너 2개 (2026-11-25)

- **결정**: orderdb(호스트 포트 5433)와 stockdb(5434)를 별도 컨테이너로 띄운다.
- **이유**: 한 인스턴스에 스키마만 나누는 방식도 가능하다. 하지만 이 프로젝트의 핵심은 "두 서비스가 하나의 DB 트랜잭션으로 묶일 수 없다"는 조건이다. 그래서 인스턴스를 물리적으로 분리해 서비스가 상대 DB에 직접 접근할 여지를 없앴다.
- **보류**: 공유 이벤트 DTO 모듈(common)은 5단계(Kafka 이벤트 흐름)에서 결정한다.

---

## 2단계: stock-service 재고 도메인

### D-006. 재고 저장 구조: 재고 테이블 + 주문별 예약 테이블 (2026-11-25)

- **결정**: `stock(product_id, on_hand, reserved)`와 `stock_reservation(order_id UNIQUE, product_id, quantity, status)`를 함께 쓴다. 가용 재고는 `on_hand - reserved`로 계산한다.
- **선택지**
  - A. 재고 테이블 하나에 수량만 관리
  - B. 재고 테이블 + 예약 테이블 ← 선택
  - C. 입출고 원장(ledger)만 쌓고 합계로 계산
- **이유**
  - A는 "주문 X가 몇 개를 예약했는지"가 남지 않는다. 그래서 확정이나 복원 때 수량을 외부 입력에 의존해야 하고, 같은 주문의 중복 확정·복원을 막을 수 없다.
  - B는 예약 기록의 상태 전이(RESERVED에서만 확정·복원 가능)로 중복 처리를 막는다. `order_id UNIQUE`는 6단계 중복 소비 방지의 기반이 된다.
  - C는 재고를 확인할 때마다 합계를 내야 해서, 이 프로젝트의 중심인 "한 행에 대한 동시 갱신과 락 비교"와 맞지 않는다.
- **구현 메모**
  - StockReservation은 Stock과 `@ManyToOne`을 맺지 않고 `productId` 값만 가진다. 둘은 변경 이유가 다른 별도 단위이고, 함께 바꾸는 일은 서비스 트랜잭션이 맡는다.
  - DB에 `CHECK (reserved <= on_hand)`를 두었다. 이 제약은 마지막 방어선일 뿐이다. 두 트랜잭션이 같은 값을 읽고 덮어쓰는 lost update는 제약 위반 없이 일어나므로 막지 못한다. 3단계 실험에서 이 차이를 확인한다.

### D-007. 주문 한 건 = 상품 한 종류 (2026-11-25)

- **결정**: 주문은 상품 하나와 수량만 가진다.
- **선택지**: A. 단일 상품 ← 선택 / B. 여러 상품
- **이유**: 실험의 초점인 "한정 재고 한 상품에 주문이 몰리는 상황"이 선명하게 드러난다. 여러 상품이면 부분 예약 실패 처리와, 비관적 락에서 여러 행을 잠그는 순서에 따른 데드락까지 다뤄야 해서 범위가 커진다.

### D-008. "복원"은 예약 해제만 의미한다 (2026-11-25)

- **결정**: 복원(release)은 RESERVED → RELEASED 전이와 `reserved` 감소만 한다. 출고 확정된 예약은 되돌리지 않는다.
- **선택지**: A. 예약 해제만 ← 선택 / B. 출고 확정 후 반품 복원까지
- **이유**: 요구사항의 보상 처리(재고 부족 또는 주문 취소 → 예약 취소)와 정확히 맞는다. 반품 흐름은 범위 밖이다.

### D-009. 업무 규칙은 엔티티 메서드에 둔다 (2026-11-25)

- **결정**: `Stock.reserve/confirm/release`, `StockReservation.confirm/release`가 규칙을 직접 검사한다. `StockService`는 조회, 엔티티 메서드 호출, 트랜잭션 경계만 맡는다.
- **선택지**: A. 엔티티 메서드 ← 선택 / B. 서비스 계층(엔티티는 getter/setter만)
- **이유**: 규칙이 한 곳에 모이고, DB와 Spring 없이 순수 JUnit으로 빠르게 단위 테스트할 수 있다. setter를 열지 않아서 규칙을 우회한 상태 변경이 불가능하다.

### D-010. 스키마 관리: Flyway + `ddl-auto: validate` (2026-11-25)

- **결정**: 테이블은 `db/migration/V{n}__*.sql`로 만들고, Hibernate는 엔티티와 테이블이 일치하는지 검사만 한다.
- **선택지**: A. Flyway + validate ← 선택 / B. `ddl-auto: create`/`update`
- **이유**
  - UNIQUE, CHECK, FK 같은 제약을 SQL로 의도대로 통제할 수 있다.
  - 실험할 때마다 같은 스키마가 보장된다.
  - 매핑 실수(컬럼명, 타입)는 기동 시점에 바로 실패한다.
  - order-service는 테이블이 생기는 4단계에서 같은 방식으로 적용한다.

### D-011. JPA 기본 설정: OSIV 끄기, SEQUENCE ID (2026-11-25)

- **결정**: 두 서비스 모두 `spring.jpa.open-in-view: false`로 둔다. ID는 `GenerationType.SEQUENCE`(`allocationSize = 50`, DB 시퀀스 `INCREMENT BY 50`과 일치)로 생성한다.
- **이유**
  - **OSIV:** 기본값 true는 HTTP 응답이 끝날 때까지 DB 커넥션을 붙잡는다. 동시 요청 실험에서 커넥션 풀 고갈의 원인이 될 수 있고, 트랜잭션 경계를 서비스 계층으로 명확히 하는 편이 좋다.
  - **SEQUENCE:** IDENTITY는 INSERT를 실행해야 ID를 알 수 있어서 Hibernate가 배치 INSERT를 하지 못한다. SEQUENCE는 ID를 50개씩 미리 받아 와서 시퀀스 호출을 줄이고 배치도 가능하다.
