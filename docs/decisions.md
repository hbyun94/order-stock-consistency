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
