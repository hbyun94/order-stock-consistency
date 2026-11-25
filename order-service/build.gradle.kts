import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    alias(libs.plugins.spring.boot)
}

dependencies {
    // Spring Boot BOM을 platform으로 가져와 라이브러리 버전을 Boot가 관리하게 한다
    implementation(platform(SpringBootPlugin.BOM_COORDINATES))

    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    // 스키마는 Flyway SQL 마이그레이션으로 관리한다 (src/main/resources/db/migration)
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    runtimeOnly("org.flywaydb:flyway-database-postgresql")
    // 주문 요청 멱등 키 저장소 (Lettuce 클라이언트)
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    runtimeOnly("org.postgresql:postgresql")
}

dependencies {
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    // @ServiceConnection: 테스트 컨테이너의 접속 정보를 Spring 설정에 자동 연결
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.testcontainers:testcontainers")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
