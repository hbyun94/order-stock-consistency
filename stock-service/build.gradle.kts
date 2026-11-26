import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    alias(libs.plugins.spring.boot)
}

dependencies {
    // Spring Boot BOM을 platform으로 가져와 라이브러리 버전을 Boot가 관리하게 한다
    implementation(platform(SpringBootPlugin.BOM_COORDINATES))

    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    // 스키마는 Flyway SQL 마이그레이션으로 관리한다 (src/main/resources/db/migration)
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    runtimeOnly("org.flywaydb:flyway-database-postgresql")
    // Redis 분산 락 (실험 1)
    implementation(libs.redisson.spring.boot.starter)
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

// 실험(@Tag("experiment"))은 오래 걸리고 결과를 기록하는 용도라 일반 test 작업에서 제외한다.
tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("experiment")
    }
}

// 실행: ./gradlew :stock-service:experiment  → 결과: build/experiment/
val experiment by tasks.registering(Test::class) {
    description = "동시성 실험을 실행하고 결과를 build/experiment/에 기록한다."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("experiment")
    }
    testLogging {
        showStandardStreams = true
    }
    // 입력이 같아도 매번 다시 측정한다
    outputs.upToDateWhen { false }
}
