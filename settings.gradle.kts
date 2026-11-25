plugins {
    // 로컬에 JDK 25가 없으면 Gradle이 자동으로 내려받도록 한다 (툴체인 자동 프로비저닝)
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "order-stock-consistency"

include("order-service", "stock-service")
