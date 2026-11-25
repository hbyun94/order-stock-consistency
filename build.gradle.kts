plugins {
    // 플러그인 버전만 루트에서 해석하고, 실제 적용은 각 서비스 모듈에서 한다
    alias(libs.plugins.spring.boot) apply false
}

subprojects {
    apply(plugin = "java")

    group = "io.github.hbyun94"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(25)
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    // ./gradlew bootRun 실행 시 루트의 .env를 읽어 애플리케이션 프로세스의 환경 변수로 넘긴다.
    // (docker compose와 같은 .env를 공유해 DB 계정을 한 곳에서만 관리하기 위함)
    tasks.withType<JavaExec>().matching { it.name == "bootRun" }.configureEach {
        val envFile = rootProject.file(".env")
        if (envFile.exists()) {
            envFile.readLines()
                .map { it.trim() }
                .filter { it.isNotEmpty() && !it.startsWith("#") && it.contains("=") }
                .forEach { line ->
                    val (key, value) = line.split("=", limit = 2)
                    environment(key.trim(), value.trim())
                }
        }
    }
}
