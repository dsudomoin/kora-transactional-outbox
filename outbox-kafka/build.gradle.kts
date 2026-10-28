// Plugins and shared dependencies are configured by the root build.
dependencies {
    api(project(":outbox-core"))
    api("org.apache.kafka:kafka-clients:3.9.2")

    testImplementation(testFixtures(project(":outbox-core")))
    testImplementation("org.testcontainers:testcontainers-kafka:2.0.5")
    testImplementation("ru.tinkoff.kora:config-hocon")
    testImplementation("ru.tinkoff.kora:test-junit5")
    testAnnotationProcessor("ru.tinkoff.kora:annotation-processors")
    testImplementation("io.opentelemetry:opentelemetry-sdk-trace:1.53.0")
}
