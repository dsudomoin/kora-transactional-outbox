// java-library, publishing and shared dependencies are configured by the root build.
dependencies {
    api(project(":outbox-kafka"))
    api(project(":outbox-kora"))

    testImplementation(testFixtures(project(":outbox-kora")))
    testImplementation(testFixtures(project(":outbox-kafka")))
    testImplementation("ru.tinkoff.kora:config-hocon")
    testImplementation("ru.tinkoff.kora:test-junit5")
    testAnnotationProcessor("ru.tinkoff.kora:annotation-processors")
    testImplementation("io.opentelemetry:opentelemetry-sdk-trace:1.53.0")
}
