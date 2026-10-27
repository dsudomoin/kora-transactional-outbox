// java-library, publishing and shared dependencies are configured by the root build; no Kora (verifyFrameworkFree).
dependencies {
    api(project(":outbox-core"))
    api("io.micrometer:micrometer-core:1.12.13")
    // ObservationOutboxTelemetry's tracing(...): the application brings micrometer-tracing and its bridge
    compileOnly("io.micrometer:micrometer-tracing:1.2.12")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")

    testImplementation(testFixtures(project(":outbox-core")))
    testImplementation("jakarta.annotation:jakarta.annotation-api:2.1.1")
    testImplementation("io.micrometer:micrometer-tracing-bridge-otel:1.2.12")
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing:1.31.0")
}
