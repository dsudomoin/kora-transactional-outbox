plugins {
    `java-test-fixtures`
}

// Plugins and shared dependencies are configured by the root build; no framework here (verifyFrameworkFree).
dependencies {
    api(project(":outbox-core"))
    api("org.apache.kafka:kafka-clients:3.9.2")
    implementation("org.slf4j:slf4j-api:2.0.17")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")

    testFixturesApi("org.testcontainers:testcontainers-kafka:2.0.5")

    testImplementation(testFixtures(project(":outbox-core")))
    testImplementation("jakarta.annotation:jakarta.annotation-api:2.1.1")
    testImplementation(project(":outbox-micrometer"))
    testImplementation("io.micrometer:micrometer-tracing-bridge-otel:1.2.12")
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing:1.31.0")
}

// test fixtures are for this repository's tests only, not part of the published artifact
(components["java"] as AdhocComponentWithVariants).run {
    withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
    withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
}
