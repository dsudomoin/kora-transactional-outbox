plugins {
    `java-test-fixtures`
}

// java-library, publishing and shared dependencies are configured by the root build.
dependencies {
    api(project(":outbox-core"))
    api("ru.tinkoff.kora:database-jdbc")
    api("ru.tinkoff.kora:config-common")
    api("ru.tinkoff.kora:opentelemetry-common")
    // optional: OutboxJsonModule / OutboxProtobufModule need them on the app's classpath
    compileOnly("ru.tinkoff.kora:json-common")
    compileOnly("com.google.protobuf:protobuf-java:3.25.8")

    testFixturesApi(testFixtures(project(":outbox-core")))
    testFixturesApi("ru.tinkoff.kora:database-jdbc")
    testFixturesApi("ru.tinkoff.kora:opentelemetry-common")

    testImplementation("ru.tinkoff.kora:config-hocon")
    testImplementation("ru.tinkoff.kora:test-junit5")
    testImplementation("ru.tinkoff.kora:json-common")
    testImplementation("com.google.protobuf:protobuf-java:3.25.8")
    testAnnotationProcessor("ru.tinkoff.kora:annotation-processors")
    testImplementation("io.opentelemetry:opentelemetry-sdk-trace:1.53.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing:1.53.0")
}

// test fixtures are for this repository's tests only, not part of the published artifact
(components["java"] as AdhocComponentWithVariants).run {
    withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
    withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
}
