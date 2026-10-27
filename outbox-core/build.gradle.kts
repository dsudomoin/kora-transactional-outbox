plugins {
    `java-test-fixtures`
}

dependencies {
    api("ru.tinkoff.kora:database-jdbc")
    api("ru.tinkoff.kora:config-common")
    api("ru.tinkoff.kora:opentelemetry-common")
    api("io.micrometer:micrometer-core:1.12.13")
    // optional: ProtobufOutbox needs it on the app's classpath
    compileOnly("com.google.protobuf:protobuf-java:3.25.8")

    testFixturesApi("org.testcontainers:testcontainers-postgresql:2.0.5")
    testFixturesApi("org.postgresql:postgresql:42.7.14")

    testImplementation("com.google.protobuf:protobuf-java:3.25.8")
}

// test fixtures are for this repository's tests only, not part of the published artifact
(components["java"] as AdhocComponentWithVariants).run {
    withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
    withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
}
