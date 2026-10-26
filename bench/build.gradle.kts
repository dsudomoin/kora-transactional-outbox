plugins {
    application
    kotlin("jvm") version "1.9.25"
    id("com.google.devtools.ksp") version "1.9.25-1.0.20"
}

val koraVersion: String by project

repositories {
    mavenCentral()
}

val koraBom: Configuration by configurations.creating
configurations {
    ksp.get().extendsFrom(koraBom)
    implementation.get().extendsFrom(koraBom)
}

dependencies {
    koraBom(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
    ksp("ru.tinkoff.kora:symbol-processors")

    implementation(project(":outbox-kafka"))
    implementation("ru.tinkoff.kora:database-jdbc")
    implementation("ru.tinkoff.kora:config-hocon")
    implementation("ru.tinkoff.kora:logging-logback")
    implementation("ru.tinkoff.kora:http-server-undertow")
    implementation("ru.tinkoff.kora:micrometer-module")
    implementation("ru.tinkoff.kora:opentelemetry-tracing")
    implementation("ru.tinkoff.kora:opentelemetry-tracing-exporter-grpc")
    implementation("ru.tinkoff.kora:kafka")
    implementation("ru.tinkoff.kora:json-module")
    implementation("org.postgresql:postgresql:42.7.14")
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass = "io.github.dsudomoin.kora.outbox.bench.BenchAppKt"
    applicationDefaultJvmArgs = listOf("-Dfile.encoding=UTF-8")
}
