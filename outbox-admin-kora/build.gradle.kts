// java-library, publishing and shared dependencies are configured by the root build.
buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath("ru.tinkoff.kora:openapi-generator:${project.property("koraVersion")}")
    }
}

plugins {
    id("org.openapi.generator") version "7.14.0"
}

openApiGenerate {
    generatorName = "kora"
    inputSpec = "$projectDir/src/main/resources/openapi/kora-outbox-admin.yaml"
    outputDir = layout.buildDirectory.dir("generated/openapi").get().asFile.path
    apiPackage = "io.github.dsudomoin.outbox.admin.kora.api"
    modelPackage = "io.github.dsudomoin.outbox.admin.kora.model"
    configOptions = mapOf(
        "mode" to "java-server",
        "interceptors" to """{"*":[{"type":"io.github.dsudomoin.outbox.admin.kora.OutboxAdminInterceptor"}]}""",
    )
}

sourceSets.main {
    java.srcDir(tasks.openApiGenerate.map { it.outputDir }) // compileJava and sourcesJar depend on the generation
}

// The test app's own API, contract-first as a service's is, with its own security scheme, models and mappers
val openApiGenerateTestApp by tasks.registering(org.openapitools.generator.gradle.plugin.tasks.GenerateTask::class) {
    generatorName = "kora"
    inputSpec = "$projectDir/src/test/resources/openapi/test-app.yaml"
    outputDir = layout.buildDirectory.dir("generated/openapi-test-app").get().asFile.path
    apiPackage = "io.github.dsudomoin.outbox.admin.kora.testapp.api"
    modelPackage = "io.github.dsudomoin.outbox.admin.kora.testapp.model"
    configOptions = mapOf("mode" to "java-server")
}

sourceSets.test {
    java.srcDir(openApiGenerateTestApp.map { it.outputDir }) // compileTestJava depends on the generation
}

tasks.sourcesJar {
    exclude(".openapi-generator*", ".openapi-generator/**") // the generator's bookkeeping, not sources
}

tasks.test {
    // Kora's generated $AdminTestAppImpl inherits JsonModule's Kafka factories; JUnit's discovery reflects on them and
    // fails without kafka-clients on the classpath. Generated classes hold no tests.
    exclude("**/\$*")
}

dependencies {
    api(project(":outbox-core"))
    api("ru.tinkoff.kora:http-server-common")
    api("ru.tinkoff.kora:json-common")
    annotationProcessor("ru.tinkoff.kora:annotation-processors")

    testImplementation(testFixtures(project(":outbox-core")))
    testImplementation("ru.tinkoff.kora:http-server-undertow")
    testImplementation("ru.tinkoff.kora:json-module")
    testImplementation("ru.tinkoff.kora:openapi-management")
    testImplementation("ru.tinkoff.kora:config-hocon")
    testImplementation("ru.tinkoff.kora:test-junit5")
    testAnnotationProcessor("ru.tinkoff.kora:annotation-processors")
}
