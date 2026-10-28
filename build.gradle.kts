import me.tylerbwong.gradle.metalava.extension.MetalavaExtension

plugins {
    id("me.tylerbwong.gradle.metalava") version "0.4.0-alpha03" apply false
}

val koraVersion: String by project

val published = listOf(
    "outbox-core", "outbox-kafka", "outbox-micrometer", "outbox-kora", "outbox-kafka-kora", "outbox-admin-kora",
)

configure(published.map { project(":$it") }) {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "me.tylerbwong.gradle.metalava")

    repositories {
        mavenCentral()
        google { content { includeGroupByRegex("com\\.android\\.tools.*") } } // Metalava only
    }

    configure<JavaPluginExtension> {
        toolchain.languageVersion = JavaLanguageVersion.of(17)
        withSourcesJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.12.2"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
        "testImplementation"("org.assertj:assertj-core:3.27.6")
        "testRuntimeOnly"("org.slf4j:slf4j-simple:2.0.17")
    }

    // -Pbenchmark runs only the tests tagged "benchmark" (never part of a normal build), always rerun, their stdout
    // shown; -Dbench.* and -Doutbox.test.* properties of the Gradle invocation reach the test JVM
    val benchmark = project.hasProperty("benchmark")
    tasks.withType<Test>().configureEach {
        useJUnitPlatform {
            if (benchmark) includeTags("benchmark") else excludeTags("benchmark")
        }
        maxParallelForks = 1
        if (benchmark) {
            outputs.upToDateWhen { false }
            testLogging.events("standard_out")
        }
        System.getProperties().forEach { (k, v) ->
            if (k.toString().startsWith("bench.") || k.toString().startsWith("outbox.test.")) {
                systemProperty(k.toString(), v)
            }
        }
    }

    // -Poutbox.test.kafkaClients, .micrometer, .micrometerTracing and .opentelemetry run the tests on that version:
    // forced on the test runtime classpath only, so the code still compiles against the declared versions
    val kafkaClients = findProperty("outbox.test.kafkaClients") as String?
    val micrometer = findProperty("outbox.test.micrometer") as String?
    val micrometerTracing = findProperty("outbox.test.micrometerTracing") as String?
    val opentelemetry = findProperty("outbox.test.opentelemetry") as String?
    configurations.named("testRuntimeClasspath") {
        resolutionStrategy.eachDependency {
            val forced = when {
                requested.group == "org.apache.kafka" && requested.name == "kafka-clients" -> kafkaClients
                requested.group == "io.micrometer" && requested.name.startsWith("micrometer-tracing") -> micrometerTracing
                requested.group == "io.micrometer" && requested.name.startsWith("micrometer-") -> micrometer
                // the -alpha artifacts are versioned apart from the stable ones
                requested.group == "io.opentelemetry" && requested.version?.endsWith("-alpha") != true -> opentelemetry
                else -> null
            }
            if (forced != null) useVersion(forced)
        }
    }

    configure<MetalavaExtension> {
        filename = "api/${project.name}.txt"
        hiddenPackages.addAll(
            "io.github.dsudomoin.outbox.internal",
            "io.github.dsudomoin.outbox.admin.kora.api",
            "io.github.dsudomoin.outbox.admin.kora.model",
        )
        hiddenAnnotations.add("io.github.dsudomoin.outbox.InternalApi")
        // the plugin finds no JDK classes on Java 9+ by itself
        val jdk = the<JavaToolchainService>().launcherFor(the<JavaPluginExtension>().toolchain)
        arguments.addAll(jdk.map { listOf("--jdk-home", it.metadata.installationPath.asFile.path) })
    }

    // Metalava's compatibility check passes additions; the checked-in signature must match the public API exactly
    val signature = layout.projectDirectory.file("api/${project.name}.txt")
    val current = layout.buildDirectory.file("metalava/current.txt") // written by metalavaCheckCompatibility
    val apiCheck by tasks.registering {
        description = "Fails when the public API differs from ${signature.asFile.name} (metalavaGenerateSignature)"
        dependsOn("metalavaCheckCompatibility")
        doLast {
            val expected = signature.asFile.readLines()
            val actual = current.get().asFile.readLines()
            if (actual != expected) { // shows the lines between the first and the last difference
                val head = expected.zip(actual).takeWhile { (e, a) -> e == a }.size
                val tail = expected.drop(head).asReversed().zip(actual.drop(head).asReversed())
                    .takeWhile { (e, a) -> e == a }.size
                val diff = expected.subList(head, expected.size - tail).map { "- $it" } +
                    actual.subList(head, actual.size - tail).map { "+ $it" }
                throw GradleException("${signature.asFile} differs from the public API at line ${head + 1}:\n" +
                    diff.joinToString("\n"))
            }
        }
    }
    tasks.named("check") { dependsOn(apiCheck) }

    configure<PublishingExtension> {
        publications.create<MavenPublication>("maven") {
            artifactId = project.name
            from(components["java"])
            pom {
                name = project.name
                description = if (project.name.endsWith("-kora")) {
                    "Transactional outbox for PostgreSQL — Kora 1.x adapter"
                } else {
                    "Transactional outbox for PostgreSQL"
                }
                url = "https://github.com/dsudomoin/outbox"
                licenses {
                    license {
                        name = "MIT License"
                        url = "https://opensource.org/licenses/MIT"
                    }
                }
                scm {
                    url = "https://github.com/dsudomoin/outbox"
                    connection = "scm:git:https://github.com/dsudomoin/outbox.git"
                }
            }
        }
    }
}

// Kora's platform and config processor reach the Kora adapters only: the core, Kafka and Micrometer modules stay
// framework-free (verifyFrameworkFree)
configure(listOf("outbox-kora", "outbox-kafka-kora", "outbox-admin-kora").map { project(":$it") }) {
    dependencies {
        "api"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "annotationProcessor"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "testAnnotationProcessor"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "annotationProcessor"("ru.tinkoff.kora:config-annotation-processor")
    }
}

// Fails when a framework reaches a framework-free module's compile or runtime classpath
mapOf(
    "outbox-core" to listOf("ru.tinkoff.kora", "io.micrometer", "io.opentelemetry"),
    "outbox-kafka" to listOf("ru.tinkoff.kora"),
    "outbox-micrometer" to listOf("ru.tinkoff.kora"),
).forEach { (name, banned) ->
    project(":$name") {
        val classpaths = listOf("compileClasspath", "runtimeClasspath").map { configurations[it] }
        val verifyFrameworkFree by tasks.registering {
            description = "Fails when ${banned.joinToString()} is on the compile or runtime classpath"
            doLast {
                classpaths.forEach { classpath ->
                    val found = classpath.incoming.resolutionResult.allComponents.mapNotNull { it.moduleVersion }
                        .filter { module -> banned.any { module.group.startsWith(it) } }
                    if (found.isNotEmpty()) {
                        throw GradleException("$path ${classpath.name} must stay framework-free: ${found.joinToString()}")
                    }
                }
            }
        }
        tasks.named("check") { dependsOn(verifyFrameworkFree) }
    }
}
