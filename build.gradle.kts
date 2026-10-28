val koraVersion: String by project

val published = listOf(
    "outbox-core", "outbox-kafka", "outbox-micrometer", "outbox-kora", "outbox-kafka-kora", "outbox-admin-kora",
)

configure(published.map { project(":$it") }) {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    repositories {
        mavenCentral()
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
                url = "https://github.com/dsudomoin/kora-transactional-outbox"
                licenses {
                    license {
                        name = "MIT License"
                        url = "https://opensource.org/licenses/MIT"
                    }
                }
                scm {
                    url = "https://github.com/dsudomoin/kora-transactional-outbox"
                    connection = "scm:git:https://github.com/dsudomoin/kora-transactional-outbox.git"
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
