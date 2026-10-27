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
        "api"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "annotationProcessor"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "testAnnotationProcessor"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "annotationProcessor"("ru.tinkoff.kora:config-annotation-processor")

        "testImplementation"(platform("org.junit:junit-bom:5.12.2"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
        "testImplementation"("org.assertj:assertj-core:3.27.6")
        "testRuntimeOnly"("org.slf4j:slf4j-simple:2.0.17")
    }

    // -Pbenchmark runs only the tests tagged "benchmark" (never part of a normal build), always rerun, their stdout
    // shown; -Dbench.* properties of the Gradle invocation reach the test JVM
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
            if (k.toString().startsWith("bench.")) systemProperty(k.toString(), v)
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
