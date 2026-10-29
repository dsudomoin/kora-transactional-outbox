val koraVersion: String by project

val published = listOf(
    "outbox-core", "outbox-kafka", "outbox-micrometer", "outbox-kora", "outbox-kafka-kora", "outbox-admin-kora",
)

configure(published.map { project(":$it") }) {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    repositories {
        mavenCentral()
        google { content { includeGroupByRegex("com\\.android\\.tools.*|androidx\\..*") } } // Metalava only
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
    // shown; -Dbench.* and -Doutbox.test.* properties of the Gradle invocation reach the test JVM. On CI (the CI
    // environment variable set) the tests tagged "timing", whose property is a speed a runner of unknown power may not
    // reach, are left out unless -Poutbox.test.timing asks for them
    val benchmark = project.hasProperty("benchmark")
    val timing = System.getenv("CI") == null || project.hasProperty("outbox.test.timing")
    tasks.withType<Test>().configureEach {
        useJUnitPlatform {
            if (benchmark) includeTags("benchmark") else excludeTags("benchmark")
            if (!timing) excludeTags("timing")
        }
        // outbox-kora holds most of the suite: its classes go round-robin to several JVMs, each with its own Postgres and
        // load tests that want cores of their own, so one JVM per 3 cores, at most 6. A small CI runner keeps one, as
        // before; --max-workers caps them too. Benchmarks keep the machine to themselves.
        maxParallelForks = if (benchmark || project.name != "outbox-kora") 1
            else (Runtime.getRuntime().availableProcessors() / 3).coerceIn(1, 6)
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

    // The public API as Metalava's signature of the main sources, without the internal and generated packages and
    // @InternalApi members: apiDump writes api/<module>.txt, apiCheck fails when the sources no longer match it
    val metalava = configurations.create("metalava") { isCanBeConsumed = false }
    dependencies { "metalava"("com.android.tools.metalava:metalava:1.0.0-alpha16") }
    val toolchains = the<JavaToolchainService>()
    val jdk = toolchains.launcherFor(the<JavaPluginExtension>().toolchain) // the JDK the API is compiled against
    val main = the<SourceSetContainer>()["main"]
    val sources = main.java.sourceDirectories // with the generated ones, so their tasks run first
    val compileClasspath = main.compileClasspath
    val excluded = listOf("internal", "admin.kora.api", "admin.kora.model").map { "io.github.dsudomoin.outbox.$it" }
    fun signatureTask(name: String, out: Provider<RegularFile>) = tasks.register<JavaExec>(name) {
        classpath = metalava
        mainClass = "com.android.tools.metalava.Driver"
        javaLauncher = toolchains.launcherFor { languageVersion = JavaLanguageVersion.of(21) } // Metalava needs 21
        inputs.files(sources).withPathSensitivity(PathSensitivity.RELATIVE)
        inputs.files(compileClasspath).withNormalizer(CompileClasspathNormalizer::class)
        outputs.file(out)
        argumentProviders.add {
            listOf(
                "main", "--format=4.0", "--api", out.get().asFile.path, "--java-source", "17",
                "--jdk-home", jdk.get().metadata.installationPath.asFile.path,
                "--source-path", sources.filter { it.exists() }.asPath,
                "--classpath", compileClasspath.filter { it.exists() }.asPath,
                "--stub-packages", (listOf("*") + excluded.map { "-$it.*" }).joinToString(":"),
                "--hide-annotation", "io.github.dsudomoin.outbox.InternalApi",
                "--hide", "AddedSealed", // Metalava does not model sealed types yet; they are listed as plain ones
            )
        }
        doLast { // Metalava lists a public member that names an excluded type instead of refusing it
            val leaks = out.get().asFile.readLines().filter { line -> excluded.any { "$it." in line } }
            if (leaks.isNotEmpty()) {
                throw GradleException("The public API names excluded types; mark these members @InternalApi:\n" +
                    leaks.joinToString("\n"))
            }
        }
    }
    val signature = layout.projectDirectory.file("api/${project.name}.txt")
    val apiDump = signatureTask("apiDump", provider { signature })
    apiDump { description = "Writes the public API to ${signature.asFile.name}" }
    val current = layout.buildDirectory.file("api/${project.name}.txt")
    val apiCheck = signatureTask("apiCheck", current)
    apiCheck {
        description = "Fails when the public API differs from ${signature.asFile.name} (apiDump updates it)"
        inputs.file(signature)
        mustRunAfter(apiDump)
        doLast {
            val expected = signature.asFile.readLines()
            val actual = current.get().asFile.readLines()
            if (actual != expected) { // shows the lines between the first and the last difference
                val head = expected.zip(actual).takeWhile { (e, a) -> e == a }.size
                val tail = expected.drop(head).asReversed().zip(actual.drop(head).asReversed())
                    .takeWhile { (e, a) -> e == a }.size
                val diff = expected.subList(head, expected.size - tail).map { "- $it" } +
                    actual.subList(head, actual.size - tail).map { "+ $it" }
                throw GradleException("${signature.asFile} differs from the public API at line ${head + 1}" +
                    " (./gradlew apiDump updates it):\n" + diff.joinToString("\n"))
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
