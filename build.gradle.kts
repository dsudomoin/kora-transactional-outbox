val koraVersion: String by project

configure(listOf(project(":outbox"), project(":outbox-kafka"), project(":outbox-admin"))) {
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

    tasks.withType<Test>().configureEach {
        useJUnitPlatform {
            excludeTags("benchmark")
        }
        maxParallelForks = 1
    }

    configure<PublishingExtension> {
        publications.create<MavenPublication>("maven") {
            artifactId = "kora-${project.name}"
            from(components["java"])
            pom {
                name = "kora-${project.name}"
                description = "Transactional outbox for Kora 1.x on PostgreSQL"
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
