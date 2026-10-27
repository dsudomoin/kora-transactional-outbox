// java-library, publishing and shared dependencies are configured by the root build.
dependencies {
    api(project(":outbox-core"))
    api("io.micrometer:micrometer-core:1.12.13")

    testImplementation(testFixtures(project(":outbox-core")))
}
