// Plugins and shared dependencies are configured by the root build.
dependencies {
    api(project(":outbox-core"))
    api("org.apache.kafka:kafka-clients:3.9.2")

    testImplementation(testFixtures(project(":outbox-core")))
}
