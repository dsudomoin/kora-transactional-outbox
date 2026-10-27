// Plugins and shared dependencies are configured by the root build; no framework here (verifyFrameworkFree).
dependencies {
    api(project(":outbox-core"))
    api("org.apache.kafka:kafka-clients:3.9.2")
    implementation("org.slf4j:slf4j-api:2.0.17")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")

    testImplementation(testFixtures(project(":outbox-core")))
    testImplementation("jakarta.annotation:jakarta.annotation-api:2.1.1")
}
