// java-library, publishing and shared dependencies are configured by the root build; no Kora (verifyFrameworkFree).
dependencies {
    api(project(":outbox-core"))
    api("io.micrometer:micrometer-core:1.12.13")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")

    testImplementation(testFixtures(project(":outbox-core")))
}
