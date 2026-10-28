rootProject.name = "outbox-root"

include("outbox-core", "outbox-kafka", "outbox-admin-kora")
include("bench") // Kotlin benchmark app, not published
