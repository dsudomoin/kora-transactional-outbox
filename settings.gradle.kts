rootProject.name = "kora-outbox-root"

include("outbox", "outbox-kafka", "outbox-admin")
include("bench") // Kotlin benchmark app, not published
