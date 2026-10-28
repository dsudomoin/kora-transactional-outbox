package io.github.dsudomoin.outbox.kora;

/**
 * The tag of a second {@code JdbcConnectionFactory}, one that logs in as the role owning the outbox's tables: with
 * it, the outbox's DDL, session locks and recoveries run through it, and the app's own pool needs DML only (with
 * {@code outbox.schema.bootstrap = false}, the tables made by the app's migrations).
 * <pre>{@code
 * @Tag(OutboxOwner.class)
 * default JdbcDatabase outboxOwnerDatabase(Config config, ConfigValueExtractor<JdbcDatabaseConfig> extractor,
 *         DataBaseTelemetryFactory telemetry) {
 *     return new JdbcDatabase(extractor.extract(config.get("outboxOwnerDb")), telemetry);
 * }
 * }</pre>
 */
public final class OutboxOwner {

    private OutboxOwner() {}
}
