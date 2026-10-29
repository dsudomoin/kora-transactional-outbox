import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.Outcome;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * POSTs the records of topic "audit" to an audit API; every other topic goes to the app's OutboxHandler (Kafka).
 * In Kora make it a @Component (with a constructor taking its config); without Kora
 * OutboxRuntime.builder(config, transactions, kafka.handler()).route(new AuditHandler(url)).
 *
 * handle() alone is enough for status mapping (the default handleBatch does the rest). handleBatch is overridden
 * here only to honour Retry-After; copy that part only if the target sends it.
 */
public final class AuditHandler implements OutboxTopicHandler {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5); // well below outbox.relay.batchTimeout

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final URI events;

    public AuditHandler(URI baseUrl) {
        this.events = URI.create(baseUrl.toString().replaceAll("/+$", "") + "/events"); // keeps a base path
    }

    @Override
    public Set<String> topics() {
        return Set.of("audit");
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        var request = HttpRequest.newBuilder(events)
                .timeout(REQUEST_TIMEOUT)
                .header("Idempotency-Key", record.messageId().toString()) // stable across redeliveries
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(record.payload()))
                .build();
        var response = http.send(request, HttpResponse.BodyHandlers.discarding());
        int code = response.statusCode();
        if (code / 100 == 2) {
            return;
        }
        if (code == 429 || code == 503) {
            throw new RetryAfter(code, Instant.now().plusSeconds(retryAfterSeconds(response)));
        }
        if (code / 100 == 4 && code != 408) {
            throw new OutboxPermanentException("audit rejected " + record.messageId() + ": HTTP " + code);
        }
        throw new IOException("audit answered HTTP " + code); // 5xx, 408: retried with backoff
    }

    /** Same contract as the default: one outcome per record, in order, and never a thrown exception. */
    @Override
    public List<Outcome> handleBatch(List<OutboxRecord> records) {
        var outcomes = new ArrayList<Outcome>(records.size());
        var failedKeys = new HashSet<Object>();
        int failures = 0;
        boolean anyDelivered = false;
        Instant targetPausedUntil = null;
        for (var record : records) {
            Object key = record.key() == null ? null : Map.entry(record.topic(), ByteBuffer.wrap(record.key()));
            if (targetPausedUntil != null) {
                outcomes.add(Outcome.notAttempted(targetPausedUntil)); // spends no attempt
                continue;
            }
            if (failedKeys.contains(key) || (!anyDelivered && failures >= 3) || OutboxHandler.windDownRequested()) {
                outcomes.add(Outcome.notAttempted());
                continue;
            }
            try {
                handle(record);
                outcomes.add(Outcome.delivered());
                anyDelivered = true;
            } catch (OutboxPermanentException e) {
                outcomes.add(Outcome.permanent(e)); // to <table>_dead; the key continues
            } catch (RetryAfter e) {
                outcomes.add(Outcome.retry(e, e.notBefore)); // capped at 24 h by the relay
                targetPausedUntil = e.notBefore; // the whole target asked to wait
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                outcomes.add(Outcome.retry(e));
                failures++;
                if (key != null) { // unkeyed records never block each other
                    failedKeys.add(key);
                }
            }
        }
        return outcomes;
    }

    private static long retryAfterSeconds(HttpResponse<?> response) {
        try {
            return response.headers().firstValueAsLong("Retry-After").orElse(30);
        } catch (NumberFormatException e) {
            return 30; // the HTTP-date form
        }
    }

    private static final class RetryAfter extends IOException {
        final Instant notBefore;

        RetryAfter(int code, Instant notBefore) {
            super("audit answered HTTP " + code + ", retry after " + notBefore);
            this.notBefore = notBefore;
        }
    }
}
