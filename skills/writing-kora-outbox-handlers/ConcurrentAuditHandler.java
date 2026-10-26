import io.github.dsudomoin.kora.outbox.OutboxHandler;
import io.github.dsudomoin.kora.outbox.OutboxPermanentException;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * POSTs audit records concurrently, at most IN_FLIGHT per handleBatch call; the rules it follows are in
 * concurrent-delivery.md. Not a @Component: build it inside the app's one OutboxHandler factory, e.g.
 * OutboxHandler.byTopic(Map.of("audit", new ConcurrentAuditHandler(url)), kafka.handler()). For Retry-After see
 * AuditHandler.java.
 */
public final class ConcurrentAuditHandler implements OutboxHandler {

    static final int IN_FLIGHT = 200;   // P per call: the target sees up to min(shards, instances × workers) × P
    static final int PROBES = 3;        // requests a call starts while the target looks down
    static final int MAX_ATTEMPTS = 20; // this route only; outbox.relay.retry.maxAttempts stays 0 for ordered topics
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1); // well below outbox.relay.batchTimeout

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final URI events;
    private volatile boolean targetDown; // the last call got no answer at all: probe before sending the rest

    public ConcurrentAuditHandler(URI baseUrl) {
        this.events = URI.create(baseUrl.toString().replaceAll("/+$", "") + "/events");
    }

    @Override
    public void handle(OutboxRecord record) throws Exception { // the relay calls handleBatch; this is for direct use
        check(record, http.send(request(record), HttpResponse.BodyHandlers.discarding()).statusCode());
    }

    /** One outcome per record, in order, never a thrown exception. */
    @Override
    @SuppressWarnings("unchecked")
    public List<Outcome> handleBatch(List<OutboxRecord> records) {
        int n = records.size();
        var decided = new Outcome[n]; // outcomes known without a finished request
        var sends = (CompletableFuture<HttpResponse<Void>>[]) new CompletableFuture<?>[n]; // null: never started
        var lastOfKey = new HashMap<Object, Integer>();
        var probes = new ArrayList<Integer>();
        var permits = new Semaphore(IN_FLIGHT);
        boolean probing = targetDown;
        try {
            for (int i = 0; i < n && !OutboxHandler.windDownRequested(); i++) {
                var record = records.get(i);
                Integer previous = record.key() == null ? null
                        : lastOfKey.put(Map.entry(record.topic(), ByteBuffer.wrap(record.key())), i);
                if (previous != null) { // one request per key in flight, in order
                    if (!await(sends[previous])) {
                        break;
                    }
                    var before = limited(records.get(previous), result(records.get(previous), decided[previous],
                            sends[previous]));
                    if (!(before instanceof Outcome.Delivered || before instanceof Outcome.Permanent)) {
                        decided[i] = Outcome.notAttempted(); // the key failed earlier in this call
                        continue;
                    }
                }
                if (probing && probes.size() == PROBES) { // breaker: no more requests until a probe is answered
                    if (!awaitAll(probes.stream().map(j -> sends[j]).toList())) {
                        break;
                    }
                    probing = probes.stream().noneMatch(j -> answered(result(records.get(j), decided[j], sends[j])));
                    if (probing) {
                        break; // still down: the rest are not attempted and keep their attempts
                    }
                }
                if (!acquire(permits)) {
                    break;
                }
                try {
                    sends[i] = http.sendAsync(request(record), HttpResponse.BodyHandlers.discarding());
                    sends[i].whenComplete((response, error) -> permits.release());
                } catch (RuntimeException e) {
                    permits.release();
                    decided[i] = Outcome.retry(e);
                }
                if (probing) {
                    probes.add(i);
                }
            }
            awaitAll(Arrays.asList(sends));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // the relay's soft deadline: wind down
        }
        var outcomes = new ArrayList<Outcome>(n);
        int answered = 0;
        int failed = 0;
        for (int i = 0; i < n; i++) {
            if (decided[i] == null && sends[i] != null && !sends[i].isDone()) { // cut off by the wind-down
                sends[i].cancel(true);
                decided[i] = Outcome.notAttempted(); // spends no attempt; a duplicate is caught by Idempotency-Key
            }
            var outcome = result(records.get(i), decided[i], sends[i]);
            if (answered(outcome)) {
                answered++;
            } else if (outcome instanceof Outcome.Retry) {
                failed++;
            }
            outcomes.add(limited(records.get(i), outcome));
        }
        if (answered + failed > 0) {
            targetDown = answered == 0;
        }
        return outcomes;
    }

    private HttpRequest request(OutboxRecord record) {
        return HttpRequest.newBuilder(events)
                .timeout(REQUEST_TIMEOUT)
                .header("Idempotency-Key", record.messageId().toString()) // stable across redeliveries
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(record.payload()))
                .build();
    }

    private static void check(OutboxRecord record, int status) throws IOException {
        if (status / 100 == 4 && status != 408 && status != 429) {
            throw new OutboxPermanentException("audit rejected " + record.messageId() + ": HTTP " + status);
        }
        if (status / 100 != 2) {
            throw new IOException("audit answered HTTP " + status); // 5xx, 408, 429: retried with backoff
        }
    }

    private static Outcome result(OutboxRecord record, Outcome decided, CompletableFuture<HttpResponse<Void>> send) {
        if (decided != null) {
            return decided;
        }
        if (send == null || !send.isDone()) {
            return Outcome.notAttempted(); // never started
        }
        try {
            check(record, send.join().statusCode());
            return Outcome.delivered();
        } catch (OutboxPermanentException e) {
            return Outcome.permanent(e);
        } catch (CompletionException e) {
            return Outcome.retry(e.getCause() != null ? e.getCause() : e); // timeout, connect failure
        } catch (IOException e) {
            return Outcome.retry(e);
        }
    }

    /** The target answered: it is up, whatever it said about this record. */
    private static boolean answered(Outcome outcome) {
        return outcome instanceof Outcome.Delivered || outcome instanceof Outcome.Permanent;
    }

    /** Dead-letters this route's records after MAX_ATTEMPTS failed deliveries. */
    private static Outcome limited(OutboxRecord record, Outcome outcome) {
        return outcome instanceof Outcome.Retry retry && record.attempts() + 1 >= MAX_ATTEMPTS
                ? Outcome.permanent(retry.error())
                : outcome;
    }

    /** Takes a permit, polling the wind-down; false: wind down now. */
    private static boolean acquire(Semaphore permits) throws InterruptedException {
        while (!permits.tryAcquire(100, TimeUnit.MILLISECONDS)) {
            if (OutboxHandler.windDownRequested()) {
                return false;
            }
        }
        return true;
    }

    /** Waits for the started requests, polling the wind-down; false: wind down now. */
    private static boolean awaitAll(List<? extends Future<?>> sends) throws InterruptedException {
        for (var send : sends) {
            if (!await(send)) {
                return false;
            }
        }
        return true;
    }

    private static boolean await(Future<?> send) throws InterruptedException {
        while (send != null && !send.isDone()) {
            if (OutboxHandler.windDownRequested()) {
                return false;
            }
            try {
                send.get(100, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException | CancellationException e) {
                // still running, or finished: its outcome is read later
            }
        }
        return true;
    }
}
