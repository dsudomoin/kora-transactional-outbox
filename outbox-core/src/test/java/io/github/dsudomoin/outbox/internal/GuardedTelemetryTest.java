package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import org.junit.jupiter.api.Test;

class GuardedTelemetryTest {

    private static final List<OutboxHeader> HEADERS = List.of(new OutboxHeader("a", new byte[] {1}));
    private static final OutboxRecord RECORD =
            new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, HEADERS, Instant.EPOCH, 0, 0);
    private static final List<OutboxRecord> RECORDS = List.of(RECORD);

    @Test
    void throwingEnqueueLeavesHeadersUnchanged() {
        var enqueue = GuardedTelemetry.of(new Throwing()).enqueueStarted();
        assertThat(enqueue.headers(HEADERS)).isSameAs(HEADERS);
        assertThatCode(enqueue::finished).doesNotThrowAnyException();
        assertThatCode(() -> enqueue.failed(new IOException("x"))).doesNotThrowAnyException();

        var failingStart = GuardedTelemetry.of(new ThrowingStart()).enqueueStarted();
        assertThat(failingStart.headers(HEADERS)).isSameAs(HEADERS);
    }

    @Test
    void throwingDeliveryHandsRecordsUnchanged() {
        var delivery = GuardedTelemetry.of(new Throwing()).deliveryStarted(RECORDS, Instant.EPOCH);
        assertThat(delivery.handed(RECORDS)).isSameAs(RECORDS);
        assertThatCode(() -> delivery.finished(List.of(Outcome.delivered()))).doesNotThrowAnyException();
        assertThatCode(() -> delivery.failed(new IOException("x"))).doesNotThrowAnyException();

        var failingStart = GuardedTelemetry.of(new ThrowingStart()).deliveryStarted(RECORDS, Instant.EPOCH);
        assertThat(failingStart.handed(RECORDS)).isSameAs(RECORDS);
    }

    @Test
    void handedListOfAnotherSizeIsIgnored() {
        var shrinking = new OutboxTelemetry() {
            @Override
            public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
                return new Delivery() {
                    @Override
                    public List<OutboxRecord> handed(List<OutboxRecord> records) {
                        return List.of();
                    }
                };
            }
        };
        var delivery = GuardedTelemetry.of(shrinking).deliveryStarted(RECORDS, Instant.EPOCH);
        assertThat(delivery.handed(RECORDS)).isSameAs(RECORDS);
    }

    @Test
    void throwingEventIsSwallowed() {
        var t = GuardedTelemetry.of(new Throwing());
        assertThatCode(() -> {
            t.enqueued("orders");
            t.delivered("orders", Duration.ofMillis(1));
            t.retried("orders");
            t.deadLettered("orders");
            t.requeued("orders");
            t.stepFinished(Duration.ofMillis(1));
            t.shardPaused();
            t.deadLetterDecision("keep");
            t.deadLetterHookFailed();
            t.inboxOutsideWindow();
            t.releaseThread();
            t.gauge(OutboxGauge.SHARD_LAG, () -> 1);
        }).doesNotThrowAnyException();
    }

    @Test
    void callExceptionsPassThrough() throws Exception {
        var plain = new OutboxTelemetry() {
            @Override
            public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
                return new Delivery() {};
            }
        };
        var delivery = GuardedTelemetry.of(plain).deliveryStarted(RECORDS, Instant.EPOCH);
        var failure = new IllegalStateException("handler");
        try (var err = new CapturedStderr()) {
            assertThatThrownBy(() -> delivery.inRecordScope(RECORD, () -> {
                throw failure;
            })).isSameAs(failure);
            assertThat(err.text()).doesNotContain("WARN");
        }
    }

    @Test
    void callRunsOnceWhenScopeThrowsAfterIt() throws Exception {
        var calls = new AtomicInteger();
        var delivery = GuardedTelemetry.of(new ScopeThrowing(true)).deliveryStarted(RECORDS, Instant.EPOCH);
        assertThat(delivery.inRecordScope(RECORD, calls::incrementAndGet)).isEqualTo(1);
        assertThat(calls).hasValue(1);
    }

    @Test
    void callRunsOnceWhenScopeThrowsBeforeIt() throws Exception {
        var calls = new AtomicInteger();
        var delivery = GuardedTelemetry.of(new ScopeThrowing(false)).deliveryStarted(RECORDS, Instant.EPOCH);
        assertThat(delivery.inRecordScope(RECORD, calls::incrementAndGet)).isEqualTo(1);
        assertThat(calls).hasValue(1);
    }

    @Test
    void warnsAtMostOncePer10s() {
        var t = GuardedTelemetry.of(new Throwing());
        try (var err = new CapturedStderr()) {
            t.enqueued("orders");
            t.enqueued("orders");
            var text = err.text();
            assertThat(text.split("WARN", -1)).as("one WARN within 10 s").hasSize(2);
            assertThat(text).contains("enqueued");
        }
    }

    @Test
    void noneIsNotWrapped() {
        assertThat(GuardedTelemetry.of(OutboxTelemetry.NONE)).isSameAs(OutboxTelemetry.NONE);
    }

    @Test
    void guardedIsNotWrappedAgain() {
        var guarded = GuardedTelemetry.of(new Throwing());
        assertThat(GuardedTelemetry.of(guarded)).isSameAs(guarded);
    }

    /** Every hook throws; its enqueue and delivery throw from every method, their scopes before running the call. */
    private static class Throwing implements OutboxTelemetry {

        private static RuntimeException bug() {
            return new IllegalStateException("telemetry bug");
        }

        @Override
        public Enqueue enqueueStarted() {
            return new Enqueue() {
                @Override
                public List<OutboxHeader> headers(List<OutboxHeader> headers) {
                    throw bug();
                }

                @Override
                public void finished() {
                    throw bug();
                }

                @Override
                public void failed(Throwable error) {
                    throw bug();
                }
            };
        }

        @Override
        public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
            return new Delivery() {
                @Override
                public List<OutboxRecord> handed(List<OutboxRecord> records) {
                    throw bug();
                }

                @Override
                public <T> T inRecordScope(OutboxRecord record, Callable<T> call) {
                    throw bug();
                }

                @Override
                public void finished(List<Outcome> outcomes) {
                    throw bug();
                }

                @Override
                public void failed(Throwable error) {
                    throw bug();
                }
            };
        }

        @Override
        public void enqueued(String topic) {
            throw bug();
        }

        @Override
        public void delivered(String topic, Duration delay) {
            throw bug();
        }

        @Override
        public void retried(String topic) {
            throw bug();
        }

        @Override
        public void deadLettered(String topic) {
            throw bug();
        }

        @Override
        public void requeued(String topic) {
            throw bug();
        }

        @Override
        public void stepFinished(Duration duration) {
            throw bug();
        }

        @Override
        public void shardPaused() {
            throw bug();
        }

        @Override
        public void deadLetterDecision(String decision) {
            throw bug();
        }

        @Override
        public void deadLetterHookFailed() {
            throw bug();
        }

        @Override
        public void inboxOutsideWindow() {
            throw bug();
        }

        @Override
        public void releaseThread() {
            throw bug();
        }

        @Override
        public void gauge(OutboxGauge gauge, DoubleSupplier value) {
            throw bug();
        }
    }

    private static final class ThrowingStart implements OutboxTelemetry {
        @Override
        public Enqueue enqueueStarted() {
            throw new IllegalStateException("telemetry bug");
        }

        @Override
        public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
            throw new IllegalStateException("telemetry bug");
        }
    }

    /** A record scope that throws after running its call ({@code after}) or instead of running it. */
    private record ScopeThrowing(boolean after) implements OutboxTelemetry {
        @Override
        public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
            return new Delivery() {
                @Override
                public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
                    if (after) {
                        call.call();
                    }
                    throw new IllegalStateException("telemetry bug");
                }
            };
        }
    }
}
