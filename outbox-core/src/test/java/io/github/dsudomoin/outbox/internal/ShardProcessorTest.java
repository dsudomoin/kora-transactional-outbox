package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.Ticks.Tick;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * The batch target choice (the newest tick whose id high-water mark fits the volume budget), and
 * the guard that keeps a soft-deadline interrupt inside the handler call it was aimed at.
 */
class ShardProcessorTest {

    @Test
    void choosesTheNewestTickThatFits() {
        var candidates = ticks(1, 150L, 200L, 250L);

        assertThat(ShardProcessor.chooseTarget(100L, candidates, 100).id()).isEqualTo(2);
        assertThat(ShardProcessor.chooseTarget(100L, candidates, 150).id()).isEqualTo(3);
        assertThat(ShardProcessor.chooseTarget(100L, candidates, 1_000_000).id()).isEqualTo(3);
    }

    @Test
    void firstTickIsChosenEvenWhenItDoesNotFit() {
        assertThat(ShardProcessor.chooseTarget(0L, ticks(1, 5000L, 5001L), 100).id()).isEqualTo(1);
        assertThat(ShardProcessor.chooseTarget(0L, ticks(1, 5000L), 100).id()).isEqualTo(1);
    }

    @Test
    void unknownHighWaterMarksStopTheMerge() {
        // ticks made before the upgrade (or by an old instance during a rolling deploy) carry no max_id
        assertThat(ShardProcessor.chooseTarget(null, ticks(1, 10L, 20L), 100).id()).isEqualTo(1);
        assertThat(ShardProcessor.chooseTarget(null, ticks(1, null, null), 100).id()).isEqualTo(1);
        assertThat(ShardProcessor.chooseTarget(0L, ticks(1, 10L, null, 30L), 100).id()).isEqualTo(1);
        assertThat(ShardProcessor.chooseTarget(0L, ticks(1, 10L, 20L, null), 100).id()).isEqualTo(2);
        assertThat(ShardProcessor.chooseTarget(0L, ticks(1, null, 20L), 100).id()).isEqualTo(2);
    }

    @Test
    void gapsInTickIdsDoNotMatter() {
        var candidates = List.of(tick(5, 110L), tick(7, 150L), tick(12, 300L));

        assertThat(ShardProcessor.chooseTarget(100L, candidates, 60).id()).isEqualTo(7);
        assertThat(ShardProcessor.chooseTarget(100L, candidates, 10).id()).isEqualTo(5);
    }

    @Test
    void softInterruptLeftUnconsumedByTheCallIsClearedWhenItReturns() throws Exception {
        var self = new AtomicReference<ShardProcessor.HandlerCall>();
        var call = new ShardProcessor.HandlerCall(() -> {
            self.get().interrupt(); // the soft deadline hits; the handler returns without looking
            return List.of();
        });
        self.set(call);
        try {
            call.call();
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void softInterruptAimedAtAFinishedCallNeverReachesTheNextTaskOnItsThread() throws Exception {
        var pool = Executors.newSingleThreadExecutor();
        try {
            var call = new ShardProcessor.HandlerCall(List::of);
            pool.submit(call).get();
            var started = new CountDownLatch(1);
            var interrupterDone = new CountDownLatch(1);
            var next = pool.submit(() -> { // the next task on the same thread
                started.countDown();
                try {
                    interrupterDone.await();
                    return Thread.currentThread().isInterrupted();
                } catch (InterruptedException e) {
                    return true;
                }
            });
            started.await();

            call.interrupt(); // a late soft deadline of the finished call
            interrupterDone.countDown();

            assertThat(next.get()).isFalse();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void windDownStaysVisibleToTheCallWhenItsClientSwallowsTheInterrupt() throws Exception {
        var self = new AtomicReference<ShardProcessor.HandlerCall>();
        var seen = new ArrayList<Boolean>();
        var call = new ShardProcessor.HandlerCall(() -> {
            seen.add(OutboxHandler.windDownRequested());
            self.get().interrupt(); // the soft deadline hits
            Thread.interrupted(); // a client clears the flag and throws something else
            seen.add(OutboxHandler.windDownRequested());
            return List.of();
        });
        self.set(call);

        call.call();

        assertThat(seen).containsExactly(false, true);
        assertThat(OutboxHandler.windDownRequested()).as("outside a call").isFalse();
    }

    @Test
    void defaultHandleBatchStartsNoRecordAfterAWindDownWhoseInterruptWasSwallowed() throws Exception {
        var self = new AtomicReference<ShardProcessor.HandlerCall>();
        var handled = new ArrayList<Long>();
        OutboxHandler handler = r -> {
            handled.add(r.id());
            if (r.id() == 2) {
                self.get().interrupt(); // the soft deadline hits during a blocking HTTP call
                Thread.interrupted(); // which clears the flag and wraps the InterruptedException
                throw new RuntimeException(new InterruptedException());
            }
        };
        var records = List.of(record(1, "a"), record(2, "b"), record(3, "c"), record(4, "d"));
        var call = new ShardProcessor.HandlerCall(() -> handler.handleBatch(records));
        self.set(call);

        var outcomes = call.call();

        assertThat(handled).containsExactly(1L, 2L);
        assertThat(outcomes).extracting(Object::getClass).containsExactly(Outcome.Delivered.class,
                Outcome.Retry.class, Outcome.NotAttempted.class, Outcome.NotAttempted.class);
    }

    private static OutboxRecord record(long id, String key) {
        return new OutboxRecord(id, UUID.randomUUID(), "t", key.getBytes(), new byte[0], List.of(), Instant.EPOCH, 0, 0);
    }

    /** Ticks with consecutive ids from {@code firstId}. */
    private static List<Tick> ticks(long firstId, Long... maxIds) {
        var ticks = new ArrayList<Tick>();
        for (int i = 0; i < maxIds.length; i++) {
            ticks.add(tick(firstId + i, maxIds[i]));
        }
        return ticks;
    }

    private static Tick tick(long id, @Nullable Long maxId) {
        return new Tick(id, PgSnapshot.parse("1:1:"), Instant.EPOCH, maxId);
    }
}
