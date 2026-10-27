package io.github.dsudomoin.kora.outbox.admin;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.kora.outbox.DeadLetterFilter;
import io.github.dsudomoin.kora.outbox.OutboxAdmin;
import io.github.dsudomoin.kora.outbox.OutboxStatus;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiDelegate;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.CountDeadLettersApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.CountDeadLettersApiResponse.CountDeadLetters200ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.CountDeadLettersApiResponse.CountDeadLetters400ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.CountDeadLettersApiResponse.CountDeadLetters409ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.DiscardDeadLettersApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.DiscardDeadLettersApiResponse.DiscardDeadLetters200ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.DiscardDeadLettersApiResponse.DiscardDeadLetters400ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.DiscardDeadLettersApiResponse.DiscardDeadLetters409ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.FindDeadLettersApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.FindDeadLettersApiResponse.FindDeadLetters200ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.FindDeadLettersApiResponse.FindDeadLetters400ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.FindDeadLettersApiResponse.FindDeadLetters409ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.GetStatusApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.GetStatusApiResponse.GetStatus200ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.GetStatusApiResponse.GetStatus400ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.GetStatusApiResponse.GetStatus409ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RecoverXidRegressionApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RecoverXidRegressionApiResponse.RecoverXidRegression200ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RecoverXidRegressionApiResponse.RecoverXidRegression400ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RecoverXidRegressionApiResponse.RecoverXidRegression409ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RedriveDeadLettersApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RedriveDeadLettersApiResponse.RedriveDeadLetters200ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RedriveDeadLettersApiResponse.RedriveDeadLetters400ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RedriveDeadLettersApiResponse.RedriveDeadLetters409ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RestoreOrphanApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RestoreOrphanApiResponse.RestoreOrphan200ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RestoreOrphanApiResponse.RestoreOrphan400ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.RestoreOrphanApiResponse.RestoreOrphan409ApiResponse;
import io.github.dsudomoin.kora.outbox.admin.model.Count;
import io.github.dsudomoin.kora.outbox.admin.model.DeadLetter;
import io.github.dsudomoin.kora.outbox.admin.model.DeadLetterCount;
import io.github.dsudomoin.kora.outbox.admin.model.DeadLetterPage;
import io.github.dsudomoin.kora.outbox.admin.model.DeadLetterStatus;
import io.github.dsudomoin.kora.outbox.admin.model.Filter;
import io.github.dsudomoin.kora.outbox.admin.model.Header;
import io.github.dsudomoin.kora.outbox.admin.model.ModelError;
import io.github.dsudomoin.kora.outbox.admin.model.Orphan;
import io.github.dsudomoin.kora.outbox.admin.model.OrphanKind;
import io.github.dsudomoin.kora.outbox.admin.model.Selection;
import io.github.dsudomoin.kora.outbox.admin.model.Status;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The admin API over {@link OutboxAdmin}: an {@link IllegalArgumentException} of a call answers 400, an
 * {@link IllegalStateException} 409, both with its message; anything else is the HTTP server's 500. Enums are mapped
 * by exhaustive {@code switch}, so a constant added on either side fails the build instead of a request.
 */
public final class OutboxAdminDelegate implements OutboxAdminApiDelegate {

    private static final int DEFAULT_LIMIT = 100;
    /** As the contract's {@code maxItems}: the library binds every id in each chunk's query. */
    private static final int MAX_IDS = 1000;

    private final OutboxAdmin admin;

    public OutboxAdminDelegate(OutboxAdmin admin) {
        this.admin = admin;
    }

    @Override
    public GetStatusApiResponse getStatus() {
        return call(() -> new GetStatus200ApiResponse(status(admin.status())), GetStatus400ApiResponse::new,
                GetStatus409ApiResponse::new);
    }

    @Override
    public FindDeadLettersApiResponse findDeadLetters(@Nullable String topic, @Nullable String key,
            @Nullable String error, @Nullable OffsetDateTime failedFrom, @Nullable OffsetDateTime failedTo,
            @Nullable DeadLetterStatus status, @Nullable Long afterId, @Nullable Integer limit) {
        return call(() -> {
            var filter = filter(topic, key, error, failedFrom, failedTo, status);
            var page = admin.findDeadLetters(filter, afterId, limit == null ? DEFAULT_LIMIT : limit);
            return new FindDeadLetters200ApiResponse(new DeadLetterPage(
                    page.letters().stream().map(OutboxAdminDelegate::letter).toList(), page.nextAfterId()));
        }, FindDeadLetters400ApiResponse::new, FindDeadLetters409ApiResponse::new);
    }

    @Override
    public CountDeadLettersApiResponse countDeadLetters(@Nullable String topic, @Nullable String key,
            @Nullable String error, @Nullable OffsetDateTime failedFrom, @Nullable OffsetDateTime failedTo,
            @Nullable DeadLetterStatus status) {
        return call(() -> new CountDeadLetters200ApiResponse(new Count(
                        admin.countDeadLetters(filter(topic, key, error, failedFrom, failedTo, status)))),
                CountDeadLetters400ApiResponse::new, CountDeadLetters409ApiResponse::new);
    }

    @Override
    public RedriveDeadLettersApiResponse redriveDeadLetters(@Nullable Selection selection) {
        return call(() -> {
            var ids = ids(selection);
            long n = ids != null ? admin.redriveDeadLetters(ids) : admin.redriveDeadLetters(filter(selection.filter()));
            return new RedriveDeadLetters200ApiResponse(new Count(n));
        }, RedriveDeadLetters400ApiResponse::new, RedriveDeadLetters409ApiResponse::new);
    }

    @Override
    public DiscardDeadLettersApiResponse discardDeadLetters(@Nullable Selection selection) {
        return call(() -> {
            var ids = ids(selection);
            long n = ids != null ? admin.discardDeadLetters(ids) : admin.discardDeadLetters(filter(selection.filter()));
            return new DiscardDeadLetters200ApiResponse(new Count(n));
        }, DiscardDeadLetters400ApiResponse::new, DiscardDeadLetters409ApiResponse::new);
    }

    @Override
    public RestoreOrphanApiResponse restoreOrphan(String table, long fromXid) {
        return call(() -> new RestoreOrphan200ApiResponse(new Count(admin.restoreOrphan(table, fromXid))),
                RestoreOrphan400ApiResponse::new, RestoreOrphan409ApiResponse::new);
    }

    @Override
    public RecoverXidRegressionApiResponse recoverXidRegression() {
        return call(() -> new RecoverXidRegression200ApiResponse(new Count(admin.recoverXidRegression())),
                RecoverXidRegression400ApiResponse::new, RecoverXidRegression409ApiResponse::new);
    }

    /** {@code ok}, or the 400 or 409 for the refusals {@link OutboxAdmin} names; any other failure propagates. */
    private static <R> R call(Supplier<R> ok, Function<ModelError, R> badRequest, Function<ModelError, R> conflict) {
        try {
            return ok.get();
        } catch (IllegalArgumentException e) {
            return badRequest.apply(error(e));
        } catch (IllegalStateException e) {
            return conflict.apply(error(e));
        }
    }

    private static ModelError error(RuntimeException e) {
        return new ModelError(Objects.requireNonNullElse(e.getMessage(), e.getClass().getName()));
    }

    /** The ids, or null for the filter: exactly one of them. */
    @Nullable
    private static List<Long> ids(@Nullable Selection selection) {
        if (selection == null || (selection.ids() == null) == (selection.filter() == null)) {
            throw new IllegalArgumentException("give exactly one of ids and filter");
        }
        var ids = selection.ids();
        if (ids != null && ids.size() > MAX_IDS) {
            throw new IllegalArgumentException("at most " + MAX_IDS + " ids, not " + ids.size()
                    + ": select more with a filter");
        }
        if (ids != null && ids.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("ids must be numbers, not null");
        }
        return ids;
    }

    private static DeadLetterFilter filter(Filter f) {
        return filter(f.topic(), f.key(), f._error(), f.failedFrom(), f.failedTo(), f.status());
    }

    private static DeadLetterFilter filter(@Nullable String topic, @Nullable String key, @Nullable String error,
            @Nullable OffsetDateTime failedFrom, @Nullable OffsetDateTime failedTo,
            @Nullable DeadLetterStatus status) {
        return new DeadLetterFilter(topic, key == null ? null : key.getBytes(UTF_8), error,
                failedFrom == null ? null : failedFrom.toInstant(), failedTo == null ? null : failedTo.toInstant(),
                status == null ? null : letterStatus(status));
    }

    private static io.github.dsudomoin.kora.outbox.DeadLetter.Status letterStatus(DeadLetterStatus status) {
        return switch (status) {
            case PENDING -> io.github.dsudomoin.kora.outbox.DeadLetter.Status.PENDING;
            case KEPT -> io.github.dsudomoin.kora.outbox.DeadLetter.Status.KEPT;
            case SCHEDULED -> io.github.dsudomoin.kora.outbox.DeadLetter.Status.SCHEDULED;
        };
    }

    private static Status status(OutboxStatus s) {
        return new Status(s.halted(), s.lagSeconds(), s.retrySize(), s.parkedSize(),
                s.deadLetters().stream().map(c -> new DeadLetterCount(c.topic(), status(c.status()), c.count()))
                        .toList(),
                s.orphans().stream().map(o -> new Orphan(o.table(), kind(o.kind()))).toList(),
                s.tailOverflow(), s.premade(), s.xidsLeft(), s.maintenanceSinceSuccessSeconds(), s.shards());
    }

    private static DeadLetter letter(io.github.dsudomoin.kora.outbox.DeadLetter l) {
        return new DeadLetter(l.id(), l.messageId(), l.topic(), l.key(), l.payload(),
                l.headers().stream().map(h -> new Header(h.name(), h.value())).toList(), utc(l.createdAt()),
                l.attempts(), utc(l.failedAt()), l.error(), status(l.status()), l.redrives(), l.handlerAttempts(),
                l.dueAt() == null ? null : utc(l.dueAt()));
    }

    private static DeadLetterStatus status(io.github.dsudomoin.kora.outbox.DeadLetter.Status status) {
        return switch (status) {
            case PENDING -> DeadLetterStatus.PENDING;
            case KEPT -> DeadLetterStatus.KEPT;
            case SCHEDULED -> DeadLetterStatus.SCHEDULED;
        };
    }

    private static OrphanKind kind(OutboxStatus.OrphanKind kind) {
        return switch (kind) {
            case DETACHED_BY_LIBRARY -> OrphanKind.DETACHED_BY_LIBRARY;
            case FOREIGN -> OrphanKind.FOREIGN;
            case SET_ASIDE -> OrphanKind.SET_ASIDE;
        };
    }

    private static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }
}
