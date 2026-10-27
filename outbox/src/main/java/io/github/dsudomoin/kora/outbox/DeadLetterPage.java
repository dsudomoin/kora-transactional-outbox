package io.github.dsudomoin.kora.outbox;

import jakarta.annotation.Nullable;
import java.util.List;

/**
 * One page of {@link OutboxAdmin#findDeadLetters}, in id order: {@code nextAfterId} is the {@code afterId} of the next
 * page, null once a page comes back short of its limit.
 */
public record DeadLetterPage(List<DeadLetter> letters, @Nullable Long nextAfterId) {

    public DeadLetterPage {
        letters = List.copyOf(letters);
    }
}
