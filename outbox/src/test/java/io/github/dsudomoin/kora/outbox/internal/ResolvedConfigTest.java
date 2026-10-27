package io.github.dsudomoin.kora.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ResolvedConfigTest {

    /** A key added to OutboxConfig but not to Resolved would silently fall back to its default. */
    @Test
    void everyConfigMethodIsDelegated() {
        var missing = Arrays.stream(OutboxConfig.class.getMethods())
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> {
                    try {
                        return OutboxSchema.Resolved.class.getDeclaredMethod(m.getName(), m.getParameterTypes())
                                .isSynthetic();
                    } catch (NoSuchMethodException e) {
                        return true;
                    }
                })
                .map(m -> m.getName())
                .toList();

        assertThat(missing).isEmpty();
    }
}
