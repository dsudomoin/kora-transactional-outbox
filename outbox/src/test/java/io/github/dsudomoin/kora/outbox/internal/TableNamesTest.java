package io.github.dsudomoin.kora.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import org.junit.jupiter.api.Test;

class TableNamesTest {

    @Test
    void derivesNames() {
        var names = TableNames.of("outbox");
        assertThat(names.base()).isEqualTo("outbox");
        assertThat(names.tick()).isEqualTo("outbox_tick");
        assertThat(names.cursor()).isEqualTo("outbox_cursor");
        assertThat(names.retry()).isEqualTo("outbox_retry");
        assertThat(names.dead()).isEqualTo("outbox_dead");
        assertThat(names.redriven()).isEqualTo("outbox_redriven");
        assertThat(names.halt()).isEqualTo("outbox_halt");
        assertThat(names.tail()).isEqualTo("outbox_tail");
        assertThat(names.partitionPrefix()).isEqualTo("outbox_x");
    }

    @Test
    void schemaQualified() {
        var names = TableNames.of("app.outbox");
        assertThat(names.base()).isEqualTo("app.outbox");
        assertThat(names.tick()).isEqualTo("app.outbox_tick");
        assertThat(names.partitionPrefix()).isEqualTo("app.outbox_x");
        assertThat(names.unqualified(names.tick())).isEqualTo("outbox_tick");
        assertThat(TableNames.of("outbox").unqualified("outbox_tick")).isEqualTo("outbox_tick");
    }

    @Test
    void rejectsInjection() {
        for (String bad : new String[] {
            "outbox; drop table x", "a-b", "", "a".repeat(42), "1outbox", "app.", ".outbox", "a.b.c", "\"outbox\""
        }) {
            assertThatThrownBy(() -> TableNames.of(bad)).as(bad).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> TableNames.of(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> TableNames.of("a".repeat(41))).doesNotThrowAnyException();
        assertThatCode(() -> TableNames.of("_s1." + "b".repeat(41))).doesNotThrowAnyException();
    }

    @Test
    void mixedCaseNormalisesToSameNamespaces() {
        // Postgres folds unquoted identifiers to lower case: these name the same tables, so they must share locks
        var mixed = TableNames.of("App.OutBox");
        assertThat(mixed).isEqualTo(TableNames.of("app.outbox"));
        assertThat(mixed.tick()).isEqualTo("app.outbox_tick");
        assertThat(TableNames.of("OUTBOX").keyNamespace()).isEqualTo(TableNames.of("outbox").keyNamespace());
        assertThat(TableNames.of("OUTBOX").internalNamespace()).isEqualTo(TableNames.of("outbox").internalNamespace());
        assertThat(TableNames.of("I").base()).isEqualTo("I".toLowerCase(Locale.ROOT));
    }

    @Test
    void canonicalConstructorValidatesBase() {
        assertThatThrownBy(() -> new TableNames("a-b", "t", "c", "r", "d", "h", "x", "p", 1, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TableNames(null, "t", "c", "r", "d", "h", "x", "p", 1, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void namespacesDiffer() {
        var names = TableNames.of("outbox");
        assertThat(names.keyNamespace()).isEqualTo("outbox-key:outbox".hashCode());
        assertThat(names.internalNamespace()).isEqualTo("outbox-internal:outbox".hashCode());
        assertThat(names.keyNamespace()).isNotEqualTo(names.internalNamespace());
        assertThat(TableNames.of("app.outbox").keyNamespace()).isNotEqualTo(names.keyNamespace());
    }
}
