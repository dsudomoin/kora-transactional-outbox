package io.github.dsudomoin.outbox.testing;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Records what is written to {@code System.err} until closed, still passing it through. The tests log through
 * slf4j-simple, which looks up {@code System.err} on every write, so this sees log lines of every thread.
 */
public final class CapturedStderr implements AutoCloseable {

    private final PrintStream original = System.err;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    public CapturedStderr() {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
                write(new byte[] {(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                synchronized (bytes) {
                    bytes.write(b, off, len);
                }
                original.write(b, off, len);
            }
        }, true, UTF_8));
    }

    public String text() {
        synchronized (bytes) {
            return bytes.toString(UTF_8);
        }
    }

    @Override
    public void close() {
        System.setErr(original);
    }
}
