package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxHeader;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Compact header encoding for the {@code headers bytea} column, big-endian:
 * {@code u16 count; per header: u16 nameLen, UTF-8 name, i32 valueLen (-1 = null), value}. Empty list = SQL null.
 */
public final class HeaderCodec {

    private static final int U16_MAX = 0xFFFF;

    private HeaderCodec() {}

    public static @Nullable byte[] encode(List<OutboxHeader> headers) {
        if (headers.isEmpty()) {
            return null;
        }
        if (headers.size() > U16_MAX) {
            throw new IllegalArgumentException("more than " + U16_MAX + " headers: " + headers.size());
        }
        var names = new byte[headers.size()][];
        int size = 2;
        for (int i = 0; i < names.length; i++) {
            OutboxHeader header = headers.get(i);
            names[i] = header.name().getBytes(UTF_8);
            if (names[i].length > U16_MAX) {
                throw new IllegalArgumentException("header name longer than " + U16_MAX + " bytes");
            }
            size += 2 + names[i].length + 4 + (header.value() == null ? 0 : header.value().length);
        }
        var buffer = ByteBuffer.allocate(size).putShort((short) names.length);
        for (int i = 0; i < names.length; i++) {
            byte[] value = headers.get(i).value();
            buffer.putShort((short) names[i].length).put(names[i]);
            if (value == null) {
                buffer.putInt(-1);
            } else {
                buffer.putInt(value.length).put(value);
            }
        }
        return buffer.array();
    }

    public static List<OutboxHeader> decode(@Nullable byte[] bytes) {
        if (bytes == null) {
            return List.of();
        }
        var buffer = ByteBuffer.wrap(bytes);
        int count = Short.toUnsignedInt(buffer.getShort());
        var headers = new ArrayList<OutboxHeader>(count);
        for (int i = 0; i < count; i++) {
            var name = new byte[Short.toUnsignedInt(buffer.getShort())];
            buffer.get(name);
            int valueLength = buffer.getInt();
            byte[] value = null;
            if (valueLength > buffer.remaining()) { // before allocating: a corrupt length must not exhaust the heap
                throw new IllegalArgumentException("corrupt headers: value length " + valueLength + " with "
                        + buffer.remaining() + " bytes left");
            } else if (valueLength >= 0) {
                value = new byte[valueLength];
                buffer.get(value);
            } else if (valueLength != -1) {
                throw new IllegalArgumentException("corrupt headers: value length " + valueLength);
            }
            headers.add(new OutboxHeader(new String(name, UTF_8), value));
        }
        return headers;
    }
}
