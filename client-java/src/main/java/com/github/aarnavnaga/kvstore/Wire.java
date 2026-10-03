package com.github.aarnavnaga.kvstore;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The binary protocol, byte for byte as the C++ server speaks it.
 *
 * <pre>
 * frame:    u32le length | u8 type | u64le request_id | payload    (length = 9 + payload)
 * PUT:      varint client_id | varint seq | bytes key | bytes value
 * GET:      bytes key
 * DELETE:   varint client_id | varint seq | bytes key
 * RESPONSE: u8 status | bytes value_or_message | bytes leader_hint
 * </pre>
 *
 * where {@code bytes} is a varint length followed by that many bytes.
 */
final class Wire {
  static final byte PING = 1;
  static final byte PUT = 2;
  static final byte GET = 3;
  static final byte DELETE = 4;
  static final byte RESPONSE = 5;
  static final byte STATUS = 7;

  static final int HEADER = 4 + 1 + 8;
  static final int MAX_FRAME = 64 << 20;

  private Wire() {}

  record Frame(byte type, long requestId, byte[] payload) {}

  record Response(StatusCode code, byte[] value, String leaderHint) {}

  static byte[] encodeFrame(byte type, long requestId, byte[] payload) {
    ByteBuffer b = ByteBuffer.allocate(HEADER + payload.length).order(ByteOrder.LITTLE_ENDIAN);
    b.putInt(1 + 8 + payload.length);
    b.put(type);
    b.putLong(requestId);
    b.put(payload);
    return b.array();
  }

  static byte[] encodePut(long clientId, long seq, String key, byte[] value) {
    Out o = new Out();
    o.varint(clientId);
    o.varint(seq);
    o.bytes(key.getBytes(StandardCharsets.UTF_8));
    o.bytes(value);
    return o.toByteArray();
  }

  static byte[] encodeGet(String key) {
    Out o = new Out();
    o.bytes(key.getBytes(StandardCharsets.UTF_8));
    return o.toByteArray();
  }

  static byte[] encodeDelete(long clientId, long seq, String key) {
    Out o = new Out();
    o.varint(clientId);
    o.varint(seq);
    o.bytes(key.getBytes(StandardCharsets.UTF_8));
    return o.toByteArray();
  }

  static Response decodeResponse(byte[] payload) {
    In in = new In(payload);
    StatusCode code = StatusCode.fromWire(in.u8());
    byte[] value = in.bytes();
    String hint = new String(in.bytes(), StandardCharsets.UTF_8);
    if (in.remaining() != 0) {
      throw new IllegalArgumentException("trailing bytes in response");
    }
    return new Response(code, value, hint);
  }

  /** Varint: 7 bits per byte, low bits first, high bit set on all but the last byte. */
  static final class Out extends ByteArrayOutputStream {
    void varint(long v) {
      while ((v & ~0x7FL) != 0) {
        write((int) ((v & 0x7F) | 0x80));
        v >>>= 7;
      }
      write((int) v);
    }

    void bytes(byte[] b) {
      varint(b.length);
      write(b, 0, b.length);
    }
  }

  static final class In {
    private final byte[] buf;
    private int pos;

    In(byte[] buf) {
      this.buf = buf;
    }

    int u8() {
      if (pos >= buf.length) {
        throw new IllegalArgumentException("truncated");
      }
      return buf[pos++] & 0xFF;
    }

    long varint() {
      long result = 0;
      for (int shift = 0; shift < 64; shift += 7) {
        int b = u8();
        result |= (long) (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
          return result;
        }
      }
      throw new IllegalArgumentException("varint too long");
    }

    byte[] bytes() {
      long len = varint();
      if (len < 0 || len > buf.length - pos) {
        throw new IllegalArgumentException("length past end of buffer");
      }
      byte[] out = Arrays.copyOfRange(buf, pos, pos + (int) len);
      pos += (int) len;
      return out;
    }

    int remaining() {
      return buf.length - pos;
    }
  }
}
