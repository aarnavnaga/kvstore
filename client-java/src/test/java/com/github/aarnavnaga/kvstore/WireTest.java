package com.github.aarnavnaga.kvstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class WireTest {
  private static byte[] hex(String s) {
    String[] parts = s.trim().split("\\s+");
    byte[] out = new byte[parts.length];
    for (int i = 0; i < parts.length; i++) {
      out[i] = (byte) Integer.parseInt(parts[i], 16);
    }
    return out;
  }

  // The same bytes are asserted by the C++ test Protocol.GoldenBytesMatchJavaClient, so
  // the two implementations cannot drift apart silently.
  @Test
  void putFrameMatchesGoldenBytes() {
    byte[] payload = Wire.encodePut(42, 1, "k", "v".getBytes(StandardCharsets.UTF_8));
    byte[] frame = Wire.encodeFrame(Wire.PUT, 7, payload);
    assertArrayEquals(
        hex("0f 00 00 00  02  07 00 00 00 00 00 00 00  2a 01 01 6b 01 76"), frame);
  }

  @Test
  void decodesNotLeaderWithHint() {
    byte[] payload = hex("05 00 0a 6e 6f 64 65 32 3a 37 30 30 30");  // "node2:7000"
    Wire.Response r = Wire.decodeResponse(payload);
    assertEquals(StatusCode.NOT_LEADER, r.code());
    assertEquals("node2:7000", r.leaderHint());
    assertEquals(0, r.value().length);
  }

  @Test
  void varintsRoundTripAcrossByteBoundaries() {
    long[] values = {0, 1, 127, 128, 300, 1L << 32, Long.MAX_VALUE, -1L};
    Wire.Out out = new Wire.Out();
    for (long v : values) {
      out.varint(v);
    }
    Wire.In in = new Wire.In(out.toByteArray());
    for (long v : values) {
      assertEquals(v, in.varint());
    }
    assertEquals(0, in.remaining());
  }

  @Test
  void rejectsTruncatedResponse() {
    assertThrows(IllegalArgumentException.class, () -> Wire.decodeResponse(hex("00 05 61")));
  }
}
