package com.github.aarnavnaga.kvstore;

/** Result codes on the wire. The numbering matches the server's {@code kv::Status::Code}. */
public enum StatusCode {
  OK(0),
  NOT_FOUND(1),
  CORRUPTION(2),
  IO_ERROR(3),
  INVALID_ARGUMENT(4),
  NOT_LEADER(5),
  TIMEOUT(6),
  UNAVAILABLE(7);

  private final int wire;

  StatusCode(int wire) {
    this.wire = wire;
  }

  int wire() {
    return wire;
  }

  static StatusCode fromWire(int code) {
    for (StatusCode c : values()) {
      if (c.wire == code) {
        return c;
      }
    }
    throw new IllegalArgumentException("unknown status code " + code);
  }

  /** Whether another attempt (possibly on another node) might succeed. */
  boolean retryable() {
    return this == NOT_LEADER || this == TIMEOUT || this == UNAVAILABLE;
  }
}
