package com.github.aarnavnaga.kvstore;

/** A request failed with a definite answer from the cluster, e.g. an invalid argument. */
public class KvException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final StatusCode code;

  public KvException(StatusCode code, String message) {
    super(code + (message == null || message.isEmpty() ? "" : ": " + message));
    this.code = code;
  }

  public KvException(StatusCode code, String message, Throwable cause) {
    super(code + (message == null || message.isEmpty() ? "" : ": " + message), cause);
    this.code = code;
  }

  public StatusCode code() {
    return code;
  }
}
