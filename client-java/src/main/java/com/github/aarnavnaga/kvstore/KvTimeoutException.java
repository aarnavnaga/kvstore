package com.github.aarnavnaga.kvstore;

/**
 * The operation did not complete before its deadline. For a write the outcome is
 * unknown: it may or may not have been applied. Retrying the same call through the same
 * {@link KvClient} is safe only for reads; a fresh write call gets a new sequence number.
 */
public class KvTimeoutException extends KvException {
  private static final long serialVersionUID = 1L;

  public KvTimeoutException(String message, Throwable cause) {
    super(StatusCode.TIMEOUT, message, cause);
  }
}
