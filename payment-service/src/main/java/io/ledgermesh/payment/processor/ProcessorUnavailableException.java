package io.ledgermesh.payment.processor;

public class ProcessorUnavailableException extends RuntimeException {

  public ProcessorUnavailableException(String message) {
    super(message);
  }
}
