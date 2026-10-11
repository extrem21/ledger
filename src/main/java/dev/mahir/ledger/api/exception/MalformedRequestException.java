package dev.mahir.ledger.api.exception;

public class MalformedRequestException extends RuntimeException {
    public MalformedRequestException(String message) {
        super(message);
    }

    public MalformedRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
