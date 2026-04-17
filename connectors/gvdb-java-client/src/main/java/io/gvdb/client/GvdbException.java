package io.gvdb.client;

public class GvdbException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public GvdbException(String message) {
        super(message);
    }

    public GvdbException(String message, Throwable cause) {
        super(message, cause);
    }
}
