package io.gvdb.client;

public final class GvdbTimeoutException extends GvdbException {

    private static final long serialVersionUID = 1L;

    public GvdbTimeoutException(String message) {
        super(message);
    }
}
