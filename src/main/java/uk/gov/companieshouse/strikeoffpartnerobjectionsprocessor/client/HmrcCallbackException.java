package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

public class HmrcCallbackException extends RuntimeException {
    private final int statusCode;

    public HmrcCallbackException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public HmrcCallbackException(String message, int statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}

