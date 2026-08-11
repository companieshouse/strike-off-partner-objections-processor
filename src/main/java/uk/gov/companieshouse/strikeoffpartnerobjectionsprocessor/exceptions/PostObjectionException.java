package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions;
public class PostObjectionException extends RuntimeException {
    public PostObjectionException(String message) {
        super(message);
    }

    public PostObjectionException(String message, Throwable cause) {
        super(message, cause);
    }
}