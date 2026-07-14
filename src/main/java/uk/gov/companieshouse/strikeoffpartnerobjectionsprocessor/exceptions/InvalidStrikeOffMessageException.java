package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions;


import consumer.exception.NonRetryableErrorException;

public class InvalidStrikeOffMessageException extends NonRetryableErrorException {
    public InvalidStrikeOffMessageException(String message) {
        super(message);
    }

    public InvalidStrikeOffMessageException(String message, Throwable cause) {
        super(message);
        initCause(cause);
    }
}