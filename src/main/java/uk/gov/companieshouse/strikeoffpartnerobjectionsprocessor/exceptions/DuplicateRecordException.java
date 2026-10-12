package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions;

import consumer.exception.NonRetryableErrorException;

public class DuplicateRecordException extends NonRetryableErrorException {
    public DuplicateRecordException(String message) {
        super(message);
    }

    public DuplicateRecordException(String message, Throwable cause) {
        super(message);
        initCause(cause);
    }
}