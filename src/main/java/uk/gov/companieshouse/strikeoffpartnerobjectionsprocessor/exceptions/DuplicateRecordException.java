package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions;
public class DuplicateRecordException extends RuntimeException {
    public DuplicateRecordException(String message) {
        super(message);
    }

    public DuplicateRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}