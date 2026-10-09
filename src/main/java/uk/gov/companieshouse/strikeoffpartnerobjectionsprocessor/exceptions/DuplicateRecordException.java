package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions;

import consumer.exception.NonRetryableErrorException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.ProcessorLogContext;

public class DuplicateRecordException extends NonRetryableErrorException {
    private final transient ProcessorLogContext logContext;

    public DuplicateRecordException(String message) {
        this(message, null, null);
    }

    public DuplicateRecordException(String message, Throwable cause) {
        this(message, cause, null);
    }

    public DuplicateRecordException(String message, ProcessorLogContext logContext) {
        this(message, null, logContext);
    }

    private DuplicateRecordException(
            String message, Throwable cause, ProcessorLogContext logContext) {
        super(message);
        initCause(cause);
        this.logContext = logContext;
    }

    public ProcessorLogContext getLogContext() {
        return logContext;
    }
}