package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DuplicateRecordExceptionTest {

    @Test
    void constructor_withMessage_setsMessage() {
        DuplicateRecordException ex = new DuplicateRecordException("duplicate detected");

        assertEquals("duplicate detected", ex.getMessage());
        assertNull(ex.getCause());
    }

    @Test
    void constructor_withMessageAndCause_setsMessageAndCause() {
        Throwable cause = new IllegalStateException("root cause");
        DuplicateRecordException ex = new DuplicateRecordException("duplicate detected", cause);

        assertEquals("duplicate detected", ex.getMessage());
        assertEquals(cause, ex.getCause());
    }
}

