package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApiExceptionMapperTest {

    private static final String OPERATION_NAME = "updateObjectionStatus";
    private static final String EVENT_ID = "evt-001";

    private final Logger logger = mock(Logger.class);

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 404, 422})
    void map_4xxExcept429_returnsNonRetryable(int status) {
        ApiErrorResponseException apiException = mock(ApiErrorResponseException.class);
        when(apiException.getStatusCode()).thenReturn(status);

        RuntimeException mapped = ApiExceptionMapper.map(OPERATION_NAME, EVENT_ID, apiException, logger);

        assertInstanceOf(InvalidStrikeOffMessageException.class, mapped);
        assertTrue(mapped.getMessage().contains("status=" + status));
        verify(logger).info(String.format("%s failed: status=%d, eventId=%s", OPERATION_NAME, status, EVENT_ID));
    }

    @ParameterizedTest
    @ValueSource(ints = {399, 429, 500, 503})
    void map_retryableStatuses_returnsRuntimeException(int status) {
        ApiErrorResponseException apiException = mock(ApiErrorResponseException.class);
        when(apiException.getStatusCode()).thenReturn(status);

        RuntimeException mapped = ApiExceptionMapper.map(OPERATION_NAME, EVENT_ID, apiException, logger);

        assertFalse(mapped instanceof InvalidStrikeOffMessageException);
        assertTrue(mapped.getMessage().contains("status=" + status));
        verify(logger).info(String.format("%s failed: status=%d, eventId=%s", OPERATION_NAME, status, EVENT_ID));
    }

    @ParameterizedTest
    @ValueSource(strings = {"evt-001", "another-event"})
    void map_uriValidationException_returnsNonRetryable(String eventId) {
        RuntimeException mapped = ApiExceptionMapper.map(OPERATION_NAME, eventId,
                mock(URIValidationException.class), logger);

        assertInstanceOf(InvalidStrikeOffMessageException.class, mapped);
        assertTrue(mapped.getMessage().contains("URI validation error"));
        assertTrue(mapped.getMessage().contains(eventId));
    }

    @ParameterizedTest
    @ValueSource(strings = {"evt-001", "evt-999"})
    void map_unknownException_returnsRetryable(String eventId) {
        RuntimeException mapped = ApiExceptionMapper.map(OPERATION_NAME, eventId,
                new IllegalStateException("boom"), logger);

        assertFalse(mapped instanceof InvalidStrikeOffMessageException);
        assertTrue(mapped.getMessage().contains("Retryable error"));
        assertTrue(mapped.getMessage().contains(eventId));
    }
}

