package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractStrikeOffPartnerEventsProcessorTest {

    private AbstractStrikeOffPartnerEventsProcessor<SpecificRecordBase> processor;

    @BeforeEach
    void setUp() {
        processor = new AbstractStrikeOffPartnerEventsProcessor<>(mock(InternalApiClient.class), msg -> "test-event-id") {
            @Override
            protected String buildResourceUri(SpecificRecordBase message, String resourceSegment) {
                return "/company/12345678/" + resourceSegment + "/strike-001";
            }

            @Override
            protected String buildInternalStatusUri(SpecificRecordBase message, String resourceSegment, String statusSegment) {
                return "/internal/company/12345678/" + resourceSegment + "/strike-001/" + statusSegment;
            }
        };
    }

    @Test
    void mapApiException_uriValidationException_isNonRetryable() {
        RuntimeException result = processor.mapApiException("evt-001", mock(URIValidationException.class));

        assertInstanceOf(uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException.class, result);
        assertTrue(result.getMessage().contains("Non-retryable URI validation error"));
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 422})
    void mapApiException_4xxStatus_isNonRetryable(int status) {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);

        RuntimeException result = processor.mapApiException("evt-001", apiEx);

        assertInstanceOf(uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException.class, result, "Expected non-retryable for status " + status);
        assertTrue(result.getMessage().contains("Non-retryable API error (status=" + status + ")"));
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 502, 503})
    void mapApiException_retriableApiStatuses_areRetryable(int status) {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);

        RuntimeException result = processor.mapApiException("evt-001", apiEx);

        assertFalse(result instanceof uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException,
                "Expected retryable for status " + status);
        assertTrue(result.getMessage().contains("Retryable API error (status=" + status + ")"));
    }

    @Test
    void mapApiException_unknownException_isRetryable() {
        RuntimeException result = processor.mapApiException("evt-001", new IllegalStateException("boom"));

        assertFalse(result instanceof uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException);
        assertTrue(result.getMessage().contains("Retryable error for eventId=evt-001"));
    }

    @Test
    void mapApiException_includesEventIdInMessage() {
        RuntimeException result = processor.mapApiException("my-event-id", new IllegalStateException());

        assertTrue(result.getMessage().contains("my-event-id"));
    }

    @Test
    void isDuplicateRecord_whenStatusMatchesProcessedStatus_returnsTrue() {
        assertTrue(processor.isDuplicateRecord("PROCESSED", "PROCESSED"));
    }

    @Test
    void isDuplicateRecord_isCaseInsensitive_returnsTrue() {
        assertTrue(processor.isDuplicateRecord("processed", "PROCESSED"));
    }

    @Test
    void isDuplicateRecord_whenStatusDoesNotMatch_returnsFalse() {
        assertFalse(processor.isDuplicateRecord("PROCESSED", "PENDING"));
    }

    @Test
    void isDuplicateRecord_whenProcessedStatusIsNull_returnsFalse() {
        assertFalse(processor.isDuplicateRecord("PROCESSED", null));
    }
}

