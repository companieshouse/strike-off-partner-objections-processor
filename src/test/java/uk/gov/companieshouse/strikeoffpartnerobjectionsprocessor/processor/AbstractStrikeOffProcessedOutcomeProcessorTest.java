package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractStrikeOffProcessedOutcomeProcessorTest {

    private AbstractStrikeOffProcessedOutcomeProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new AbstractStrikeOffProcessedOutcomeProcessor(mock(InternalApiClient.class)) {
            @Override
            protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
                // no-op
            }
        };
    }

    @Test
    void process_validMessage_doesNotThrow() {
        assertDoesNotThrow(() -> processor.process(validMessage()));
    }

    @Test
    void validate_nullMessage_throwsInvalidMessage() {
        assertThrows(InvalidStrikeOffMessageException.class, () -> processor.process(null));
    }

    @Test
    void validate_missingStrikeOffEventId_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed message = validMessage();
        message.setStrikeOffEventId(" ");

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("strike_off_event_id"));
    }

    @Test
    void validate_missingOutcome_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed message = validMessage();
        message.setSuccessFailureIndicator(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("success_failure_indicator"));
    }

    @Test
    void validate_missingEventType_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed message = validMessage();
        message.setEventType(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("event_type"));
    }

    @Test
    void validate_missingCompanyNumber_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed message = validMessage();
        message.setCompanyNumber(" ");

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("company_number"));
    }

    @Test
    void mapApiException_uriValidation_isNonRetryable() {
        RuntimeException result = processor.mapApiException("evt-001", mock(URIValidationException.class));
        assertInstanceOf(InvalidStrikeOffMessageException.class, result);
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 422})
    void mapApiException_4xxExcept429_isNonRetryable(int status) {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);

        RuntimeException result = processor.mapApiException("evt-001", apiEx);

        assertInstanceOf(InvalidStrikeOffMessageException.class, result);
        assertTrue(result.getMessage().contains("status=" + status));
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 502, 503})
    void mapApiException_429And5xx_isRetryable(int status) {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);

        RuntimeException result = processor.mapApiException("evt-001", apiEx);

        assertFalse(result instanceof InvalidStrikeOffMessageException);
        assertTrue(result.getMessage().contains("status=" + status));
    }

    @Test
    void mapApiException_unknown_isRetryable() {
        RuntimeException result = processor.mapApiException("evt-001", new IllegalStateException("boom"));
        assertFalse(result instanceof InvalidStrikeOffMessageException);
        assertTrue(result.getMessage().contains("Retryable error"));
    }

    private StrikeOffPartnerObjectionsProcessed validMessage() {
        return new StrikeOffPartnerObjectionsProcessed(
                "strike-001",
                SuccessFailureIndicator.SUCCESS,
                null,
                EventType.OBJECTION,
                LocalDate.of(2026, 12, 31),
                "12345678"
        );
    }
}

