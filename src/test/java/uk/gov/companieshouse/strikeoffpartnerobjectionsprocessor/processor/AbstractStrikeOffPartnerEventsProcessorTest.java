package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractStrikeOffPartnerEventsProcessorTest {

    private AbstractStrikeOffPartnerEventsProcessor processor;

    @BeforeEach
    void setUp() {
        // Minimal concrete subclass that supports OBJECTION
        processor = new AbstractStrikeOffPartnerEventsProcessor(Mockito.mock(InternalApiClient.class)) {
            @Override
            protected boolean supports(EventType eventType) {
                return eventType == EventType.OBJECTION;
            }

            @Override
            protected void doProcess(StrikeOffPartnerObjections message) {
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
        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(null));
        assertTrue(ex.getMessage().contains("eventType"));
    }

    @Test
    void validate_nullEventType_throwsInvalidMessage() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setEventType(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("eventType"));
    }

    @Test
    void validate_nullEventId_throwsInvalidMessage() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setEventId(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("eventId"));
    }

    @Test
    void validate_nullPartnerOrganisation_throwsInvalidMessage() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setPartnerOrganisation(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("PartnerOrganisation"));
    }

    @Test
    void validate_blankEventId_throwsInvalidMessage() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setEventId("   ");

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("eventId"));
    }

    @Test
    void validate_blankPartnerOrganisation_throwsInvalidMessage() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setPartnerOrganisation("  ");

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("PartnerOrganisation"));
    }

    @Test
    void validate_blankCompanyNumber_throwsInvalidMessage() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setCompanyNumber("  ");

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("Company number"));
    }

    @Test
    void validate_blankStrikeOffEventId_throwsInvalidMessage() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setStrikeOffEventId("  ");

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("StrikeOffEventId"));
    }

    @Test
    void process_unsupportedEventType_throwsRuntimeException() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setEventType(EventType.WITHDRAWAL);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("unsupported event type"));
    }

    @Test
    void buildResourceUri_withDifferentSegment() {
        StrikeOffPartnerObjections msg = validMessage();
        assertEquals("/company/12345678/strike-off-partner-withdrawals/strike-001",
                processor.buildResourceUri(msg, "strike-off-partner-withdrawals"));
    }

    // --- mapApiException: non-retryable cases ---

    @Test
    void mapApiException_uriValidationException_isNonRetryable() {
        RuntimeException result = processor.mapApiException("evt-001", mock(URIValidationException.class));

        assertInstanceOf(InvalidStrikeOffMessageException.class, result);
        assertTrue(result.getMessage().contains("Non-retryable URI validation error"));
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 422})
    void mapApiException_4xxStatus_isNonRetryable(int status) {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);

        RuntimeException result = processor.mapApiException("evt-001", apiEx);

        assertInstanceOf(InvalidStrikeOffMessageException.class, result, "Expected non-retryable for status " + status);
        assertTrue(result.getMessage().contains("Non-retryable API error (status=" + status + ")"));
    }

    // --- mapApiException: retryable cases ---

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 502, 503})
    void mapApiException_retriableApiStatuses_areRetryable(int status) {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);

        RuntimeException result = processor.mapApiException("evt-001", apiEx);

        assertFalse(result instanceof InvalidStrikeOffMessageException,
                "Expected retryable for status " + status);
        assertTrue(result.getMessage().contains("Retryable API error (status=" + status + ")"));
    }

    @Test
    void mapApiException_unknownException_isRetryable() {
        RuntimeException result = processor.mapApiException("evt-001", new IllegalStateException("boom"));

        assertFalse(result instanceof InvalidStrikeOffMessageException);
        assertTrue(result.getMessage().contains("Retryable error for eventId=evt-001"));
    }

    @Test
    void mapApiException_includesEventIdInMessage() {
        RuntimeException result = processor.mapApiException("my-event-id", new IllegalStateException());

        assertTrue(result.getMessage().contains("my-event-id"));
    }

    private StrikeOffPartnerObjections validMessage() {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setCompanyNumber("12345678")
                .setEventType(EventType.OBJECTION)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();
    }
}
