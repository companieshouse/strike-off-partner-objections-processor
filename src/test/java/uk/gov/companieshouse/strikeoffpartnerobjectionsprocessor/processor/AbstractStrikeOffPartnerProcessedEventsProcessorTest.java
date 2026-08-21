package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.WITHDRAWAL;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

class AbstractStrikeOffPartnerProcessedEventsProcessorTest {

    private AbstractStrikeOffPartnerProcessedEventsProcessor processor;

    @BeforeEach
    void setUp() {
        // Minimal concrete subclass that supports OBJECTION
        processor = new AbstractStrikeOffPartnerProcessedEventsProcessor(mock(InternalApiClient.class)) {
            @Override
            protected boolean supports(ProcessedEventType eventType) {
                return eventType == OBJECTION;
            }

            @Override
            protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
                // no-op
            }
        };
    }

    @Test
    void process_validMessage_doesNotThrow() {
        assertDoesNotThrow(() -> processor.process(validMessage(true)));
    }

    @Test
    void validate_nullMessage_throwsInvalidMessage() {
        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(null));
        assertTrue(ex.getMessage().contains("message"));
    }

    @Test
    void validate_nullEventType_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed msg = validMessage(true);
        msg.setEventType(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("processedEventType"));
    }

    @Test
    void validate_nullCompanyNumber_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed msg = validMessage(true);
        msg.setCompanyNumber(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("Company number"));
    }

    @Test
    void process_unsupportedEventType_throwsRuntimeException() {
        StrikeOffPartnerObjectionsProcessed msg = validMessage(true);
        msg.setEventType(WITHDRAWAL);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("unsupported event type"));
    }

    @Test
    void validate_invalidIdField_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed msg = validMessage(true);
        msg.setStrikeOffEventId("  ");

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("StrikeOffEventId"),
                "Expected error message to contain: " + "StrikeOffEventId");
    }

    @Test
    void validate_failureButNoErrorMessage_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed msg = validMessage(false);
        msg.setErrorMessage(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("ErrorMessage"),
                "Expected error message to contain: " + "ErrorMessage");
    }

    StrikeOffPartnerObjectionsProcessed validMessage(boolean succeeded) {
        return StrikeOffPartnerObjectionsProcessed.newBuilder()
                .setEventType(OBJECTION)
                .setInitialExpirationOn(succeeded ? LocalDate.parse("2024-12-31") : null)
                .setCompanyNumber("12345678")
                .setSuccessFailureIndicator(succeeded ? SUCCESS : FAILURE)
                .setErrorMessage(succeeded ? null : "Some error message")
                .setStrikeOffEventId("strike-001")
                .build();
    }
}
