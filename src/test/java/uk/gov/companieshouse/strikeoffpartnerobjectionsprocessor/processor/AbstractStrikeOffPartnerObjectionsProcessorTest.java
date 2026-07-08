package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractStrikeOffPartnerObjectionsProcessorTest {

    private AbstractStrikeOffPartnerObjectionsProcessor processor;

    @BeforeEach
    void setUp() {
        // Minimal concrete subclass that supports OBJECTION
        processor = new AbstractStrikeOffPartnerObjectionsProcessor() {
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
    void process_unsupportedEventType_throwsRuntimeException() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setEventType(EventType.WITHDRAWAL);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("unsupported event type"));
    }

    private StrikeOffPartnerObjections validMessage() {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setEventType(EventType.OBJECTION)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();
    }
}
