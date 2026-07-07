package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StrikeOffPartnerObjectionsProcessorTest {

    private final StrikeOffPartnerObjectionsProcessor processor =
            new StrikeOffPartnerObjectionsProcessor();

    @Test
    void supportsObjections_butNotWithdrawals() {
        assertTrue(processor.supports(EventType.OBJECTION));
        assertFalse(processor.supports(EventType.WITHDRAWAL));
    }

    @Test
    void doProcess_validMessage_doesNotThrow() {
        StrikeOffPartnerObjections message = StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setEventType(EventType.OBJECTION)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();

        // process() calls validate() then doProcess(); both should succeed for a valid OBJECTION
        assertDoesNotThrow(() -> processor.process(message));
    }
}

