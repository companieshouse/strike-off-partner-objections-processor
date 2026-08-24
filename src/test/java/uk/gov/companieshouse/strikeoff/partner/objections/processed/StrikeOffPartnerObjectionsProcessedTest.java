package uk.gov.companieshouse.strikeoff.partner.objections.processed;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class StrikeOffPartnerObjectionsProcessedTest {

    @Test
    void defaultConstructor_allowsSettingAndGettingAllFields() {
        StrikeOffPartnerObjectionsProcessed message = new StrikeOffPartnerObjectionsProcessed();
        LocalDate initialExpirationOn = LocalDate.of(2026, 12, 31);

        message.setStrikeOffEventId("strike-001");
        message.setSuccessFailureIndicator(SuccessFailureIndicator.SUCCESS);
        message.setErrorMessage("error");
        message.setEventType(EventType.OBJECTION);
        message.setInitialExpirationOn(initialExpirationOn);
        message.setCompanyNumber("12345678");

        assertEquals("strike-001", message.getStrikeOffEventId());
        assertSame(SuccessFailureIndicator.SUCCESS, message.getSuccessFailureIndicator());
        assertEquals("error", message.getErrorMessage());
        assertSame(EventType.OBJECTION, message.getEventType());
        assertEquals(initialExpirationOn, message.getInitialExpirationOn());
        assertEquals("12345678", message.getCompanyNumber());
    }

    @Test
    void allArgsConstructor_setsAllFields() {
        LocalDate initialExpirationOn = LocalDate.of(2026, 12, 31);

        StrikeOffPartnerObjectionsProcessed message = new StrikeOffPartnerObjectionsProcessed(
                "strike-002",
                SuccessFailureIndicator.FAILURE,
                "processing failed",
                EventType.WITHDRAWAL,
                initialExpirationOn,
                "87654321"
        );

        assertEquals("strike-002", message.getStrikeOffEventId());
        assertSame(SuccessFailureIndicator.FAILURE, message.getSuccessFailureIndicator());
        assertEquals("processing failed", message.getErrorMessage());
        assertSame(EventType.WITHDRAWAL, message.getEventType());
        assertEquals(initialExpirationOn, message.getInitialExpirationOn());
        assertEquals("87654321", message.getCompanyNumber());
    }

    @Test
    void setters_allowNullValues() {
        StrikeOffPartnerObjectionsProcessed message = new StrikeOffPartnerObjectionsProcessed(
                "strike-001",
                SuccessFailureIndicator.SUCCESS,
                "error",
                EventType.OBJECTION,
                LocalDate.of(2026, 12, 31),
                "12345678"
        );

        message.setStrikeOffEventId(null);
        message.setSuccessFailureIndicator(null);
        message.setErrorMessage(null);
        message.setEventType(null);
        message.setInitialExpirationOn(null);
        message.setCompanyNumber(null);

        assertNull(message.getStrikeOffEventId());
        assertNull(message.getSuccessFailureIndicator());
        assertNull(message.getErrorMessage());
        assertNull(message.getEventType());
        assertNull(message.getInitialExpirationOn());
        assertNull(message.getCompanyNumber());
    }
}

