package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration;

import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;

import java.time.LocalDate;

final class IntegrationTestFixtures {
    static final String COMPANY_NUMBER = "12345678";
    static final String EVENT_ID = "evt-001";
    static final String STRIKE_OFF_EVENT_ID = "strike-001";
    static final String INCOMING_TOPIC = "strike-off-partner-objections-incoming";
    static final String PROCESSED_TOPIC = "strike-off-partner-objections-processed";

    private IntegrationTestFixtures() {
    }

    static StrikeOffPartnerObjections incomingMessage(EventType eventType) {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId(EVENT_ID)
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setCompanyNumber(COMPANY_NUMBER)
                .setEventType(eventType)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId(STRIKE_OFF_EVENT_ID)
                .build();
    }

    static StrikeOffPartnerObjectionsProcessed processedMessage(
            ProcessedEventType eventType,
            SuccessFailureIndicator indicator) {
        boolean succeeded = indicator == SuccessFailureIndicator.SUCCESS;
        return StrikeOffPartnerObjectionsProcessed.newBuilder()
                .setEventType(eventType)
                .setCompanyNumber(COMPANY_NUMBER)
                .setSuccessFailureIndicator(indicator)
                .setErrorMessage(succeeded ? null : "Processing failed")
                .setInitialExpirationOn(succeeded ? LocalDate.parse("2026-07-06") : null)
                .setStrikeOffEventId(STRIKE_OFF_EVENT_ID)
                .build();
    }
}
