package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;

import java.time.LocalDate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ProcessorTestFixtures {

    static final String OBJECTION_ID = "obj-001";
    static final String WITHDRAWAL_ID = "withdrawal-001";
    static final String COMPANY_NUMBER = "12345678";
    static final String EVENT_ID = "evt-001";
    static final String STRIKE_OFF_EVENT_ID = "strike-001";

    private ProcessorTestFixtures() {
    }

    static StrikeOffPartnerObjections mockIncomingMessage() {
        StrikeOffPartnerObjections message = mock(StrikeOffPartnerObjections.class);
        when(message.getEventId()).thenReturn(EVENT_ID);
        when(message.getCompanyNumber()).thenReturn(COMPANY_NUMBER);
        when(message.getStrikeOffEventId()).thenReturn(STRIKE_OFF_EVENT_ID);
        return message;
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
            ProcessedEventType eventType, SuccessFailureIndicator indicator) {
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

    static PrivateStrikeOffPartnerObjectionsResourceHandler mockResourceHandler(
            InternalApiClient internalApiClient) {
        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
        return handler;
    }
}
