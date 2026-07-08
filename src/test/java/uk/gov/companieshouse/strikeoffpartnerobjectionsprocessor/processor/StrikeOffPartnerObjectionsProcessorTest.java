package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.StrikeOffPartnerObjectionsApiClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StrikeOffPartnerObjectionsProcessorTest {

    private StrikeOffPartnerObjectionsApiClient apiClient;
    private StrikeOffPartnerObjectionsProcessor processor;

    @BeforeEach
    void setUp() {
        apiClient = mock(StrikeOffPartnerObjectionsApiClient.class);
        processor = new StrikeOffPartnerObjectionsProcessor(apiClient);
    }

    @Test
    void supportsObjections_butNotWithdrawals() {
        assertTrue(processor.supports(EventType.OBJECTION));
        assertFalse(processor.supports(EventType.WITHDRAWAL));
    }

    @Test
    void process_objectionSubmitted_updatesToProcessing() {
        StrikeOffPartnerObjections message = objectionMessage();
        when(apiClient.getObjectionProcessingStatus("00006401", "objection-001", "evt-001"))
                .thenReturn("objection-submitted");

        assertDoesNotThrow(() -> processor.process(message));

        verify(apiClient).getObjectionProcessingStatus("00006401", "objection-001", "evt-001");
        verify(apiClient).updateObjectionStatusToProcessing("00006401", "objection-001", "evt-001");
    }

    @Test
    void process_duplicateObjection_doesNotUpdateStateAgain() {
        StrikeOffPartnerObjections message = objectionMessage();
        when(apiClient.getObjectionProcessingStatus("00006401", "objection-001", "evt-001"))
                .thenReturn("objection-processing");

        assertDoesNotThrow(() -> processor.process(message));

        verify(apiClient).getObjectionProcessingStatus("00006401", "objection-001", "evt-001");
        verify(apiClient, never()).updateObjectionStatusToProcessing("00006401", "objection-001", "evt-001");
    }

    @Test
    void process_missingCompanyNumber_throwsInvalidMessage() {
        StrikeOffPartnerObjections message = objectionMessage();
        message.setSource(null);

        InvalidStrikeOffMessageException ex =
                assertThrows(InvalidStrikeOffMessageException.class, () -> processor.process(message));

        assertEquals("Missing companyNumber in source", ex.getMessage());
    }

    @Test
    void process_blankCompanyNumber_throwsInvalidMessage() {
        StrikeOffPartnerObjections message = objectionMessage();
        message.setSource("   ");

        InvalidStrikeOffMessageException ex =
                assertThrows(InvalidStrikeOffMessageException.class, () -> processor.process(message));

        assertEquals("Missing companyNumber in source", ex.getMessage());
    }

    @Test
    void process_missingObjectionId_throwsInvalidMessage() {
        StrikeOffPartnerObjections message = objectionMessage();
        message.setStrikeOffEventId(null);

        InvalidStrikeOffMessageException ex =
                assertThrows(InvalidStrikeOffMessageException.class, () -> processor.process(message));

        assertEquals("Missing objectionId", ex.getMessage());
    }

    @Test
    void process_blankObjectionId_throwsInvalidMessage() {
        StrikeOffPartnerObjections message = objectionMessage();
        message.setStrikeOffEventId("  ");

        InvalidStrikeOffMessageException ex =
                assertThrows(InvalidStrikeOffMessageException.class, () -> processor.process(message));

        assertEquals("Missing objectionId", ex.getMessage());
    }

    private StrikeOffPartnerObjections objectionMessage() {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-08T00:00:00Z")
                .setSource("00006401")
                .setEventType(EventType.OBJECTION)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("objection-001")
                .build();
    }
}
