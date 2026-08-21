package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.WITHDRAWAL;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.STRIKE_OFF_EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.incomingMessage;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.OBJECTION_ID;

class IncomingObjectionsProcessorTest {

    private IncomingObjectionsProcessor processor;

    @BeforeEach
    void setUp() {
        // Spy lets the test override inherited methods while keeping the rest of the class behavior intact
        processor = spy(new IncomingObjectionsProcessor(mock(InternalApiClient.class)));
    }

    @Test
    void eventTypeSupported_supportsOnlyObjections() {
        assertTrue(processor.eventTypeSupported(incomingMessage(OBJECTION)));
        assertFalse(processor.eventTypeSupported(incomingMessage(WITHDRAWAL)));
    }

    @Test
    void process_submittedObjection_updatesStatusToProcessing() {
        StrikeOffPartnerObjections message = incomingMessage(OBJECTION);
        BaseObjectionResponse objection = objectionWithStatus(
                ObjectionProcessingStatus.OBJECTION_SUBMITTED);
        doReturn(objection).when(processor).getObjectionDetails(message);
        doNothing().when(processor).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_PROCESSING);

        assertDoesNotThrow(() -> processor.process(message));

        verify(processor).getObjectionDetails(message);
        verify(processor).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_PROCESSING);
    }

    @Test
    void process_objectionAlreadyProcessing_throwsDuplicateWithoutUpdatingStatus() {
        StrikeOffPartnerObjections message = incomingMessage(OBJECTION);
        BaseObjectionResponse objection = objectionWithStatus(
                ObjectionProcessingStatus.OBJECTION_PROCESSING);
        doReturn(objection).when(processor).getObjectionDetails(message);

        DuplicateRecordException exception =
                assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(STRIKE_OFF_EVENT_ID));
        assertTrue(exception.getMessage().contains(OBJECTION_ID));
        assertTrue(exception.getMessage().contains(
                ObjectionProcessingStatus.OBJECTION_PROCESSING.getValue()));
        verify(processor, never()).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_PROCESSING);
    }

    private static BaseObjectionResponse objectionWithStatus(
            ObjectionProcessingStatus processingStatus) {
        return new BaseObjectionResponse()
                .objectionId(OBJECTION_ID)
                .processingStatus(processingStatus);
    }
}
