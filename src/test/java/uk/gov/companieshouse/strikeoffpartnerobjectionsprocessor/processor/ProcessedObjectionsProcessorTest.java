package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
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
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.WITHDRAWAL;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.OBJECTION_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.STRIKE_OFF_EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.processedMessage;

class ProcessedObjectionsProcessorTest {

    private ProcessedObjectionsProcessor processor;

    @BeforeEach
    void setUp() {
        // Spy lets the test override inherited methods while retaining this class's behavior.
        processor = spy(new ProcessedObjectionsProcessor(mock(InternalApiClient.class)));
    }

    @Test
    void eventTypeSupported_supportsOnlyObjections() {
        assertTrue(processor.eventTypeSupported(processedMessage(OBJECTION, SUCCESS)));
        assertFalse(processor.eventTypeSupported(processedMessage(WITHDRAWAL, SUCCESS)));
        assertTrue(processor.eventTypeSupported(processedMessage(OBJECTION, FAILURE)));
        assertFalse(processor.eventTypeSupported(processedMessage(WITHDRAWAL, FAILURE)));
    }

    @Test
    void process_successfulObjection_updatesStatusToAccepted() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, SUCCESS);
        stubSubmittedObjection(message);
        doNothing().when(processor).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_ACCEPTED);

        assertDoesNotThrow(() -> processor.process(message));

        verify(processor).getObjectionDetails(message);
        verify(processor).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_ACCEPTED);
    }

    @Test
    void process_failedObjection_updatesStatusToRejected() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, FAILURE);
        stubSubmittedObjection(message);
        doNothing().when(processor).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_REJECTED);

        assertDoesNotThrow(() -> processor.process(message));

        verify(processor).getObjectionDetails(message);
        verify(processor).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_REJECTED);
    }

    @ParameterizedTest
    @EnumSource(
            value = ObjectionProcessingStatus.class,
            names = {"OBJECTION_ACCEPTED", "OBJECTION_REJECTED"})
    void process_terminalObjection_throwsDuplicateWithoutUpdatingStatus(
            ObjectionProcessingStatus terminalStatus) {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, SUCCESS);
        doReturn(objectionWithStatus(terminalStatus)).when(processor).getObjectionDetails(message);

        DuplicateRecordException exception =
                assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(STRIKE_OFF_EVENT_ID));
        assertTrue(exception.getMessage().contains(OBJECTION_ID));
        assertTrue(exception.getMessage().contains(terminalStatus.getValue()));
        verify(processor, never()).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_ACCEPTED);
        verify(processor, never()).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_REJECTED);
    }

    private void stubSubmittedObjection(StrikeOffPartnerObjectionsProcessed message) {
        doReturn(objectionWithStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED))
                .when(processor).getObjectionDetails(message);
    }

    private static BaseObjectionResponse objectionWithStatus(
            ObjectionProcessingStatus processingStatus) {
        return new BaseObjectionResponse()
                .objectionId(OBJECTION_ID)
                .processingStatus(processingStatus);
    }
}
