package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.WITHDRAWAL;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.OBJECTION_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.STRIKE_OFF_EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.incomingMessage;

class IncomingObjectionsProcessorTest {

    private IncomingObjectionsProcessor processor;
    private final ChipsPartnerObjectionsSubmissionClient chipsPartnerObjectionsSubmissionClient =
            mock(ChipsPartnerObjectionsSubmissionClient.class);

    @BeforeEach
    void setUp() {
        // Spy lets the test override inherited methods while keeping the rest of the class behavior intact
        processor = spy(new IncomingObjectionsProcessor(mock(InternalApiClient.class), chipsPartnerObjectionsSubmissionClient));
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

    @Test
    void process_chipsSubmission400_isNonRetryable() {
        StrikeOffPartnerObjections message = incomingMessage(OBJECTION);
        BaseObjectionResponse objection = objectionWithStatus(
                ObjectionProcessingStatus.OBJECTION_SUBMITTED);
        doReturn(objection).when(processor).getObjectionDetails(message);
        doNothing().when(processor).updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_PROCESSING);
        doThrow(new ChipsSubmissionException("bad request", 400))
                .when(chipsPartnerObjectionsSubmissionClient)
                .submit(message);

        InvalidStrikeOffMessageException exception = assertThrows(
                InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(exception.getMessage().contains("Non-retryable API error (status=400)"));
        verify(chipsPartnerObjectionsSubmissionClient).submit(message);
    }

    @Test
    void process_duplicateObjection_doesNotSubmitToChips() {
        StrikeOffPartnerObjections message = incomingMessage(OBJECTION);
        BaseObjectionResponse objection = objectionWithStatus(
                ObjectionProcessingStatus.OBJECTION_PROCESSING);
        doReturn(objection).when(processor).getObjectionDetails(message);

        assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        verify(chipsPartnerObjectionsSubmissionClient, never()).submit(any());
    }

    private static BaseObjectionResponse objectionWithStatus(
            ObjectionProcessingStatus processingStatus) {
        return new BaseObjectionResponse()
                .objectionId(OBJECTION_ID)
                .processingStatus(processingStatus);
    }
}
