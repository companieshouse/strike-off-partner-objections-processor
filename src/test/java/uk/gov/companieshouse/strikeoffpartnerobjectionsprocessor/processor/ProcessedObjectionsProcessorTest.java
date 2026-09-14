package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcCallbackException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcOutcomeCallbackClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcOutcomeCallbackRequest;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
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
    private HmrcOutcomeCallbackClient hmrcOutcomeCallbackClient;

    @BeforeEach
    void setUp() {
        hmrcOutcomeCallbackClient = mock(HmrcOutcomeCallbackClient.class);
        processor = spy(new ProcessedObjectionsProcessor(
                mock(InternalApiClient.class), hmrcOutcomeCallbackClient));
    }

    @Test
    void eventTypeSupported_supportsOnlyObjections() {
        assertTrue(processor.eventTypeSupported(processedMessage(OBJECTION, SUCCESS)));
        assertFalse(processor.eventTypeSupported(processedMessage(WITHDRAWAL, SUCCESS)));
        assertTrue(processor.eventTypeSupported(processedMessage(OBJECTION, FAILURE)));
        assertFalse(processor.eventTypeSupported(processedMessage(WITHDRAWAL, FAILURE)));
    }

    @Test
    void process_successfulObjection_updatesStatusToAccepted_andSubmitsOutcomeCallback() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, SUCCESS);
        stubSubmittedObjection(message);
        doNothing().when(processor).updateObjectionStatus(eq(message), any(UpdateObjectionStatusRequest.class));

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateObjectionStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateObjectionStatusRequest.class);

        verify(processor).getObjectionDetails(message);
        verify(processor).updateObjectionStatus(eq(message), requestCaptor.capture());
        assertEquals(ObjectionProcessingStatus.OBJECTION_ACCEPTED, requestCaptor.getValue().getProcessingStatus());
        assertNotNull(requestCaptor.getValue().getInitialExpirationOn());
        assertNull(requestCaptor.getValue().getFailureReason());
        verify(hmrcOutcomeCallbackClient).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_failedObjection_updatesStatusToRejected_andSubmitsOutcomeCallback() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, FAILURE);
        stubSubmittedObjection(message);
        doNothing().when(processor).updateObjectionStatus(eq(message), any(UpdateObjectionStatusRequest.class));

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateObjectionStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateObjectionStatusRequest.class);

        verify(processor).getObjectionDetails(message);
        verify(processor).updateObjectionStatus(eq(message), requestCaptor.capture());
        assertEquals(ObjectionProcessingStatus.OBJECTION_REJECTED, requestCaptor.getValue().getProcessingStatus());
        assertEquals(message.getErrorMessage(), requestCaptor.getValue().getFailureReason());
        assertNull(requestCaptor.getValue().getInitialExpirationOn());
        verify(hmrcOutcomeCallbackClient).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_terminalObjectionWithConflictingStatus_throwsDuplicateWithoutUpdatingStatus() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, SUCCESS);
        doReturn(objectionWithStatus(ObjectionProcessingStatus.OBJECTION_REJECTED))
                .when(processor).getObjectionDetails(message);

        DuplicateRecordException exception =
                assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(STRIKE_OFF_EVENT_ID));
        assertTrue(exception.getMessage().contains(OBJECTION_ID));
        assertTrue(exception.getMessage().contains(ObjectionProcessingStatus.OBJECTION_REJECTED.getValue()));
        verify(processor, never()).updateObjectionStatus(eq(message), any(UpdateObjectionStatusRequest.class));
        verify(hmrcOutcomeCallbackClient, never()).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_whenStatusAlreadyMatchesTarget_skipsUpdateAndStillSendsCallback() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, SUCCESS);
        doReturn(objectionWithStatus(ObjectionProcessingStatus.OBJECTION_ACCEPTED))
                .when(processor).getObjectionDetails(message);

        assertDoesNotThrow(() -> processor.process(message));

        verify(processor, never()).updateObjectionStatus(eq(message), any(UpdateObjectionStatusRequest.class));
        verify(hmrcOutcomeCallbackClient).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_objectionNotFound_throwsDuplicateWithoutUpdatingStatus() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, SUCCESS);
        doThrow(new InvalidStrikeOffMessageException(
                "Non-retryable API error (status=404) for eventId=" + STRIKE_OFF_EVENT_ID))
                .when(processor).getObjectionDetails(message);

        DuplicateRecordException exception =
                assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(STRIKE_OFF_EVENT_ID));
        verify(processor, never()).updateObjectionStatus(eq(message), any(UpdateObjectionStatusRequest.class));
        verify(hmrcOutcomeCallbackClient, never()).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_whenCallbackFailsWith503_throwsRetryableException() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(OBJECTION, SUCCESS);
        stubSubmittedObjection(message);
        doNothing().when(processor).updateObjectionStatus(eq(message), any(UpdateObjectionStatusRequest.class));
        doThrow(new HmrcCallbackException("service unavailable", 503))
                .when(hmrcOutcomeCallbackClient)
                .submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> processor.process(message));

        assertEquals(
                "Retryable API error (status=503) for eventId=" + message.getStrikeOffEventId(),
                exception.getMessage());
    }

    private void stubSubmittedObjection(StrikeOffPartnerObjectionsProcessed message) {
        doReturn(objectionWithStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED))
                .when(processor).getObjectionDetails(message);
    }

    private static BaseObjectionResponse objectionWithStatus(ObjectionProcessingStatus processingStatus) {
        return new BaseObjectionResponse()
                .objectionId(OBJECTION_ID)
                .processingStatus(processingStatus);
    }
}
