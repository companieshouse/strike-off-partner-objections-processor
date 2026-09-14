package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcCallbackException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcOutcomeCallbackClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcOutcomeCallbackRequest;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.STRIKE_OFF_EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.WITHDRAWAL_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.processedMessage;

class ProcessedWithdrawalsProcessorTest {

    private ProcessedWithdrawalsProcessor processor;
    private HmrcOutcomeCallbackClient hmrcOutcomeCallbackClient;

    @BeforeEach
    void setUp() {
        hmrcOutcomeCallbackClient = mock(HmrcOutcomeCallbackClient.class);
        processor = spy(new ProcessedWithdrawalsProcessor(
                mock(InternalApiClient.class), hmrcOutcomeCallbackClient));
    }

    @Test
    void eventTypeSupported_supportsOnlyWithdrawals() {
        assertTrue(processor.eventTypeSupported(processedMessage(WITHDRAWAL, SUCCESS)));
        assertFalse(processor.eventTypeSupported(processedMessage(OBJECTION, SUCCESS)));
        assertTrue(processor.eventTypeSupported(processedMessage(WITHDRAWAL, FAILURE)));
        assertFalse(processor.eventTypeSupported(processedMessage(OBJECTION, FAILURE)));
    }

    @Test
    void process_successfulWithdrawal_updatesStatusToAccepted_andSubmitsOutcomeCallback() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, SUCCESS);
        stubProcessingWithdrawal(message);
        doNothing().when(processor).updateWithdrawalStatus(eq(message), any(UpdateWithdrawalStatusRequest.class));

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);

        verify(processor).getWithdrawalDetails(message);
        verify(processor).updateWithdrawalStatus(eq(message), requestCaptor.capture());
        assertEquals(WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED, requestCaptor.getValue().getProcessingStatus());
        assertNull(requestCaptor.getValue().getFailureReason());
        verify(hmrcOutcomeCallbackClient).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_failedWithdrawal_updatesStatusToRejected_andSubmitsOutcomeCallback() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, FAILURE);
        stubProcessingWithdrawal(message);
        doNothing().when(processor).updateWithdrawalStatus(eq(message), any(UpdateWithdrawalStatusRequest.class));

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);

        verify(processor).getWithdrawalDetails(message);
        verify(processor).updateWithdrawalStatus(eq(message), requestCaptor.capture());
        assertEquals(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED, requestCaptor.getValue().getProcessingStatus());
        assertEquals(message.getErrorMessage(), requestCaptor.getValue().getFailureReason());
        verify(hmrcOutcomeCallbackClient).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_terminalWithdrawalWithConflictingStatus_throwsDuplicateWithoutUpdatingStatus() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, SUCCESS);
        doReturn(withdrawalWithStatus(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED))
                .when(processor).getWithdrawalDetails(message);

        DuplicateRecordException exception =
                assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(STRIKE_OFF_EVENT_ID));
        assertTrue(exception.getMessage().contains(WITHDRAWAL_ID));
        assertTrue(exception.getMessage().contains(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED.getValue()));
        verify(processor, never()).updateWithdrawalStatus(eq(message), any(UpdateWithdrawalStatusRequest.class));
        verify(hmrcOutcomeCallbackClient, never()).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_whenStatusAlreadyMatchesTarget_skipsUpdateAndStillSendsCallback() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, FAILURE);
        doReturn(withdrawalWithStatus(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED))
                .when(processor).getWithdrawalDetails(message);

        assertDoesNotThrow(() -> processor.process(message));

        verify(processor, never()).updateWithdrawalStatus(eq(message), any(UpdateWithdrawalStatusRequest.class));
        verify(hmrcOutcomeCallbackClient).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_withdrawalNotFound_throwsDuplicateWithoutUpdatingStatus() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, SUCCESS);
        doThrow(new InvalidStrikeOffMessageException(
                "Non-retryable API error (status=404) for eventId=" + STRIKE_OFF_EVENT_ID))
                .when(processor).getWithdrawalDetails(message);

        DuplicateRecordException exception =
                assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(STRIKE_OFF_EVENT_ID));
        verify(processor, never()).updateWithdrawalStatus(eq(message), any(UpdateWithdrawalStatusRequest.class));
        verify(hmrcOutcomeCallbackClient, never()).submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));
    }

    @Test
    void process_whenCallbackFailsWith503_throwsRetryableException() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, SUCCESS);
        stubProcessingWithdrawal(message);
        doNothing().when(processor).updateWithdrawalStatus(eq(message), any(UpdateWithdrawalStatusRequest.class));
        doThrow(new HmrcCallbackException("service unavailable", 503))
                .when(hmrcOutcomeCallbackClient)
                .submitOutcomeCallback(any(HmrcOutcomeCallbackRequest.class));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> processor.process(message));

        assertEquals(
                "Retryable API error (status=503) for eventId=" + message.getStrikeOffEventId(),
                exception.getMessage());
    }

    private void stubProcessingWithdrawal(StrikeOffPartnerObjectionsProcessed message) {
        doReturn(withdrawalWithStatus(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING))
                .when(processor).getWithdrawalDetails(message);
    }

    private static WithdrawAllObjectionsResponse withdrawalWithStatus(WithdrawalProcessingStatus processingStatus) {
        return new WithdrawAllObjectionsResponse()
                .withdrawalId(WITHDRAWAL_ID)
                .processingStatus(processingStatus);
    }
}
