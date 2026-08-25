package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.WITHDRAWAL;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.STRIKE_OFF_EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.WITHDRAWAL_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.incomingMessage;

class IncomingWithdrawalsProcessorTest {

    private IncomingWithdrawalsProcessor processor;
    private final ChipsPartnerObjectionsSubmissionClient chipsPartnerObjectionsSubmissionClient =
            mock(ChipsPartnerObjectionsSubmissionClient.class);

    @BeforeEach
    void setUp() {
        // Spy lets the test override inherited methods while retaining this class's behavior.
        processor = spy(new IncomingWithdrawalsProcessor(mock(InternalApiClient.class), chipsPartnerObjectionsSubmissionClient));
    }

    @Test
    void eventTypeSupported_supportsOnlyWithdrawals() {
        assertTrue(processor.eventTypeSupported(incomingMessage(WITHDRAWAL)));
        assertFalse(processor.eventTypeSupported(incomingMessage(OBJECTION)));
    }

    @Test
    void process_requestedWithdrawal_updatesStatusToProcessing() {
        StrikeOffPartnerObjections message = incomingMessage(WITHDRAWAL);
        WithdrawAllObjectionsResponse withdrawal = withdrawalWithStatus(
                WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        doReturn(withdrawal).when(processor).getWithdrawalDetails(message);
        doNothing().when(processor).updateWithdrawalStatus(
                eq(message), any(UpdateWithdrawalStatusRequest.class));

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);

        verify(processor).getWithdrawalDetails(message);
        verify(processor).updateWithdrawalStatus(eq(message), requestCaptor.capture());
        assertEquals(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING,
                requestCaptor.getValue().getProcessingStatus());
    }

    @Test
    void process_withdrawalAlreadyProcessing_throwsDuplicateWithoutUpdatingStatus() {
        StrikeOffPartnerObjections message = incomingMessage(WITHDRAWAL);
        WithdrawAllObjectionsResponse withdrawal = withdrawalWithStatus(
                WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING);
        doReturn(withdrawal).when(processor).getWithdrawalDetails(message);

        DuplicateRecordException exception =
                assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(STRIKE_OFF_EVENT_ID));
        assertTrue(exception.getMessage().contains(WITHDRAWAL_ID));
        assertTrue(exception.getMessage().contains(
                WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING.getValue()));
        verify(processor, never()).updateWithdrawalStatus(
                eq(message), any(UpdateWithdrawalStatusRequest.class));
    }

    @Test
    void process_withdrawalInUnexpectedStatus_throwsInvalidMessageWithoutUpdatingStatus() {
        StrikeOffPartnerObjections message = incomingMessage(WITHDRAWAL);
        WithdrawAllObjectionsResponse withdrawal = withdrawalWithStatus(
                WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED);
        doReturn(withdrawal).when(processor).getWithdrawalDetails(message);

        InvalidStrikeOffMessageException exception = assertThrows(
                InvalidStrikeOffMessageException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(
                WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED.getValue()));
        assertTrue(exception.getMessage().contains("expected=WITHDRAWAL_REQUESTED"));
        assertTrue(exception.getMessage().contains(WITHDRAWAL_ID));
        verify(processor, never()).updateWithdrawalStatus(
                eq(message), any(UpdateWithdrawalStatusRequest.class));
    }

    @Test
    void process_chipsSubmission403_isNonRetryable() {
        StrikeOffPartnerObjections message = incomingMessage(WITHDRAWAL);
        WithdrawAllObjectionsResponse withdrawal = withdrawalWithStatus(
                WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        doReturn(withdrawal).when(processor).getWithdrawalDetails(message);
        doNothing().when(processor).updateWithdrawalStatus(
                eq(message), any(UpdateWithdrawalStatusRequest.class));
        doThrow(new ChipsSubmissionException("forbidden", 403))
                .when(chipsPartnerObjectionsSubmissionClient)
                .submit(any());

        InvalidStrikeOffMessageException exception = assertThrows(
                InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(exception.getMessage().contains("Non-retryable API error (status=403)"));
        verify(chipsPartnerObjectionsSubmissionClient).submit(message);
    }

    private static WithdrawAllObjectionsResponse withdrawalWithStatus(
            WithdrawalProcessingStatus processingStatus) {
        return new WithdrawAllObjectionsResponse()
                .withdrawalId(WITHDRAWAL_ID)
                .processingStatus(processingStatus);
    }
}
