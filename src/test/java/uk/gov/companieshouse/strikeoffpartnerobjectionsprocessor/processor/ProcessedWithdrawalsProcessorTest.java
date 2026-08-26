package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
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

    @BeforeEach
    void setUp() {
        processor = spy(new ProcessedWithdrawalsProcessor(mock(InternalApiClient.class)));
    }

    @Test
    void eventTypeSupported_supportsOnlyWithdrawals() {
        assertTrue(processor.eventTypeSupported(processedMessage(WITHDRAWAL, SUCCESS)));
        assertFalse(processor.eventTypeSupported(processedMessage(OBJECTION, SUCCESS)));
        assertTrue(processor.eventTypeSupported(processedMessage(WITHDRAWAL, FAILURE)));
        assertFalse(processor.eventTypeSupported(processedMessage(OBJECTION, FAILURE)));
    }

    @Test
    void process_successfulWithdrawal_updatesStatusToAccepted() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, SUCCESS);
        stubProcessingWithdrawal(message);
        doNothing().when(processor).updateWithdrawalStatus(
                eq(message), any(UpdateWithdrawalStatusRequest.class));

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);

        verify(processor).getWithdrawalDetails(message);
        verify(processor).updateWithdrawalStatus(eq(message), requestCaptor.capture());
        assertEquals(WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED,
                requestCaptor.getValue().getProcessingStatus());
        assertNull(requestCaptor.getValue().getFailureReason());
    }

    @Test
    void process_failedWithdrawal_updatesStatusToRejected() {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, FAILURE);
        stubProcessingWithdrawal(message);
        doNothing().when(processor).updateWithdrawalStatus(
                eq(message), any(UpdateWithdrawalStatusRequest.class));

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);

        verify(processor).getWithdrawalDetails(message);
        verify(processor).updateWithdrawalStatus(eq(message), requestCaptor.capture());
        assertEquals(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED,
                requestCaptor.getValue().getProcessingStatus());
        assertEquals(message.getErrorMessage(), requestCaptor.getValue().getFailureReason());
    }

    @ParameterizedTest
    @EnumSource(
            value = WithdrawalProcessingStatus.class,
            names = {"WITHDRAWAL_ACCEPTED", "WITHDRAWAL_REJECTED"})
    void process_terminalWithdrawal_throwsDuplicateWithoutUpdatingStatus(
            WithdrawalProcessingStatus terminalStatus) {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(WITHDRAWAL, SUCCESS);
        doReturn(withdrawalWithStatus(terminalStatus)).when(processor).getWithdrawalDetails(message);

        DuplicateRecordException exception =
                assertThrows(DuplicateRecordException.class, () -> processor.process(message));

        assertTrue(exception.getMessage().contains(STRIKE_OFF_EVENT_ID));
        assertTrue(exception.getMessage().contains(WITHDRAWAL_ID));
        assertTrue(exception.getMessage().contains(terminalStatus.getValue()));
        verify(processor, never()).updateWithdrawalStatus(
                eq(message), any(UpdateWithdrawalStatusRequest.class));
    }

    private void stubProcessingWithdrawal(StrikeOffPartnerObjectionsProcessed message) {
        doReturn(withdrawalWithStatus(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING))
                .when(processor).getWithdrawalDetails(message);
    }

    private static WithdrawAllObjectionsResponse withdrawalWithStatus(
            WithdrawalProcessingStatus processingStatus) {
        return new WithdrawAllObjectionsResponse()
                .withdrawalId(WITHDRAWAL_ID)
                .processingStatus(processingStatus);
    }
}

