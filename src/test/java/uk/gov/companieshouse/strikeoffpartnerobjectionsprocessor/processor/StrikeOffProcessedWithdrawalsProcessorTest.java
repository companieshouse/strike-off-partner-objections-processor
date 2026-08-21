package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.UpdateWithdrawalStatus;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StrikeOffProcessedWithdrawalsProcessorTest {

    private final InternalApiClient internalApiClient = mock(InternalApiClient.class);
    private final StrikeOffProcessedWithdrawalsProcessor processor =
            new StrikeOffProcessedWithdrawalsProcessor(internalApiClient);

    @Test
    void process_successWithdrawal_setsAccepted() throws Exception {
        PrivateStrikeOffPartnerObjectionsResourceHandler handler = stubUpdateSuccess();
        StrikeOffPartnerObjectionsProcessed message = message(EventType.WITHDRAWAL, SuccessFailureIndicator.SUCCESS);

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);
        verify(handler).updateWithdrawalStatus(anyString(), requestCaptor.capture());
        assertSame(WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED, requestCaptor.getValue().getProcessingStatus());
    }

    @Test
    void process_failedWithdrawal_setsRejectedAndFailureReason() throws Exception {
        PrivateStrikeOffPartnerObjectionsResourceHandler handler = stubUpdateSuccess();
        StrikeOffPartnerObjectionsProcessed message = message(EventType.WITHDRAWAL, SuccessFailureIndicator.FAILURE);

        assertDoesNotThrow(() -> processor.process(message));

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);
        verify(handler).updateWithdrawalStatus(anyString(), requestCaptor.capture());

        UpdateWithdrawalStatusRequest request = requestCaptor.getValue();
        assertSame(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED, request.getProcessingStatus());
        assertEquals("processing failed", request.getFailureReason());
    }

    @Test
    void process_nonWithdrawalEvent_ignored() {
        StrikeOffPartnerObjectionsProcessed message = message(EventType.OBJECTION, SuccessFailureIndicator.SUCCESS);

        assertDoesNotThrow(() -> processor.process(message));
        verifyNoInteractions(internalApiClient);
    }

    @Test
    void process_4xxApiErrors_areNonRetryable() throws Exception {
        int[] statuses = {400, 404};
        for (int status : statuses) {
            stubUpdateError(status);
            StrikeOffPartnerObjectionsProcessed message = message(EventType.WITHDRAWAL, SuccessFailureIndicator.SUCCESS);

            try {
                processor.process(message);
                fail("Expected InvalidStrikeOffMessageException for status " + status);
            } catch (InvalidStrikeOffMessageException ex) {
                assertTrue(ex.getMessage().contains("Non-retryable API error"));
            }
        }
    }

    @Test
    void process_429And5xxApiErrors_areRetryable() throws Exception {
        int[] statuses = {429, 500};
        for (int status : statuses) {
            stubUpdateError(status);
            StrikeOffPartnerObjectionsProcessed message = message(EventType.WITHDRAWAL, SuccessFailureIndicator.SUCCESS);

            try {
                processor.process(message);
                fail("Expected RuntimeException for status " + status);
            } catch (RuntimeException ex) {
                assertFalse(ex instanceof InvalidStrikeOffMessageException);
                assertTrue(ex.getMessage().contains("Retryable API error"));
            }
        }
    }

    private PrivateStrikeOffPartnerObjectionsResourceHandler stubUpdateSuccess() throws Exception {
        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

        UpdateWithdrawalStatus update = mock(UpdateWithdrawalStatus.class);
        when(update.execute()).thenReturn(new ApiResponse<>(204, null, null));
        when(handler.updateWithdrawalStatus(anyString(), any(UpdateWithdrawalStatusRequest.class))).thenReturn(update);
        return handler;
    }

    private void stubUpdateError(int status) throws Exception {
        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);

        UpdateWithdrawalStatus update = mock(UpdateWithdrawalStatus.class);
        when(update.execute()).thenThrow(apiEx);
        when(handler.updateWithdrawalStatus(anyString(), any(UpdateWithdrawalStatusRequest.class))).thenReturn(update);
    }

    private StrikeOffPartnerObjectionsProcessed message(EventType eventType, SuccessFailureIndicator outcome) {
        String errorMessage = outcome == SuccessFailureIndicator.FAILURE ? "processing failed" : null;
        return new StrikeOffPartnerObjectionsProcessed(
                "strike-002",
                outcome,
                errorMessage,
                eventType,
                LocalDate.of(2026, 12, 31),
                "12345678"
        );
    }
}

