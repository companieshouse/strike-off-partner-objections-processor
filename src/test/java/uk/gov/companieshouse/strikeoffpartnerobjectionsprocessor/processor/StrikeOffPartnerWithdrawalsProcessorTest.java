package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.GetAllWithdrawals;
import uk.gov.companieshouse.api.handler.objections.request.UpdateWithdrawalStatus;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StrikeOffPartnerWithdrawalsProcessorTest {

    private final InternalApiClient internalApiClient = mock(InternalApiClient.class);
    private final StrikeOffPartnerWithdrawalsProcessor processor =
            new StrikeOffPartnerWithdrawalsProcessor(internalApiClient);

    @Test
    void supportsWithdrawals_butNotObjections() {
        assertTrue(processor.supports(EventType.WITHDRAWAL));
        assertFalse(processor.supports(EventType.OBJECTION));
    }

    @Test
    void doProcess_validMessage_callsFetchAndUpdateStatus() throws Exception {
        WithdrawAllObjectionsResponse response = new WithdrawAllObjectionsResponse()
                .withdrawalId("withdrawal-001")
                .processingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                stubGetAndUpdateSuccess(response);

        assertDoesNotThrow(() -> processor.process(withdrawalMessage()));

        verify(handler).getAllWithdrawals(anyString());
        verify(handler).updateWithdrawalStatus(anyString(), any(UpdateWithdrawalStatusRequest.class));

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);
        verify(handler).updateWithdrawalStatus(anyString(), requestCaptor.capture());
        assertSame(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING, requestCaptor.getValue().getProcessingStatus());
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 429, 503})
    void doProcess_retryableApiError_isRetryable(int status) throws Exception {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);
        stubWithdrawalCallThrowing(apiEx);
        StrikeOffPartnerObjections message = withdrawalMessage();

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> processor.process(message));

        assertFalse(ex instanceof InvalidStrikeOffMessageException);
        assertTrue(ex.getMessage().contains("Retryable API error"));
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 404})
    void doProcess_nonRetryableApiError_isNonRetryable(int status) throws Exception {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(status);
        stubWithdrawalCallThrowing(apiEx);
        StrikeOffPartnerObjections message = withdrawalMessage();

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("Non-retryable API error"));
    }


    @Test
    void doProcess_uriValidationError_isNonRetryable() throws Exception {
        stubWithdrawalCallThrowing(mock(URIValidationException.class));
        StrikeOffPartnerObjections message = withdrawalMessage();

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("Non-retryable URI validation error"));
    }

    @Test
    void doProcess_unknownException_isRetryable() throws Exception {
        stubWithdrawalCallThrowing(new IllegalStateException("connection reset"));
        StrikeOffPartnerObjections message = withdrawalMessage();

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> processor.process(message));

        assertFalse(ex instanceof InvalidStrikeOffMessageException);
        assertTrue(ex.getMessage().contains("Retryable error"));
    }

    @Test
    void doProcess_ShouldThrowDuplicateRecordException_WhenWithdrawalAlreadyProcessed() throws Exception {
        WithdrawAllObjectionsResponse withdrawalResponse =
                new WithdrawAllObjectionsResponse();
        withdrawalResponse.setWithdrawalId("withdrawal-123");
        withdrawalResponse.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING);
        StrikeOffPartnerObjections message = withdrawalMessage();

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                stubGetOnly(withdrawalResponse);

        DuplicateRecordException exception = assertThrows(
                DuplicateRecordException.class,
                () -> processor.process(message));

        assertTrue(exception.getMessage()
                .contains("Duplicate/complete Withdrawal skipped"));
        verify(handler, never()).updateWithdrawalStatus(anyString(), any(UpdateWithdrawalStatusRequest.class));
    }

    @Test
    void doProcess_updateWithdrawalStatus_apiError500_isRetryable() throws Exception {
        WithdrawAllObjectionsResponse response = new WithdrawAllObjectionsResponse()
                .withdrawalId("withdrawal-500")
                .processingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        StrikeOffPartnerObjections message = withdrawalMessage();

        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(500);
        stubWithdrawalUpdateThrowing(response, apiEx);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> processor.process(message));

        assertFalse(ex instanceof InvalidStrikeOffMessageException);
        assertTrue(ex.getMessage().contains("Retryable API error"));
    }

    @Test
    void doProcess_updateWithdrawalStatus_apiError404_isNonRetryable() throws Exception {
        WithdrawAllObjectionsResponse response = new WithdrawAllObjectionsResponse()
                .withdrawalId("withdrawal-404")
                .processingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        StrikeOffPartnerObjections message = withdrawalMessage();

        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(404);
        stubWithdrawalUpdateThrowing(response, apiEx);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("Non-retryable API error"));
    }

    @Test
    void doProcess_updateWithdrawalStatus_uriValidationError_isNonRetryable() throws Exception {
        WithdrawAllObjectionsResponse response = new WithdrawAllObjectionsResponse()
                .withdrawalId("withdrawal-uri")
                .processingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        StrikeOffPartnerObjections message = withdrawalMessage();

        stubWithdrawalUpdateThrowing(response, mock(URIValidationException.class));

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("Non-retryable URI validation error"));
    }

    @Test
    void doProcess_updateWithdrawalStatus_unknownException_isRetryable() throws Exception {
        WithdrawAllObjectionsResponse response = new WithdrawAllObjectionsResponse()
                .withdrawalId("withdrawal-unknown")
                .processingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        StrikeOffPartnerObjections message = withdrawalMessage();

        stubWithdrawalUpdateThrowing(response, new IllegalStateException("Unknown error"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> processor.process(message));

        assertFalse(ex instanceof InvalidStrikeOffMessageException);
        assertTrue(ex.getMessage().contains("Retryable error"));
    }

    // --- helpers ---

    private PrivateStrikeOffPartnerObjectionsResourceHandler stubHandler(GetAllWithdrawals get) {
        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
        when(handler.getAllWithdrawals(anyString())).thenReturn(get);
        return handler;
    }

    private void stubWithdrawalCallThrowing(Exception toThrow) throws Exception {
        GetAllWithdrawals get = mock(GetAllWithdrawals.class);
        when(get.execute()).thenThrow(toThrow);
        stubHandler(get);
    }

    private PrivateStrikeOffPartnerObjectionsResourceHandler stubGetOnly(
            WithdrawAllObjectionsResponse responseBody) throws Exception {
        GetAllWithdrawals get = mock(GetAllWithdrawals.class);
        when(get.execute()).thenReturn(new ApiResponse<>(200, null, responseBody));
        return stubHandler(get);
    }

    private PrivateStrikeOffPartnerObjectionsResourceHandler stubGetAndUpdateSuccess(
            WithdrawAllObjectionsResponse responseBody) throws Exception {
        PrivateStrikeOffPartnerObjectionsResourceHandler handler = stubGetOnly(responseBody);

        UpdateWithdrawalStatus update = mock(UpdateWithdrawalStatus.class);
        when(update.execute()).thenReturn(new ApiResponse<>(204, null, null));
        when(handler.updateWithdrawalStatus(anyString(), any(UpdateWithdrawalStatusRequest.class))).thenReturn(update);
        return handler;
    }

    private void stubWithdrawalUpdateThrowing(
            WithdrawAllObjectionsResponse responseBody,
            Exception toThrow) throws Exception {
        GetAllWithdrawals get = mock(GetAllWithdrawals.class);
        when(get.execute()).thenReturn(new ApiResponse<>(200, null, responseBody));
        PrivateStrikeOffPartnerObjectionsResourceHandler handler = stubHandler(get);

        UpdateWithdrawalStatus update = mock(UpdateWithdrawalStatus.class);
        when(update.execute()).thenThrow(toThrow);
        when(handler.updateWithdrawalStatus(anyString(), any(UpdateWithdrawalStatusRequest.class))).thenReturn(update);
    }

    private StrikeOffPartnerObjections withdrawalMessage() {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-002")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setCompanyNumber("12345678")
                .setEventType(EventType.WITHDRAWAL)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-002")
                .build();
    }
}