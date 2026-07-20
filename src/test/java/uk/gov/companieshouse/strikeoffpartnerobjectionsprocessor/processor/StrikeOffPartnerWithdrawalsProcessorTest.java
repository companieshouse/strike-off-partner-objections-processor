package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.GetAllWithdrawals;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
        // Given
        StrikeOffPartnerObjections message = new StrikeOffPartnerObjections();
        message.setEventId("event-123");

        WithdrawAllObjectionsResponse withdrawalResponse =
                new WithdrawAllObjectionsResponse();
        withdrawalResponse.setWithdrawalId("withdrawal-123");
        withdrawalResponse.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING);

        @SuppressWarnings("unchecked")
        ApiResponse<WithdrawAllObjectionsResponse> apiResponse =
                mock(ApiResponse.class);

        when(apiResponse.getData()).thenReturn(withdrawalResponse);
        when(apiResponse.getStatusCode()).thenReturn(200);
        stubWithdrawal(apiResponse);

        // When / Then
        DuplicateRecordException exception = assertThrows(
                DuplicateRecordException.class,
                () -> processor.doProcess(message));

        assertTrue(exception.getMessage()
                .contains("Duplicate/complete Withdrawal skipped"));
    }

    // --- helpers ---
    private void stubWithdrawal(ApiResponse<WithdrawAllObjectionsResponse> response) throws Exception {
        GetAllWithdrawals get = mock(GetAllWithdrawals.class);
        when(get.execute()).thenReturn(response);

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
        when(handler.getAllWithdrawals(anyString())).thenReturn(get);
    }
    private void stubWithdrawalCallThrowing(Exception toThrow) throws Exception {
        GetAllWithdrawals get = mock(GetAllWithdrawals.class);
        when(get.execute()).thenThrow(toThrow);

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
        when(handler.getAllWithdrawals(anyString())).thenReturn(get);
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