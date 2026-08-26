package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.GetAllWithdrawals;
import uk.gov.companieshouse.api.handler.objections.request.UpdateWithdrawalStatus;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.mockIncomingMessage;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.mockResourceHandler;

class AbstractWithdrawalsEventsProcessorTest {

    private static final String WITHDRAWAL_URI =
            "/company/12345678/strike-off-partner-objections-withdrawals/strike-001";
    private static final String WITHDRAWAL_STATUS_URI =
            "/internal/company/12345678/strike-off-partner-objections-withdrawals/strike-001/withdrawal-status";

    private final InternalApiClient internalApiClient = mock(InternalApiClient.class);

    private AbstractWithdrawalsEventsProcessor<StrikeOffPartnerObjections> processor;
    private PrivateStrikeOffPartnerObjectionsResourceHandler handler;
    private StrikeOffPartnerObjections message;

    @BeforeEach
    void setUp() {
        processor = new AbstractWithdrawalsEventsProcessor<>(
                internalApiClient,
                StrikeOffPartnerObjections::getEventId,
                StrikeOffPartnerObjections::getCompanyNumber,
                StrikeOffPartnerObjections::getStrikeOffEventId) {
            @Override
            protected void validate(StrikeOffPartnerObjections message) {
                // Not required when testing withdrawal-specific API operations.
            }

            @Override
            protected boolean eventTypeSupported(StrikeOffPartnerObjections message) {
                return true;
            }

            @Override
            protected void doProcess(StrikeOffPartnerObjections message) {
                // Not required when testing withdrawal-specific API operations.
            }
        };
        message = mockIncomingMessage();
        handler = mockResourceHandler(internalApiClient);
    }

    @Test
    void getWithdrawalDetails_success_returnsApiResponseData() throws Exception {
        WithdrawAllObjectionsResponse withdrawal = new WithdrawAllObjectionsResponse()
                .withdrawalId("withdrawal-001")
                .processingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        GetAllWithdrawals getWithdrawal = mock(GetAllWithdrawals.class);
        when(handler.getAllWithdrawals(WITHDRAWAL_URI)).thenReturn(getWithdrawal);
        when(getWithdrawal.execute()).thenReturn(new ApiResponse<>(200, null, withdrawal));

        WithdrawAllObjectionsResponse result = processor.getWithdrawalDetails(message);

        assertSame(withdrawal, result);
        verify(handler).getAllWithdrawals(WITHDRAWAL_URI);
    }

    @Test
    void getWithdrawalDetails_apiFailure_mapsAndPropagatesException() throws Exception {
        IllegalStateException cause = new IllegalStateException("API unavailable");
        GetAllWithdrawals getWithdrawal = mock(GetAllWithdrawals.class);
        when(handler.getAllWithdrawals(WITHDRAWAL_URI)).thenReturn(getWithdrawal);
        when(getWithdrawal.execute()).thenThrow(cause);

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> processor.getWithdrawalDetails(message));

        assertEquals("Retryable error for eventId=" + EVENT_ID, exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    void updateWithdrawalStatus_success_sendsRequestedStatus() throws Exception {
        UpdateWithdrawalStatus updateStatus = mock(UpdateWithdrawalStatus.class);
        when(handler.updateWithdrawalStatus(eq(WITHDRAWAL_STATUS_URI), any(UpdateWithdrawalStatusRequest.class)))
                .thenReturn(updateStatus);
        when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);

        assertDoesNotThrow(() -> processor.updateWithdrawalStatus(
                message, WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING));

        verify(handler).updateWithdrawalStatus(eq(WITHDRAWAL_STATUS_URI), requestCaptor.capture());
        assertEquals(
                WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING,
                requestCaptor.getValue().getProcessingStatus());
    }

    @Test
    void updateWithdrawalStatus_apiFailure_mapsAndPropagatesException() throws Exception {
        IllegalStateException cause = new IllegalStateException("API unavailable");
        UpdateWithdrawalStatus updateStatus = mock(UpdateWithdrawalStatus.class);
        when(handler.updateWithdrawalStatus(eq(WITHDRAWAL_STATUS_URI), any(UpdateWithdrawalStatusRequest.class)))
                .thenReturn(updateStatus);
        when(updateStatus.execute()).thenThrow(cause);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> processor.updateWithdrawalStatus(
                        message, WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING));

        assertEquals("Retryable error for eventId=" + EVENT_ID, exception.getMessage());
        assertSame(cause, exception.getCause());
    }
}
