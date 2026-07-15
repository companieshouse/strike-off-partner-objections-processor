package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.GetObjection;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StrikeOffPartnerObjectionsProcessorTest {

    private final InternalApiClient internalApiClient = mock(InternalApiClient.class);
    private final StrikeOffPartnerEventsProcessor processor =
            new StrikeOffPartnerEventsProcessor(internalApiClient);

    @Test
    void supportsObjections_butNotWithdrawals() {
        assertTrue(processor.supports(EventType.OBJECTION));
        assertFalse(processor.supports(EventType.WITHDRAWAL));
    }

    @Test
    void doProcess_validMessage_callsApiAndDoesNotThrow() throws Exception {
        stubGetObjection(new ApiResponse<>(200, null, new BaseObjectionResponse().objectionId("objection-001").processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED)));

        assertDoesNotThrow(() -> processor.process(validMessage()));
    }

    @Test
    void doProcess_apiErrorResponse_isWrappedAsRuntimeException() throws Exception {
        GetObjection get = mock(GetObjection.class);
        when(get.execute()).thenThrow(mock(IllegalStateException.class));
        stubHandlerReturning(get);
        var message = validMessage();

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> processor.process(message));
        assertTrue(ex.getMessage().contains("Retryable error"));
    }


    @Test
    void doProcess_apiError500_isRetryableRuntimeException() throws Exception {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(500);

        GetObjection get = mock(GetObjection.class);
        when(get.execute()).thenThrow(apiEx);
        stubHandlerReturning(get);
        StrikeOffPartnerObjections message = validMessage();

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> processor.process(message));

        assertFalse(ex instanceof InvalidStrikeOffMessageException);
        assertTrue(ex.getMessage().contains("Retryable API error"));
    }

    @Test
    void doProcess_apiError404_isNonRetryable() throws Exception {
        ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
        when(apiEx.getStatusCode()).thenReturn(404);

        GetObjection get = mock(GetObjection.class);
        when(get.execute()).thenThrow(apiEx);
        stubHandlerReturning(get);
        StrikeOffPartnerObjections message = validMessage();

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("Non-retryable API error"));
    }

    @Test
    void doProcess_uriValidationError_isNonRetryable() throws Exception {
        GetObjection get = mock(GetObjection.class);
        when(get.execute()).thenThrow(mock(URIValidationException.class));
        stubHandlerReturning(get);
        StrikeOffPartnerObjections message = validMessage();

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));

        assertTrue(ex.getMessage().contains("Non-retryable URI validation error"));
    }

    @Test
    void doProcess_ShouldThrowDuplicateRecordException_WhenObjectionAlreadyProcessed() throws Exception {
        // Given
        StrikeOffPartnerObjections message = new StrikeOffPartnerObjections();
        message.setEventId("event-123");

        BaseObjectionResponse objectionResponse = new BaseObjectionResponse();
        objectionResponse.setObjectionId("objection-123");
        objectionResponse.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_PROCESSING);

        ApiResponse<BaseObjectionResponse> apiResponse = mock(ApiResponse.class);
        when(apiResponse.getData()).thenReturn(objectionResponse);
        when(apiResponse.getStatusCode()).thenReturn(200);

        stubGetObjection(apiResponse);

        // When / Then
        DuplicateRecordException exception = assertThrows(
                DuplicateRecordException.class,
                () -> processor.doProcess(message));

        assertTrue(exception.getMessage().contains("Duplicate/complete Objection skipped"));
    }
    // --- helpers ---
    private void stubGetObjection(ApiResponse<BaseObjectionResponse> response) throws Exception {
        GetObjection get = mock(GetObjection.class);
        when(get.execute()).thenReturn(response);
        stubHandlerReturning(get);
    }

    private void stubHandlerReturning(GetObjection get) {
        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
        when(handler.getObjection(anyString())).thenReturn(get);
    }

    private StrikeOffPartnerObjections validMessage() {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setEventType(EventType.OBJECTION)
                .setCompanyNumber("12345678")
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();
    }
}