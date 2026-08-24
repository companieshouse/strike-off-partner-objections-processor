package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.GetObjection;
import uk.gov.companieshouse.api.handler.objections.request.UpdateObjectionStatus;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class StrikeOffPartnerObjectionsProcessorTest {

    private final InternalApiClient internalApiClient = mock(InternalApiClient.class);
    private final ChipsPartnerObjectionsSubmissionClient chipsPartnerObjectionsSubmissionClient =
            mock(ChipsPartnerObjectionsSubmissionClient.class);
    private final StrikeOffPartnerObjectionsProcessor processor =
            new StrikeOffPartnerObjectionsProcessor(internalApiClient, chipsPartnerObjectionsSubmissionClient);

    @Test
    void supportsObjections_butNotWithdrawals() {
        assertTrue(processor.supports(EventType.OBJECTION));
        assertFalse(processor.supports(EventType.WITHDRAWAL));
    }

      @Test
    void doProcess_validMessage_callsApiAndDoesNotThrow() throws Exception {
        BaseObjectionResponse response = new BaseObjectionResponse()
                .objectionId("objection-001")
                .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

        GetObjection getObjection = mock(GetObjection.class);
        when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
        when(handler.getObjection(anyString())).thenReturn(getObjection);

        UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
        when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);

        assertDoesNotThrow(() -> processor.process(validMessage()));
        verify(chipsPartnerObjectionsSubmissionClient).submit(org.mockito.ArgumentMatchers.any());
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

        @SuppressWarnings("unchecked")
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

    @Test
    void doProcess_ShouldCallUpdateStatusAfterSuccessfulFetch() throws Exception {
        // Given - a successfully fetched objection that's not yet processing
        BaseObjectionResponse response = new BaseObjectionResponse()
                .objectionId("objection-002")
                .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

        GetObjection getObjection = mock(GetObjection.class);
        when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
        when(handler.getObjection(anyString())).thenReturn(getObjection);

        UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
        when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);

        // When
        processor.process(validMessage());

        // Then - verify both methods were called
        verify(handler).getObjection(anyString());
        verify(handler).updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any());
        verify(chipsPartnerObjectionsSubmissionClient).submit(org.mockito.ArgumentMatchers.any());
    }

     @Test
     void doProcess_updateObjectionStatus_apiError500_isRetryable() throws Exception {
         // Given - getObjectionDetails succeeds but updateObjectionStatus throws 500
         BaseObjectionResponse response = new BaseObjectionResponse()
                 .objectionId("objection-003")
                 .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

         GetObjection getObjection = mock(GetObjection.class);
         when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
         when(handler.getObjection(anyString())).thenReturn(getObjection);

         ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
         when(apiEx.getStatusCode()).thenReturn(500);

         UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
         when(updateStatus.execute()).thenThrow(apiEx);
         when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);
         StrikeOffPartnerObjections message = validMessage();

         // When / Then
         RuntimeException ex = assertThrows(RuntimeException.class,
                 () -> processor.process(message));

         assertFalse(ex instanceof InvalidStrikeOffMessageException);
         assertTrue(ex.getMessage().contains("Retryable API error"));
     }

     @Test
     void doProcess_updateObjectionStatus_apiError404_isNonRetryable() throws Exception {
         // Given - getObjectionDetails succeeds but updateObjectionStatus throws 404
         BaseObjectionResponse response = new BaseObjectionResponse()
                 .objectionId("objection-004")
                 .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

         GetObjection getObjection = mock(GetObjection.class);
         when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
         when(handler.getObjection(anyString())).thenReturn(getObjection);

         ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
         when(apiEx.getStatusCode()).thenReturn(404);

         UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
         when(updateStatus.execute()).thenThrow(apiEx);
         when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);
         StrikeOffPartnerObjections message = validMessage();

         // When / Then
         InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                 () -> processor.process(message));

         assertTrue(ex.getMessage().contains("Non-retryable API error"));
     }

     @Test
     void doProcess_updateObjectionStatus_apiError429_isRetryable() throws Exception {
         // Given - getObjectionDetails succeeds but updateObjectionStatus throws 429 (rate limit)
         BaseObjectionResponse response = new BaseObjectionResponse()
                 .objectionId("objection-005")
                 .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

         GetObjection getObjection = mock(GetObjection.class);
         when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
         when(handler.getObjection(anyString())).thenReturn(getObjection);

         ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
         when(apiEx.getStatusCode()).thenReturn(429);

         UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
         when(updateStatus.execute()).thenThrow(apiEx);
         when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);
         StrikeOffPartnerObjections message = validMessage();

         // When / Then
         RuntimeException ex = assertThrows(RuntimeException.class,
                 () -> processor.process(message));

         assertFalse(ex instanceof InvalidStrikeOffMessageException);
         assertTrue(ex.getMessage().contains("Retryable API error"));
     }

     @Test
     void doProcess_updateObjectionStatus_uriValidationError_isNonRetryable() throws Exception {
         // Given - getObjectionDetails succeeds but updateObjectionStatus throws URI validation error
         BaseObjectionResponse response = new BaseObjectionResponse()
                 .objectionId("objection-006")
                 .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

         GetObjection getObjection = mock(GetObjection.class);
         when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
         when(handler.getObjection(anyString())).thenReturn(getObjection);

         UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
         when(updateStatus.execute()).thenThrow(mock(URIValidationException.class));
         when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);
         StrikeOffPartnerObjections message = validMessage();

         // When / Then
         InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                 () -> processor.process(message));

         assertTrue(ex.getMessage().contains("Non-retryable URI validation error"));
     }

     @Test
     void doProcess_updateObjectionStatus_unknownException_isRetryable() throws Exception {
         // Given - getObjectionDetails succeeds but updateObjectionStatus throws unknown exception
         BaseObjectionResponse response = new BaseObjectionResponse()
                 .objectionId("objection-007")
                 .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

         GetObjection getObjection = mock(GetObjection.class);
         when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
         when(handler.getObjection(anyString())).thenReturn(getObjection);

         UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
         when(updateStatus.execute()).thenThrow(new IllegalStateException("Unknown error"));
         when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);
         StrikeOffPartnerObjections message = validMessage();

         // When / Then
         RuntimeException ex = assertThrows(RuntimeException.class,
                 () -> processor.process(message));

         assertFalse(ex instanceof InvalidStrikeOffMessageException);
         assertTrue(ex.getMessage().contains("Retryable error"));
     }

     @Test
     void doProcess_duplicateWithDifferentStatuses_notDuplicate() throws Exception {
         // Given - objection with OBJECTION_REJECTED status (not a duplicate of OBJECTION_PROCESSING)
         BaseObjectionResponse response = new BaseObjectionResponse()
                 .objectionId("objection-008")
                 .processingStatus(ObjectionProcessingStatus.OBJECTION_REJECTED);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

         GetObjection getObjection = mock(GetObjection.class);
         when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
         when(handler.getObjection(anyString())).thenReturn(getObjection);

         UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
         when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
         when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);

         // When / Then
         assertDoesNotThrow(() -> processor.process(validMessage()));

         // Verify update was called since it's not a duplicate
         verify(handler).updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any());
     }

     @Test
     void doProcess_duplicateWithObjectionSubmittedStatus_notDuplicate() throws Exception {
         // Given - objection with OBJECTION_SUBMITTED status (not a duplicate)
         BaseObjectionResponse response = new BaseObjectionResponse()
                 .objectionId("objection-009")
                 .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

         GetObjection getObjection = mock(GetObjection.class);
         when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
         when(handler.getObjection(anyString())).thenReturn(getObjection);

         UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
         when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
         when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);

         // When
         assertDoesNotThrow(() -> processor.process(validMessage()));

         // Then - verify update was called
         verify(handler).updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any());
     }

     @Test
     void doProcess_successfulProcessing_logsObjectionId() throws Exception {
         // Given
         BaseObjectionResponse response = new BaseObjectionResponse()
                 .objectionId("objection-010")
                 .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

         GetObjection getObjection = mock(GetObjection.class);
         when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
         when(handler.getObjection(anyString())).thenReturn(getObjection);

         UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
         when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
         when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);

         // When
         StrikeOffPartnerObjections message = validMessage();
         assertDoesNotThrow(() -> processor.process(message));

         // Then - verify both API calls were made (validates complete flow)
         verify(handler).getObjection(anyString());
         verify(handler).updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any());
         verify(chipsPartnerObjectionsSubmissionClient).submit(org.mockito.ArgumentMatchers.any());
     }

     @Test
     void doProcess_apiError400_isNonRetryable() throws Exception {
         // Given - getObjectionDetails throws 400 (client error)
         ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
         when(apiEx.getStatusCode()).thenReturn(400);

         GetObjection get = mock(GetObjection.class);
         when(get.execute()).thenThrow(apiEx);
         stubHandlerReturning(get);
         StrikeOffPartnerObjections message = validMessage();

         // When / Then
         InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                 () -> processor.process(message));

         assertTrue(ex.getMessage().contains("Non-retryable API error"));
     }

     @Test
     void doProcess_apiError503_isRetryable() throws Exception {
         // Given - getObjectionDetails throws 503 (service unavailable)
         ApiErrorResponseException apiEx = mock(ApiErrorResponseException.class);
         when(apiEx.getStatusCode()).thenReturn(503);

         GetObjection get = mock(GetObjection.class);
         when(get.execute()).thenThrow(apiEx);
         stubHandlerReturning(get);
         StrikeOffPartnerObjections message = validMessage();

         // When / Then
         RuntimeException ex = assertThrows(RuntimeException.class,
                 () -> processor.process(message));

         assertFalse(ex instanceof InvalidStrikeOffMessageException);
         assertTrue(ex.getMessage().contains("Retryable API error"));
     }

    @Test
    void doProcess_chipsSubmission400_isNonRetryable() throws Exception {
        BaseObjectionResponse response = new BaseObjectionResponse()
                .objectionId("objection-011")
                .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);
        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

        GetObjection getObjection = mock(GetObjection.class);
        when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
        when(handler.getObjection(anyString())).thenReturn(getObjection);

        UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
        when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        when(handler.updateObjectionStatus(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(updateStatus);

        doThrow(new ChipsSubmissionException("bad request", 400))
                .when(chipsPartnerObjectionsSubmissionClient)
                .submit(org.mockito.ArgumentMatchers.any());
        StrikeOffPartnerObjections message = validMessage();

        InvalidStrikeOffMessageException exception = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(message));
        assertTrue(exception.getMessage().contains("Non-retryable API error"));
    }

    @Test
    void doProcess_duplicateObjection_doesNotSubmitToChips() throws Exception {
        StrikeOffPartnerObjections message = new StrikeOffPartnerObjections();
        message.setEventId("event-123");

        BaseObjectionResponse objectionResponse = new BaseObjectionResponse();
        objectionResponse.setObjectionId("objection-123");
        objectionResponse.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_PROCESSING);

        @SuppressWarnings("unchecked")
        ApiResponse<BaseObjectionResponse> apiResponse = mock(ApiResponse.class);
        when(apiResponse.getData()).thenReturn(objectionResponse);
        when(apiResponse.getStatusCode()).thenReturn(200);

        stubGetObjection(apiResponse);

        assertThrows(DuplicateRecordException.class, () -> processor.doProcess(message));
        verify(chipsPartnerObjectionsSubmissionClient, never()).submit(org.mockito.ArgumentMatchers.any());
    }

     // --- helpers ---

     private void stubGetObjection(ApiResponse<BaseObjectionResponse> response) throws Exception {
         GetObjection get = mock(GetObjection.class);
         when(get.execute()).thenReturn(response);

         PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                 mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
         when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
         when(handler.getObjection(anyString())).thenReturn(get);
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