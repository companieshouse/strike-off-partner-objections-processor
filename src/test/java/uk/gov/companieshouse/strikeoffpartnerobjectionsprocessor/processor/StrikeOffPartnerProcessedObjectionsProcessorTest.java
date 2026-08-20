package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.GetObjection;
import uk.gov.companieshouse.api.handler.objections.request.UpdateObjectionStatus;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.ArgumentCaptor;
import static uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus.OBJECTION_ACCEPTED;
import static uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus.OBJECTION_SUBMITTED;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;

@ExtendWith(MockitoExtension.class)
class StrikeOffPartnerProcessedObjectionsProcessorTest extends AbstractStrikeOffPartnerProcessedEventsProcessorTest{

    private final InternalApiClient internalApiClient = mock(InternalApiClient.class);
    private final StrikeOffPartnerProcessedObjectionsProcessor processor =
            new StrikeOffPartnerProcessedObjectionsProcessor(internalApiClient);

    @Test
    void supportsObjections_butNotWithdrawals() {
        assertTrue(processor.supports(ProcessedEventType.OBJECTION));
        assertFalse(processor.supports(ProcessedEventType.WITHDRAWAL));
    }

    @Test
    void doProcess_validMessage_callsApiAndDoesNotThrow() throws Exception {
        BaseObjectionResponse response = new BaseObjectionResponse()
                .objectionId("objection-001")
                .processingStatus(OBJECTION_SUBMITTED);

        GetObjection getObjection = mock(GetObjection.class);
        when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
        when(handler.getObjection(anyString())).thenReturn(getObjection);

        UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
        when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        when(handler.updateObjectionStatus(anyString(), any())).thenReturn(updateStatus);

        assertDoesNotThrow(() -> processor.process(validMessage(true)));

        ArgumentCaptor<UpdateObjectionStatusRequest> captor = ArgumentCaptor.forClass(UpdateObjectionStatusRequest.class);
        verify(handler).updateObjectionStatus(anyString(), captor.capture());
        assertEquals(OBJECTION_ACCEPTED, captor.getValue().getProcessingStatus());
    }

    @Test
    void doProcess_ShouldCallUpdateStatusAfterSuccessfulFetch() throws Exception {
        BaseObjectionResponse response = new BaseObjectionResponse()
                .objectionId("objection-002")
                .processingStatus(OBJECTION_SUBMITTED);

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

        GetObjection getObjection = mock(GetObjection.class);
        when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
        when(handler.getObjection(anyString())).thenReturn(getObjection);

        UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
        when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        when(handler.updateObjectionStatus(anyString(), any())).thenReturn(updateStatus);

        processor.process(validMessage(true));

        verify(handler).getObjection(anyString());
        verify(handler).updateObjectionStatus(anyString(), any());
    }

    @Test
    void doProcess_successfulProcessing_logsObjectionId() throws Exception {
        BaseObjectionResponse response = new BaseObjectionResponse()
                .objectionId("objection-010")
                .processingStatus(OBJECTION_SUBMITTED);

        PrivateStrikeOffPartnerObjectionsResourceHandler handler =
                mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);

        GetObjection getObjection = mock(GetObjection.class);
        when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, response));
        when(handler.getObjection(anyString())).thenReturn(getObjection);

        UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
        when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        when(handler.updateObjectionStatus(anyString(), any())).thenReturn(updateStatus);

        StrikeOffPartnerObjectionsProcessed message = validMessage(true);
        assertDoesNotThrow(() -> processor.process(message));

        verify(handler).getObjection(anyString());
        verify(handler).updateObjectionStatus(anyString(), any());
    }
}
