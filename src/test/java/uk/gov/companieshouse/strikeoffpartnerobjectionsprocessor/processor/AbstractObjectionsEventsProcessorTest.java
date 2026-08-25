package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.GetObjection;
import uk.gov.companieshouse.api.handler.objections.request.UpdateObjectionStatus;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.mockIncomingMessage;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorTestFixtures.mockResourceHandler;

class AbstractObjectionsEventsProcessorTest {

    private static final String OBJECTION_URI =
            "/company/12345678/strike-off-partner-objections/strike-001";
    private static final String OBJECTION_STATUS_URI =
            "/internal/company/12345678/strike-off-partner-objections/strike-001/status";

    private final InternalApiClient internalApiClient = mock(InternalApiClient.class);
    private AbstractObjectionsEventsProcessor<StrikeOffPartnerObjections> processor;
    private PrivateStrikeOffPartnerObjectionsResourceHandler handler;
    private StrikeOffPartnerObjections message;

    @BeforeEach
    void setUp() {
        processor = new AbstractObjectionsEventsProcessor<>(
                internalApiClient,
                StrikeOffPartnerObjections::getEventId,
                StrikeOffPartnerObjections::getCompanyNumber,
                StrikeOffPartnerObjections::getStrikeOffEventId) {
            @Override
            protected void validate(StrikeOffPartnerObjections message) {
                // Not required when testing objection-specific API operations.
            }

            @Override
            protected boolean eventTypeSupported(StrikeOffPartnerObjections message) {
                // Testing is covered in base class test file
                return true;
            }

            @Override
            protected void doProcess(StrikeOffPartnerObjections message) {
                // Not required when testing objection-specific API operations.
            }
        };
        message = mockIncomingMessage();
        handler = mockResourceHandler(internalApiClient);
    }

    @Test
    void getObjectionDetails_success_returnsApiResponseData() throws Exception {
        BaseObjectionResponse objection = new BaseObjectionResponse()
                .objectionId("objection-001")
                .processingStatus(ObjectionProcessingStatus.OBJECTION_SUBMITTED);
        GetObjection getObjection = mock(GetObjection.class);
        when(handler.getObjection(OBJECTION_URI)).thenReturn(getObjection);
        when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, objection));

        BaseObjectionResponse result = processor.getObjectionDetails(message);

        assertSame(objection, result);
        verify(handler).getObjection(OBJECTION_URI);
    }

    @Test
    void getObjectionDetails_apiFailure_mapsAndPropagatesException() throws Exception {
        IllegalStateException cause = new IllegalStateException("API unavailable");
        GetObjection getObjection = mock(GetObjection.class);
        when(handler.getObjection(OBJECTION_URI)).thenReturn(getObjection);
        when(getObjection.execute()).thenThrow(cause);

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> processor.getObjectionDetails(message));

        assertEquals("Retryable error for eventId=" + EVENT_ID, exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    void updateObjectionStatus_success_sendsRequestedStatus() throws Exception {
        UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
        when(handler.updateObjectionStatus(
                org.mockito.ArgumentMatchers.eq(OBJECTION_STATUS_URI),
                org.mockito.ArgumentMatchers.any(UpdateObjectionStatusRequest.class)))
                .thenReturn(updateStatus);
        when(updateStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        ArgumentCaptor<UpdateObjectionStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateObjectionStatusRequest.class);

        assertDoesNotThrow(() -> processor.updateObjectionStatus(
                message, ObjectionProcessingStatus.OBJECTION_PROCESSING));

        verify(handler).updateObjectionStatus(
                org.mockito.ArgumentMatchers.eq(OBJECTION_STATUS_URI), requestCaptor.capture());
        assertEquals(
                ObjectionProcessingStatus.OBJECTION_PROCESSING,
                requestCaptor.getValue().getProcessingStatus());
    }

    @Test
    void updateObjectionStatus_apiFailure_mapsAndPropagatesException() throws Exception {
        IllegalStateException cause = new IllegalStateException("API unavailable");
        UpdateObjectionStatus updateStatus = mock(UpdateObjectionStatus.class);
        when(handler.updateObjectionStatus(
                org.mockito.ArgumentMatchers.eq(OBJECTION_STATUS_URI),
                org.mockito.ArgumentMatchers.any(UpdateObjectionStatusRequest.class)))
                .thenReturn(updateStatus);
        when(updateStatus.execute()).thenThrow(cause);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> processor.updateObjectionStatus(
                        message, ObjectionProcessingStatus.OBJECTION_REJECTED));

        assertEquals("Retryable error for eventId=" + EVENT_ID, exception.getMessage());
        assertSame(cause, exception.getCause());
    }
}
