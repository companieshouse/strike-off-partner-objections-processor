package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.handler.objections.PrivateStrikeOffPartnerObjectionsResourceHandler;
import uk.gov.companieshouse.api.handler.objections.request.GetAllWithdrawals;
import uk.gov.companieshouse.api.handler.objections.request.GetObjection;
import uk.gov.companieshouse.api.handler.objections.request.UpdateObjectionStatus;
import uk.gov.companieshouse.api.handler.objections.request.UpdateWithdrawalStatus;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers.StrikeOffPartnerObjectionsKafkaConsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration.IntegrationTestFixtures.COMPANY_NUMBER;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration.IntegrationTestFixtures.PROCESSED_TOPIC;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration.IntegrationTestFixtures.STRIKE_OFF_EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration.IntegrationTestFixtures.processedMessage;

@Tag("integration-test")
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.topic.strikeoff.objections=strike-off-partner-objections-incoming",
        "kafka.topic.strikeoff.processed-objections=" + PROCESSED_TOPIC,
        "kafka.strikeoff.objections.group-id=integration-incoming-group-processed-tests",
        "kafka.strikeoff.processed-objections.group-id=integration-processed-group-tests",
        "kafka.max-attempts=1",
        "kafka.backoff-delay=10"
})
@EmbeddedKafka(partitions = 1, topics = {
        "strike-off-partner-objections-incoming",
        PROCESSED_TOPIC
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ProcessedKafkaIntegrationTest {

    private static final String OBJECTION_URI =
            "/company/" + COMPANY_NUMBER + "/strike-off-partner-objections/" + STRIKE_OFF_EVENT_ID;
    private static final String OBJECTION_STATUS_URI =
            "/internal/company/" + COMPANY_NUMBER + "/strike-off-partner-objections/" + STRIKE_OFF_EVENT_ID + "/status";
    private static final String WITHDRAWAL_URI =
            "/company/" + COMPANY_NUMBER + "/strike-off-partner-objections-withdrawals/" + STRIKE_OFF_EVENT_ID;
    private static final String WITHDRAWAL_STATUS_URI =
            "/internal/company/" + COMPANY_NUMBER + "/strike-off-partner-objections-withdrawals/" + STRIKE_OFF_EVENT_ID + "/withdrawal-status";

    @Autowired
    @Qualifier("processedKafkaConsumerTemplate")
    private KafkaTemplate<String, StrikeOffPartnerObjectionsProcessed> processedTemplate;

    @Autowired
    private StrikeOffPartnerObjectionsKafkaConsumer consumer;

    @MockitoBean
    private InternalApiClient internalApiClient;

    @MockitoBean
    private ChipsPartnerObjectionsSubmissionClient chipsSubmissionClient;

    private PrivateStrikeOffPartnerObjectionsResourceHandler handler;

    @BeforeEach
    void setUp() {
        handler = org.mockito.Mockito.mock(PrivateStrikeOffPartnerObjectionsResourceHandler.class);
        when(internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()).thenReturn(handler);
    }

    @Test
    void processedObjectionSuccess_fromKafka_updatesAcceptedStatusAndInitialExpiration() throws Exception {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(
                ProcessedEventType.OBJECTION, SuccessFailureIndicator.SUCCESS);
        stubGetObjection(ObjectionProcessingStatus.OBJECTION_SUBMITTED);
        UpdateObjectionStatus updateObjectionStatus = org.mockito.Mockito.mock(UpdateObjectionStatus.class);
        when(handler.updateObjectionStatus(eq(OBJECTION_STATUS_URI), any(UpdateObjectionStatusRequest.class)))
                .thenReturn(updateObjectionStatus);
        when(updateObjectionStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));

        assertDoesNotThrow(() -> consumer.consumeProcessedStrikeOffObjectionsMessage(
                1, new ConsumerRecord<>(PROCESSED_TOPIC, 0, 0L, message.getStrikeOffEventId(), message)));

        ArgumentCaptor<UpdateObjectionStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateObjectionStatusRequest.class);
        verify(handler, timeout(5000)).getObjection(OBJECTION_URI);
        verify(handler, timeout(5000))
                .updateObjectionStatus(eq(OBJECTION_STATUS_URI), requestCaptor.capture());
        assertEquals(ObjectionProcessingStatus.OBJECTION_ACCEPTED, requestCaptor.getValue().getProcessingStatus());
        assertNotNull(requestCaptor.getValue().getInitialExpirationOn());
        assertNull(requestCaptor.getValue().getFailureReason());
        verify(chipsSubmissionClient, never()).submit(any());
    }

    @Test
    void processedWithdrawalFailure_fromKafka_updatesRejectedStatusAndFailureReason() throws Exception {
        StrikeOffPartnerObjectionsProcessed message = processedMessage(
                ProcessedEventType.WITHDRAWAL, SuccessFailureIndicator.FAILURE);
        stubGetWithdrawal(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING);
        UpdateWithdrawalStatus updateWithdrawalStatus = org.mockito.Mockito.mock(UpdateWithdrawalStatus.class);
        when(handler.updateWithdrawalStatus(eq(WITHDRAWAL_STATUS_URI), any(UpdateWithdrawalStatusRequest.class)))
                .thenReturn(updateWithdrawalStatus);
        when(updateWithdrawalStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));

        processedTemplate.send(PROCESSED_TOPIC, message.getStrikeOffEventId(), message);
        processedTemplate.flush();

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);
        verify(handler, timeout(5000)).getAllWithdrawals(WITHDRAWAL_URI);
        verify(handler, timeout(5000))
                .updateWithdrawalStatus(eq(WITHDRAWAL_STATUS_URI), requestCaptor.capture());
        assertEquals(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED, requestCaptor.getValue().getProcessingStatus());
        assertEquals(message.getErrorMessage(), requestCaptor.getValue().getFailureReason());
        verify(chipsSubmissionClient, never()).submit(any());
    }

    private void stubGetObjection(ObjectionProcessingStatus status) throws Exception {
        GetObjection getObjection = org.mockito.Mockito.mock(GetObjection.class);
        BaseObjectionResponse objection = new BaseObjectionResponse()
                .objectionId("obj-001")
                .processingStatus(status);
        when(handler.getObjection(OBJECTION_URI)).thenReturn(getObjection);
        when(getObjection.execute()).thenReturn(new ApiResponse<>(200, null, objection));
    }

    private void stubGetWithdrawal(WithdrawalProcessingStatus status) throws Exception {
        GetAllWithdrawals getAllWithdrawals = org.mockito.Mockito.mock(GetAllWithdrawals.class);
        WithdrawAllObjectionsResponse withdrawal = new WithdrawAllObjectionsResponse()
                .withdrawalId("wd-001")
                .processingStatus(status);
        when(handler.getAllWithdrawals(WITHDRAWAL_URI)).thenReturn(getAllWithdrawals);
        when(getAllWithdrawals.execute()).thenReturn(new ApiResponse<>(200, null, withdrawal));
    }
}
