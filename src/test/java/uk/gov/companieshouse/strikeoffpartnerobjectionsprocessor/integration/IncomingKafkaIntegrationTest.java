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
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers.StrikeOffPartnerObjectionsKafkaConsumer;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration.IntegrationTestFixtures.COMPANY_NUMBER;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration.IntegrationTestFixtures.INCOMING_TOPIC;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration.IntegrationTestFixtures.STRIKE_OFF_EVENT_ID;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration.IntegrationTestFixtures.incomingMessage;

@Tag("integration-test")
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.topic.strikeoff.objections=" + INCOMING_TOPIC,
        "kafka.topic.strikeoff.processed-objections=strike-off-partner-objections-processed",
        "kafka.strikeoff.objections.group-id=integration-incoming-group",
        "kafka.strikeoff.processed-objections.group-id=integration-processed-group",
        "kafka.max-attempts=1",
        "kafka.backoff-delay=10"
})
@EmbeddedKafka(partitions = 1, topics = {
        INCOMING_TOPIC,
        "strike-off-partner-objections-processed"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class IncomingKafkaIntegrationTest {

    private static final String OBJECTION_URI =
            "/company/" + COMPANY_NUMBER + "/strike-off-partner-objections/" + STRIKE_OFF_EVENT_ID;
    private static final String OBJECTION_STATUS_URI =
            "/internal/company/" + COMPANY_NUMBER + "/strike-off-partner-objections/" + STRIKE_OFF_EVENT_ID + "/status";
    private static final String WITHDRAWAL_URI =
            "/company/" + COMPANY_NUMBER + "/strike-off-partner-objections-withdrawals/" + STRIKE_OFF_EVENT_ID;
    private static final String WITHDRAWAL_STATUS_URI =
            "/internal/company/" + COMPANY_NUMBER + "/strike-off-partner-objections-withdrawals/" + STRIKE_OFF_EVENT_ID + "/withdrawal-status";

    @Autowired
    @Qualifier("kafkaConsumerTemplate")
    private KafkaTemplate<String, StrikeOffPartnerObjections> incomingTemplate;

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
    void incomingObjectionEvent_fromKafka_updatesStatusAndSubmitsToChips() throws Exception {
        StrikeOffPartnerObjections message = incomingMessage(EventType.OBJECTION);
        stubGetObjection(ObjectionProcessingStatus.OBJECTION_SUBMITTED);
        UpdateObjectionStatus updateObjectionStatus = org.mockito.Mockito.mock(UpdateObjectionStatus.class);
        when(handler.updateObjectionStatus(eq(OBJECTION_STATUS_URI), any(UpdateObjectionStatusRequest.class)))
                .thenReturn(updateObjectionStatus);
        when(updateObjectionStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));

        incomingTemplate.send(INCOMING_TOPIC, message.getEventId(), message);
        incomingTemplate.flush();

        ArgumentCaptor<UpdateObjectionStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateObjectionStatusRequest.class);
        verify(handler, timeout(5000)).getObjection(OBJECTION_URI);
        verify(handler, timeout(5000))
                .updateObjectionStatus(eq(OBJECTION_STATUS_URI), requestCaptor.capture());
        verify(chipsSubmissionClient, timeout(5000)).submit(message);
        assertEquals(ObjectionProcessingStatus.OBJECTION_PROCESSING, requestCaptor.getValue().getProcessingStatus());
    }

    @Test
    void incomingWithdrawalEvent_fromKafka_updatesStatusAndSubmitsToChips() throws Exception {
        StrikeOffPartnerObjections message = incomingMessage(EventType.WITHDRAWAL);
        stubGetWithdrawal(WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        UpdateWithdrawalStatus updateWithdrawalStatus = org.mockito.Mockito.mock(UpdateWithdrawalStatus.class);
        when(handler.updateWithdrawalStatus(eq(WITHDRAWAL_STATUS_URI), any(UpdateWithdrawalStatusRequest.class)))
                .thenReturn(updateWithdrawalStatus);
        when(updateWithdrawalStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));

        incomingTemplate.send(INCOMING_TOPIC, message.getEventId(), message);
        incomingTemplate.flush();

        ArgumentCaptor<UpdateWithdrawalStatusRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateWithdrawalStatusRequest.class);
        verify(handler, timeout(5000)).getAllWithdrawals(WITHDRAWAL_URI);
        verify(handler, timeout(5000))
                .updateWithdrawalStatus(eq(WITHDRAWAL_STATUS_URI), requestCaptor.capture());
        verify(chipsSubmissionClient, timeout(5000)).submit(message);
        assertEquals(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING, requestCaptor.getValue().getProcessingStatus());
    }

    @Test
    void duplicateIncomingObjection_isHandledWithoutRethrow() throws Exception {
        StrikeOffPartnerObjections message = incomingMessage(EventType.OBJECTION);
        stubGetObjection(ObjectionProcessingStatus.OBJECTION_PROCESSING);

        assertDoesNotThrow(() -> consumer.consumeStrikeOffObjectionsMessage(
                1, new ConsumerRecord<>(INCOMING_TOPIC, 0, 0L, message.getEventId(), message)));

        verify(handler).getObjection(OBJECTION_URI);
        verify(handler, never()).updateObjectionStatus(eq(OBJECTION_STATUS_URI), any(UpdateObjectionStatusRequest.class));
        verify(chipsSubmissionClient, never()).submit(any());
    }

    @Test
    void chips503_isClassifiedAsRetryable() throws Exception {
        StrikeOffPartnerObjections message = incomingMessage(EventType.OBJECTION);
        stubGetObjection(ObjectionProcessingStatus.OBJECTION_SUBMITTED);
        UpdateObjectionStatus updateObjectionStatus = org.mockito.Mockito.mock(UpdateObjectionStatus.class);
        when(handler.updateObjectionStatus(eq(OBJECTION_STATUS_URI), any(UpdateObjectionStatusRequest.class)))
                .thenReturn(updateObjectionStatus);
        when(updateObjectionStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        doThrow(new ChipsSubmissionException("service unavailable", 503))
                .when(chipsSubmissionClient).submit(message);
        ConsumerRecord<String, StrikeOffPartnerObjections> kafkaRecord =
                new ConsumerRecord<>(INCOMING_TOPIC, 0, 0L, message.getEventId(), message);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, kafkaRecord));

        assertEquals("Retryable API error (status=503) for eventId=" + message.getEventId(), exception.getMessage());
    }

    @Test
    void chips403_isClassifiedAsNonRetryable() throws Exception {
        StrikeOffPartnerObjections message = incomingMessage(EventType.WITHDRAWAL);
        stubGetWithdrawal(WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED);
        UpdateWithdrawalStatus updateWithdrawalStatus = org.mockito.Mockito.mock(UpdateWithdrawalStatus.class);
        when(handler.updateWithdrawalStatus(eq(WITHDRAWAL_STATUS_URI), any(UpdateWithdrawalStatusRequest.class)))
                .thenReturn(updateWithdrawalStatus);
        when(updateWithdrawalStatus.execute()).thenReturn(new ApiResponse<>(204, null, null));
        doThrow(new ChipsSubmissionException("forbidden", 403))
                .when(chipsSubmissionClient).submit(message);
        ConsumerRecord<String, StrikeOffPartnerObjections> kafkaRecord =
                new ConsumerRecord<>(INCOMING_TOPIC, 0, 0L, message.getEventId(), message);

        InvalidStrikeOffMessageException exception = assertThrows(
                InvalidStrikeOffMessageException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, kafkaRecord));

        assertEquals("Non-retryable API error (status=403) for eventId=" + message.getEventId(), exception.getMessage());
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
