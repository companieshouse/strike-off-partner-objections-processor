package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorDispatcher;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers.StrikeOffPartnerObjectionsKafkaConsumer;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.DataMapHolder;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

@ExtendWith(MockitoExtension.class)
class StructuredLoggingKafkaListenerAspectTest {

    private static final String SENSITIVE_FAILURE_TEXT = "sensitive failure message";
    private static final String OBJECTION_ID = "objection-001";
    private static final String WITHDRAWAL_ID = "withdrawal-001";

    @Mock
    private ProcessorDispatcher processorDispatcher;

    @InjectMocks
    private StrikeOffPartnerObjectionsKafkaConsumer consumer;

    private Logger structuredLogger;
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        AspectJProxyFactory proxyFactory = new AspectJProxyFactory(consumer);
        proxyFactory.addAspect(new StructuredLoggingKafkaListenerAspect(3));
        consumer = proxyFactory.getProxy();
        structuredLogger = (Logger) LoggerFactory.getLogger(
                uk.gov.companieshouse.logging.StructuredLogger.class);
        logAppender = new ListAppender<>();
        logAppender.setContext(structuredLogger.getLoggerContext());
        logAppender.start();
        structuredLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        structuredLogger.detachAppender(logAppender);
        assertEquals(Map.of(), DataMapHolder.snapshot().build().getLogMap());
        DataMapHolder.clear();
    }

    @Test
    void consumeMessage_delegatesToProcessorDispatcher() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();

        consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord);

        verify(processorDispatcher).dispatch(objectionRecord.value());
    }

    @Test
    void consumeMessage_firstAttempt_nullAttemptNumber_doesNotThrow() {
        // attemptNumber is null on the first delivery (no retry header present)
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        consumer.consumeStrikeOffObjectionsMessage(null, objectionRecord);

        verify(processorDispatcher).dispatch(objectionRecord.value());
    }

    @Test
    void consumeMessage_dispatcherThrows_rethrowsException() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new RuntimeException("processing failed"))
                .when(processorDispatcher).dispatch(objectionRecord.value());

        assertThrows(RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord));
    }

    @Test
    void consumeMessage_intermediateRetryIncludesAttemptContextAndSafeApiFailureDetails() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        StrikeOffPartnerObjections event = objectionRecord.value();
        doThrow(new RuntimeException(SENSITIVE_FAILURE_TEXT,
                new ChipsSubmissionException(SENSITIVE_FAILURE_TEXT, 503)))
                .when(processorDispatcher).dispatch(event);

        assertThrows(RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(2, objectionRecord));

        Map<?, ?> retryData = logDataForOperation(DataMapHolder.RETRY);
        assertEquals(1, retryData.get("retry_count"));
        assertEquals("503", retryData.get("status"));
        assertEquals("ChipsSubmissionException", retryData.get("error_type"));
        assertEquals("ChipsSubmissionException", retryData.get("retry_reason"));

        verify(processorDispatcher).dispatch(event);
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeProcessedMessage_retryContextReachesOutcomeLogAndDispatcher() {
        StrikeOffPartnerObjectionsProcessed event = triggerProcessedObjectionEvent(false).value();
        event.setErrorMessage(SENSITIVE_FAILURE_TEXT);
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> processedRecord =
                new ConsumerRecord<>("processed-topic", 0, 0L, "key", event);
        doThrow(new RuntimeException(SENSITIVE_FAILURE_TEXT))
                .when(processorDispatcher).dispatch(event);

        assertThrows(RuntimeException.class,
                () -> consumer.consumeProcessedStrikeOffObjectionsMessage(2, processedRecord));

        assertEquals(1, logDataForOperation(DataMapHolder.PROCESSED_OUTCOME_RECEIVED)
                .get("retry_count"));
        verify(processorDispatcher).dispatch(event);
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeMessage_exhaustedAttemptLogsTerminalFailure() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new RuntimeException(SENSITIVE_FAILURE_TEXT))
                .when(processorDispatcher).dispatch(objectionRecord.value());

        assertThrows(RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(3, objectionRecord));

        Map<?, ?> failureData = logDataForOperation(DataMapHolder.PROCESSING_FAILED);
        assertEquals(2, failureData.get("retry_count"));
        assertEquals("RuntimeException", failureData.get("error_type"));
        assertNoSensitiveFailureTextLogged();
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 503, 403, 404})
    void consumeMessage_internalApiFailureLogsTypedStatusAndErrorType(int statusCode) {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        ApiErrorResponseException apiException = mock(ApiErrorResponseException.class);
        when(apiException.getStatusCode()).thenReturn(statusCode);
        boolean terminal = statusCode == 403 || statusCode == 404;
        RuntimeException failure = terminal
                ? new InvalidStrikeOffMessageException(SENSITIVE_FAILURE_TEXT, apiException)
                : new RuntimeException(SENSITIVE_FAILURE_TEXT, apiException);
        doThrow(failure)
                .when(processorDispatcher)
                .dispatch(objectionRecord.value());

        assertThrows(RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(2, objectionRecord));

        Map<?, ?> retryData = logDataForOperation(terminal ? DataMapHolder.PROCESSING_FAILED : DataMapHolder.RETRY);
        assertEquals(String.valueOf(statusCode), retryData.get("status"));
        assertEquals("ApiErrorResponseException", retryData.get("error_type"));
        assertNoSensitiveFailureTextLogged();
    }

    @ParameterizedTest
    @ValueSource(ints = {403, 404})
    void consumeMessage_nonRetryableApiFailureRetainsTypedStatus(int statusCode) {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new InvalidStrikeOffMessageException(
                        SENSITIVE_FAILURE_TEXT,
                        new ChipsSubmissionException(SENSITIVE_FAILURE_TEXT, statusCode)))
                .when(processorDispatcher)
                .dispatch(objectionRecord.value());

        assertThrows(InvalidStrikeOffMessageException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord));

        Map<?, ?> failureData = logDataForOperation(DataMapHolder.PROCESSING_FAILED);
        assertEquals(String.valueOf(statusCode), failureData.get("status"));
        assertEquals("ChipsSubmissionException", failureData.get("error_type"));
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeMessage_nonRetryableFailureLogsImmediately() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new NonRetryableErrorException(SENSITIVE_FAILURE_TEXT))
                .when(processorDispatcher)
                .dispatch(objectionRecord.value());

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord));

        Map<?, ?> failureData = logDataForOperation(DataMapHolder.PROCESSING_FAILED);
        assertEquals(0, failureData.get("retry_count"));
        assertEquals("NonRetryableErrorException", failureData.get("error_type"));
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeMessage_dispatcherThrowsNonRetryable_rethrowsAsIs() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new NonRetryableErrorException("bad message"))
                .when(processorDispatcher).dispatch(objectionRecord.value());

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord));
    }



    @Test
    void consume_ShouldHandleDuplicateRecordException() {
        // Given
        StrikeOffPartnerObjections event = new StrikeOffPartnerObjections();
        event.setEventId("event-123");

        ConsumerRecord<String, StrikeOffPartnerObjections> consumerRecord =
                new ConsumerRecord<>("topic", 0, 0L, "key", event);

        doThrow(new DuplicateRecordException("Duplicate record"))
                .when(processorDispatcher)
                .dispatch(event);

        // When / Then
        assertDoesNotThrow(() ->
                consumer.consumeStrikeOffObjectionsMessage(1, consumerRecord));

        verify(processorDispatcher).dispatch(event);
    }

    @Test
    void consumeMessage_nullPayload_throwsNonRetryableErrorException() {
        ConsumerRecord<String, StrikeOffPartnerObjections> recordWithNullPayload =
                new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, null);

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, recordWithNullPayload));
        assertEquals("strike-off-partner-objections-incoming",
                logDataForOperation(DataMapHolder.PROCESSING_FAILED).get("topic"));
    }

    @Test
    void consumeMessage_eventWithNullEventId_usesUnknownFallbackAndDelegates() {
        StrikeOffPartnerObjections event = new StrikeOffPartnerObjections();
        // eventId deliberately left null to exercise the "unknown" fallback at line 69
        ConsumerRecord<String, StrikeOffPartnerObjections> recordWithNullEventId =
                new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, event);

        assertDoesNotThrow(() -> consumer.consumeStrikeOffObjectionsMessage(1, recordWithNullEventId));
        verify(processorDispatcher).dispatch(event);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_delegatesToProcessorDispatcher(boolean wasSuccessful) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> objectionRecord = triggerProcessedObjectionEvent(wasSuccessful);

        consumer.consumeProcessedStrikeOffObjectionsMessage(1, objectionRecord);

        verify(processorDispatcher).dispatch(objectionRecord.value());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_firstAttempt_nullAttemptNumber_doesNotThrow(boolean wasSuccessful) {
        // attemptNumber is null on the first delivery (no retry header present)
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> objectionRecord = triggerProcessedObjectionEvent(wasSuccessful);
        consumer.consumeProcessedStrikeOffObjectionsMessage(null, objectionRecord);

        verify(processorDispatcher).dispatch(objectionRecord.value());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_dispatcherThrows_rethrowsException(boolean wasSuccessful) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> objectionRecord = triggerProcessedObjectionEvent(wasSuccessful);
        doThrow(new RuntimeException("processing failed"))
                .when(processorDispatcher).dispatch(objectionRecord.value());

        assertThrows(RuntimeException.class,
                () -> consumer.consumeProcessedStrikeOffObjectionsMessage(1, objectionRecord));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_dispatcherThrowsNonRetryable_rethrowsAsIs(boolean wasSuccessful) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> objectionRecord = triggerProcessedObjectionEvent(wasSuccessful);
        doThrow(new NonRetryableErrorException("bad message"))
                .when(processorDispatcher).dispatch(objectionRecord.value());

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeProcessedStrikeOffObjectionsMessage(1, objectionRecord));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_ShouldHandleDuplicateRecordException(boolean wasSuccessful) {
        // Given
        StrikeOffPartnerObjectionsProcessed event = new StrikeOffPartnerObjectionsProcessed();
        event.setStrikeOffEventId("event-123");

        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord =
                new ConsumerRecord<>("topic", 0, 0L, "key", event);

        doThrow(new DuplicateRecordException("Duplicate record"))
                .when(processorDispatcher)
                .dispatch(event);

        // When / Then
        assertDoesNotThrow(() ->
                consumer.consumeProcessedStrikeOffObjectionsMessage(1, consumerRecord));

        verify(processorDispatcher).dispatch(event);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_nullPayload_throwsNonRetryableErrorException(boolean wasSuccessful) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> recordWithNullPayload =
                new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, null);

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeProcessedStrikeOffObjectionsMessage(1, recordWithNullPayload));
        assertEquals("NonRetryableErrorException",
                logDataForOperation(DataMapHolder.PROCESSING_FAILED).get("error_type"));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void incomingDelivery_logsLifecycleAndSharedMetadata(int attempt) {
        StrikeOffPartnerObjections event = triggerObjectionEvent().value();
        ConsumerRecord<String, StrikeOffPartnerObjections> message =
                new ConsumerRecord<>("incoming-topic", 2, 15L, "message-key", event);
        int retryCount = Math.max(attempt - 1, 0);
        doAnswer(invocation -> {
            assertEquals(retryCount, DataMapHolder.snapshot().build().getLogMap().get("retry_count"));
            DataMapHolder.get().objectionId(OBJECTION_ID);
            DataMapHolder.snapshot().operation(DataMapHolder.CHIPS_RESPONSE).status("202")
                    .resourceUri("/operation-only").errorType("operation-only").build();
            return null;
        }).when(processorDispatcher).dispatch(event);

        consumer.consumeStrikeOffObjectionsMessage(attempt, message);

        List<String> operations = List.of(DataMapHolder.KAFKA_MESSAGE_RECEIVED,
                DataMapHolder.PROCESSING_STARTED, DataMapHolder.PROCESSING_COMPLETED);
        assertEquals(operations, loggedOperations());
        for (String operation : operations) {
            Map<?, ?> data = logDataForOperation(operation);
            assertEquals(retryCount, data.get("retry_count"));
            assertEquals("12345678", data.get("company_number"));
            assertEquals("strike-001", data.get("correlation_id"));
            assertEquals("TEST_ORG", data.get("partner_organisation"));
            assertEquals("test", data.get("source"));
            assertEquals("incoming-topic", data.get("topic"));
            assertEquals(2, data.get("partition"));
            assertEquals(15L, data.get("offset"));
            assertEquals("message-key", data.get("kafka_message_key"));
            assertNoOperationFields(data);
        }
        assertEquals(OBJECTION_ID, logDataForOperation(DataMapHolder.PROCESSING_COMPLETED).get("objection_id"));
    }

    @Test
    void processedDelivery_logsOutcomeWithoutLeakingItsStatus() {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> message = triggerProcessedObjectionEvent(true);
        doAnswer(invocation -> {
            DataMapHolder.get().withdrawalId(WITHDRAWAL_ID);
            return null;
        }).when(processorDispatcher).dispatch(message.value());

        consumer.consumeProcessedStrikeOffObjectionsMessage(3, message);

        assertEquals(List.of(DataMapHolder.KAFKA_MESSAGE_RECEIVED, DataMapHolder.PROCESSING_STARTED,
                DataMapHolder.PROCESSED_OUTCOME_RECEIVED, DataMapHolder.PROCESSING_COMPLETED), loggedOperations());
        assertEquals("SUCCESS", logDataForOperation(DataMapHolder.PROCESSED_OUTCOME_RECEIVED).get("status"));
        Map<?, ?> completion = logDataForOperation(DataMapHolder.PROCESSING_COMPLETED);
        assertEquals(WITHDRAWAL_ID, completion.get("withdrawal_id"));
        assertEquals(2, completion.get("retry_count"));
        assertNoOperationFields(completion);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void incomingFailureRetainsEnrichedIdAndClearsContext(boolean duplicate) {
        ConsumerRecord<String, StrikeOffPartnerObjections> message = triggerObjectionEvent();
        doAnswer(invocation -> {
            DataMapHolder.get().objectionId(OBJECTION_ID);
            if (duplicate) {
                throw new DuplicateRecordException(SENSITIVE_FAILURE_TEXT);
            }
            throw new InvalidStrikeOffMessageException(SENSITIVE_FAILURE_TEXT);
        }).when(processorDispatcher).dispatch(message.value());

        if (duplicate) {
            assertDoesNotThrow(() -> consumer.consumeStrikeOffObjectionsMessage(1, message));
        } else {
            assertThrows(InvalidStrikeOffMessageException.class,
                    () -> consumer.consumeStrikeOffObjectionsMessage(1, message));
        }

        String operation = duplicate ? DataMapHolder.PROCESSING_SKIPPED : DataMapHolder.PROCESSING_FAILED;
        assertEquals(OBJECTION_ID, logDataForOperation(operation).get("objection_id"));
        assertEquals(Map.of(), DataMapHolder.snapshot().build().getLogMap());
        assertNoSensitiveFailureTextLogged();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void processedFailureRetainsEnrichedIdAndClearsContext(boolean duplicate) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> message = triggerProcessedObjectionEvent(false);
        doAnswer(invocation -> {
            DataMapHolder.get().withdrawalId(WITHDRAWAL_ID);
            if (duplicate) {
                throw new DuplicateRecordException(SENSITIVE_FAILURE_TEXT);
            }
            throw new RuntimeException(SENSITIVE_FAILURE_TEXT);
        }).when(processorDispatcher).dispatch(message.value());

        if (duplicate) {
            assertDoesNotThrow(() -> consumer.consumeProcessedStrikeOffObjectionsMessage(3, message));
        } else {
            assertThrows(RuntimeException.class,
                    () -> consumer.consumeProcessedStrikeOffObjectionsMessage(3, message));
        }

        String operation = duplicate ? DataMapHolder.PROCESSING_SKIPPED : DataMapHolder.PROCESSING_FAILED;
        assertEquals(WITHDRAWAL_ID, logDataForOperation(operation).get("withdrawal_id"));
        assertEquals(2, logDataForOperation(operation).get("retry_count"));
        assertEquals(Map.of(), DataMapHolder.snapshot().build().getLogMap());
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consecutiveDeliveriesDoNotShareMetadata() {
        ConsumerRecord<String, StrikeOffPartnerObjections> incomingMessage = triggerObjectionEvent();
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> processedMessage =
                triggerProcessedObjectionEvent(true);
        doAnswer(invocation -> {
            DataMapHolder.get().objectionId(OBJECTION_ID);
            return null;
        }).when(processorDispatcher).dispatch(incomingMessage.value());

        consumer.consumeStrikeOffObjectionsMessage(3, incomingMessage);
        assertEquals(Map.of(), DataMapHolder.snapshot().build().getLogMap());
        logAppender.list.clear();
        consumer.consumeProcessedStrikeOffObjectionsMessage(null, processedMessage);

        Map<?, ?> receipt = logDataForOperation(DataMapHolder.KAFKA_MESSAGE_RECEIVED);
        assertEquals(0, receipt.get("retry_count"));
        assertNull(receipt.get("objection_id"));
        assertNull(receipt.get("partner_organisation"));
        assertNull(receipt.get("source"));
        assertNoOperationFields(receipt);
    }

    @Test
    void concurrentListenerThreadsHaveIsolatedContexts() throws Exception {
        ConsumerRecord<String, StrikeOffPartnerObjections> incomingMessage = triggerObjectionEvent();
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> processedMessage =
                triggerProcessedObjectionEvent(true);
        CyclicBarrier barrier = new CyclicBarrier(2);
        doAnswer(invocation -> {
            DataMapHolder.get().objectionId(OBJECTION_ID);
            barrier.await(5, TimeUnit.SECONDS);
            assertEquals(OBJECTION_ID, DataMapHolder.snapshot().build().getLogMap().get("objection_id"));
            assertNull(DataMapHolder.snapshot().build().getLogMap().get("withdrawal_id"));
            return null;
        }).when(processorDispatcher).dispatch(incomingMessage.value());
        doAnswer(invocation -> {
            DataMapHolder.get().withdrawalId(WITHDRAWAL_ID);
            barrier.await(5, TimeUnit.SECONDS);
            assertEquals(WITHDRAWAL_ID, DataMapHolder.snapshot().build().getLogMap().get("withdrawal_id"));
            assertNull(DataMapHolder.snapshot().build().getLogMap().get("objection_id"));
            return null;
        }).when(processorDispatcher).dispatch(processedMessage.value());

        try (var executor = Executors.newFixedThreadPool(2)) {
            var incomingResult = executor.submit(() -> {
                consumer.consumeStrikeOffObjectionsMessage(1, incomingMessage);
                return DataMapHolder.snapshot().build().getLogMap();
            });
            var processedResult = executor.submit(() -> {
                consumer.consumeProcessedStrikeOffObjectionsMessage(1, processedMessage);
                return DataMapHolder.snapshot().build().getLogMap();
            });
            assertEquals(Map.of(), incomingResult.get(10, TimeUnit.SECONDS));
            assertEquals(Map.of(), processedResult.get(10, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_eventWithNullEventId_usesUnknownFallbackAndDelegates(boolean wasSuccessful) {
        StrikeOffPartnerObjectionsProcessed event = new StrikeOffPartnerObjectionsProcessed();
        // eventId deliberately left null to exercise the "unknown" fallback at line 69
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> recordWithNullEventId =
                new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, event);

        assertDoesNotThrow(() -> consumer.consumeProcessedStrikeOffObjectionsMessage(1, recordWithNullEventId));
        verify(processorDispatcher).dispatch(event);
    }

    private ConsumerRecord<String, StrikeOffPartnerObjections> triggerObjectionEvent() {
        StrikeOffPartnerObjections event = StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setCompanyNumber("12345678")
                .setEventType(EventType.OBJECTION)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();
        return new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, event);
    }

    private ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> triggerProcessedObjectionEvent(boolean wasSuccessful) {
        StrikeOffPartnerObjectionsProcessed event = StrikeOffPartnerObjectionsProcessed.newBuilder()
                .setCompanyNumber("12345678")
                .setEventType(OBJECTION)
                .setStrikeOffEventId("strike-001")
                .setInitialExpirationOn(wasSuccessful ? LocalDate.parse("2026-07-06") : null)
                .setErrorMessage(wasSuccessful? null : "Processing failed")
                .setSuccessFailureIndicator(wasSuccessful ? SUCCESS : FAILURE)
                .build();
        return new ConsumerRecord<>("strike-off-partner-objections-processed", 0, 0L, null, event);
    }

    private Map<?, ?> logDataForOperation(String operation) {
        return logData().stream()
                .filter(data -> operation.equals(data.get("operation")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No log found for operation " + operation));
    }

    private List<Map<?, ?>> logData() {
        return logAppender.list.stream()
                .map(ILoggingEvent::getArgumentArray)
                .filter(arguments -> arguments != null && arguments.length > 0)
                .map(arguments -> arguments[0])
                .map(argument -> argument instanceof Map<?, ?> log ? log.get("data") : null)
                .<Map<?, ?>>map(data -> data instanceof Map<?, ?> map ? map : Map.of())
                .toList();
    }

    private List<String> loggedOperations() {
        return logData().stream().map(data -> String.valueOf(data.get("operation"))).toList();
    }

    private void assertNoOperationFields(Map<?, ?> data) {
        assertNull(data.get("status"));
        assertNull(data.get("resource_uri"));
        assertNull(data.get("error_type"));
    }

    private void assertNoSensitiveFailureTextLogged() {
        boolean sensitiveTextLogged = logAppender.list.stream()
                .anyMatch(event -> event.getFormattedMessage().contains(SENSITIVE_FAILURE_TEXT)
                        || Arrays.deepToString(event.getArgumentArray()).contains(SENSITIVE_FAILURE_TEXT));
        assertFalse(sensitiveTextLogged);
        logAppender.list.forEach(event -> assertNull(event.getThrowableProxy()));
    }
}
