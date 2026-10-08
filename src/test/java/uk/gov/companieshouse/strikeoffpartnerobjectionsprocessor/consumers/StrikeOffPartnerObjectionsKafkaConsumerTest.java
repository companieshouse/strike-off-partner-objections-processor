package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers;

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
import org.springframework.test.util.ReflectionTestUtils;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorDispatcher;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.ProcessorLogContext;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

@ExtendWith(MockitoExtension.class)
class StrikeOffPartnerObjectionsKafkaConsumerTest {

    private static final String SENSITIVE_FAILURE_TEXT = "sensitive failure message";

    @Mock
    private ProcessorDispatcher processorDispatcher;

    @InjectMocks
    private StrikeOffPartnerObjectionsKafkaConsumer consumer;

    private Logger structuredLogger;
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(consumer, "maxAttempts", 3);
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
    }

    @Test
    void consumeMessage_delegatesToProcessorDispatcher() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();

        consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord);

        verify(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));
    }

    @Test
    void consumeMessage_firstAttempt_nullAttemptNumber_doesNotThrow() {
        // attemptNumber is null on the first delivery (no retry header present)
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        consumer.consumeStrikeOffObjectionsMessage(null, objectionRecord);

        verify(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));
    }

    @Test
    void consumeMessage_dispatcherThrows_rethrowsException() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new RuntimeException("processing failed"))
                .when(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));

        assertThrows(RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord));
    }

    @Test
    void consumeMessage_intermediateRetryIncludesAttemptContextAndSafeApiFailureDetails() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        StrikeOffPartnerObjections event = objectionRecord.value();
        doThrow(new RuntimeException(SENSITIVE_FAILURE_TEXT,
                new ChipsSubmissionException(SENSITIVE_FAILURE_TEXT, 503)))
                .when(processorDispatcher).dispatch(eq(event), any(ProcessorLogContext.class));

        assertThrows(RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(2, objectionRecord));

        Map<?, ?> retryData = logDataForOperation(ProcessorLogContext.RETRY);
        assertEquals(1, retryData.get("retry_count"));
        assertEquals("503", retryData.get("status"));
        assertEquals("ChipsSubmissionException", retryData.get("error_type"));
        assertEquals("ChipsSubmissionException", retryData.get("retry_reason"));

        var contextCaptor = org.mockito.ArgumentCaptor.forClass(ProcessorLogContext.class);
        verify(processorDispatcher).dispatch(eq(event), contextCaptor.capture());
        assertEquals(1, contextCaptor.getValue().toLogMap().get("retry_count"));
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeProcessedMessage_retryContextReachesOutcomeLogAndDispatcher() {
        StrikeOffPartnerObjectionsProcessed event = triggerProcessedObjectionEvent(false).value();
        event.setErrorMessage(SENSITIVE_FAILURE_TEXT);
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> processedRecord =
                new ConsumerRecord<>("processed-topic", 0, 0L, "key", event);
        doThrow(new RuntimeException(SENSITIVE_FAILURE_TEXT))
                .when(processorDispatcher).dispatch(eq(event), any(ProcessorLogContext.class));

        assertThrows(RuntimeException.class,
                () -> consumer.consumeProcessedStrikeOffObjectionsMessage(2, processedRecord));

        assertEquals(1, logDataForOperation(ProcessorLogContext.PROCESSED_OUTCOME_RECEIVED)
                .get("retry_count"));
        var contextCaptor = org.mockito.ArgumentCaptor.forClass(ProcessorLogContext.class);
        verify(processorDispatcher).dispatch(eq(event), contextCaptor.capture());
        assertEquals(1, contextCaptor.getValue().toLogMap().get("retry_count"));
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeMessage_exhaustedAttemptLogsTerminalFailure() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new RuntimeException(SENSITIVE_FAILURE_TEXT))
                .when(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));

        assertThrows(RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(3, objectionRecord));

        Map<?, ?> failureData = logDataForOperation(ProcessorLogContext.PROCESSING_FAILED);
        assertEquals(2, failureData.get("retry_count"));
        assertEquals("RuntimeException", failureData.get("error_type"));
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeMessage_internalApiFailureLogsTypedStatusAndErrorType() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        ApiErrorResponseException apiException = mock(ApiErrorResponseException.class);
        when(apiException.getStatusCode()).thenReturn(429);
        doThrow(new RuntimeException(SENSITIVE_FAILURE_TEXT, apiException))
                .when(processorDispatcher)
                .dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));

        assertThrows(RuntimeException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(2, objectionRecord));

        Map<?, ?> retryData = logDataForOperation(ProcessorLogContext.RETRY);
        assertEquals("429", retryData.get("status"));
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
                .dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));

        assertThrows(InvalidStrikeOffMessageException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord));

        Map<?, ?> failureData = logDataForOperation(ProcessorLogContext.PROCESSING_FAILED);
        assertEquals(String.valueOf(statusCode), failureData.get("status"));
        assertEquals("ChipsSubmissionException", failureData.get("error_type"));
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeMessage_nonRetryableFailureLogsImmediately() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new NonRetryableErrorException(SENSITIVE_FAILURE_TEXT))
                .when(processorDispatcher)
                .dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, objectionRecord));

        Map<?, ?> failureData = logDataForOperation(ProcessorLogContext.PROCESSING_FAILED);
        assertEquals(0, failureData.get("retry_count"));
        assertEquals("NonRetryableErrorException", failureData.get("error_type"));
        assertNoSensitiveFailureTextLogged();
    }

    @Test
    void consumeMessage_dispatcherThrowsNonRetryable_rethrowsAsIs() {
        ConsumerRecord<String, StrikeOffPartnerObjections> objectionRecord = triggerObjectionEvent();
        doThrow(new NonRetryableErrorException("bad message"))
                .when(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));

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
                .dispatch(eq(event), any(ProcessorLogContext.class));

        // When / Then
        assertDoesNotThrow(() ->
                consumer.consumeStrikeOffObjectionsMessage(1, consumerRecord));

        verify(processorDispatcher).dispatch(eq(event), any(ProcessorLogContext.class));
    }

    @Test
    void consumeMessage_nullPayload_throwsNonRetryableErrorException() {
        ConsumerRecord<String, StrikeOffPartnerObjections> recordWithNullPayload =
                new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, null);

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeStrikeOffObjectionsMessage(1, recordWithNullPayload));
    }

    @Test
    void consumeMessage_eventWithNullEventId_usesUnknownFallbackAndDelegates() {
        StrikeOffPartnerObjections event = new StrikeOffPartnerObjections();
        // eventId deliberately left null to exercise the "unknown" fallback at line 69
        ConsumerRecord<String, StrikeOffPartnerObjections> recordWithNullEventId =
                new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, event);

        assertDoesNotThrow(() -> consumer.consumeStrikeOffObjectionsMessage(1, recordWithNullEventId));
        verify(processorDispatcher).dispatch(eq(event), any(ProcessorLogContext.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_delegatesToProcessorDispatcher(boolean wasSuccessful) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> objectionRecord = triggerProcessedObjectionEvent(wasSuccessful);

        consumer.consumeProcessedStrikeOffObjectionsMessage(1, objectionRecord);

        verify(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_firstAttempt_nullAttemptNumber_doesNotThrow(boolean wasSuccessful) {
        // attemptNumber is null on the first delivery (no retry header present)
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> objectionRecord = triggerProcessedObjectionEvent(wasSuccessful);
        consumer.consumeProcessedStrikeOffObjectionsMessage(null, objectionRecord);

        verify(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_dispatcherThrows_rethrowsException(boolean wasSuccessful) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> objectionRecord = triggerProcessedObjectionEvent(wasSuccessful);
        doThrow(new RuntimeException("processing failed"))
                .when(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));

        assertThrows(RuntimeException.class,
                () -> consumer.consumeProcessedStrikeOffObjectionsMessage(1, objectionRecord));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_dispatcherThrowsNonRetryable_rethrowsAsIs(boolean wasSuccessful) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> objectionRecord = triggerProcessedObjectionEvent(wasSuccessful);
        doThrow(new NonRetryableErrorException("bad message"))
                .when(processorDispatcher).dispatch(eq(objectionRecord.value()), any(ProcessorLogContext.class));

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
                .dispatch(eq(event), any(ProcessorLogContext.class));

        // When / Then
        assertDoesNotThrow(() ->
                consumer.consumeProcessedStrikeOffObjectionsMessage(1, consumerRecord));

        verify(processorDispatcher).dispatch(eq(event), any(ProcessorLogContext.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_nullPayload_throwsNonRetryableErrorException(boolean wasSuccessful) {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> recordWithNullPayload =
                new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, null);

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeProcessedStrikeOffObjectionsMessage(1, recordWithNullPayload));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumeProcessedMessage_eventWithNullEventId_usesUnknownFallbackAndDelegates(boolean wasSuccessful) {
        StrikeOffPartnerObjectionsProcessed event = new StrikeOffPartnerObjectionsProcessed();
        // eventId deliberately left null to exercise the "unknown" fallback at line 69
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> recordWithNullEventId =
                new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, event);

        assertDoesNotThrow(() -> consumer.consumeProcessedStrikeOffObjectionsMessage(1, recordWithNullEventId));
        verify(processorDispatcher).dispatch(eq(event), any(ProcessorLogContext.class));
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
        return logAppender.list.stream()
                .map(ILoggingEvent::getArgumentArray)
                .filter(arguments -> arguments != null && arguments.length > 0)
                .map(arguments -> arguments[0])
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .map(log -> log.get("data"))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .filter(data -> operation.equals(data.get("operation")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No log found for operation " + operation));
    }

    private void assertNoSensitiveFailureTextLogged() {
        boolean sensitiveTextLogged = logAppender.list.stream()
                .anyMatch(event -> event.getFormattedMessage().contains(SENSITIVE_FAILURE_TEXT)
                        || Arrays.deepToString(event.getArgumentArray()).contains(SENSITIVE_FAILURE_TEXT));
        assertFalse(sensitiveTextLogged);
    }
}
