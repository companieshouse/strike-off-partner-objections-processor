package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers;

import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorDispatcher;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

@ExtendWith(MockitoExtension.class)
class StrikeOffPartnerObjectionsKafkaConsumerTest {

    @Mock
    private ProcessorDispatcher processorDispatcher;

    @InjectMocks
    private StrikeOffPartnerObjectionsKafkaConsumer consumer;

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
}

