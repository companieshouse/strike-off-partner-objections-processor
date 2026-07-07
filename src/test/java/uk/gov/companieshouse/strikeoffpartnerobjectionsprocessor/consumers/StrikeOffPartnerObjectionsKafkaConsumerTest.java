package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers;

import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorDispatcher;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

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
        consumer.consumeStrikeOffObjectionsMessage(null, triggerObjectionEvent());

        verify(processorDispatcher).dispatch(triggerObjectionEvent().value());
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

    private ConsumerRecord<String, StrikeOffPartnerObjections> triggerObjectionEvent() {
        StrikeOffPartnerObjections event = StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setEventType(EventType.OBJECTION)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();
        return new ConsumerRecord<>("strike-off-partner-objections-incoming", 0, 0L, null, event);
    }
}

