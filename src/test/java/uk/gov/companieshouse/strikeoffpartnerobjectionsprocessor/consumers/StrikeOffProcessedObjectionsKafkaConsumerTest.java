package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers;

import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessedOutcomeDispatcher;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StrikeOffProcessedObjectionsKafkaConsumerTest {

    @Mock
    private ProcessedOutcomeDispatcher processedOutcomeDispatcher;

    @InjectMocks
    private StrikeOffProcessedObjectionsKafkaConsumer consumer;

    @Test
    void consumeMessage_delegatesToDispatcher() {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord = validRecord();

        consumer.consumeProcessedObjectionsMessage(1, consumerRecord);

        verify(processedOutcomeDispatcher).dispatch(consumerRecord.value());
    }

    @Test
    void consumeMessage_nullAttemptNumber_firstDelivery_doesNotThrow() {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord = validRecord();

        assertDoesNotThrow(() -> consumer.consumeProcessedObjectionsMessage(null, consumerRecord));
        verify(processedOutcomeDispatcher).dispatch(consumerRecord.value());
    }

    @Test
    void consumeMessage_dispatcherThrowsRuntime_rethrows() {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord = validRecord();
        doThrow(new RuntimeException("processing failed"))
                .when(processedOutcomeDispatcher).dispatch(consumerRecord.value());

        assertThrows(RuntimeException.class, () -> consumer.consumeProcessedObjectionsMessage(1, consumerRecord));
    }

    @Test
    void consumeMessage_dispatcherThrowsNonRetryable_rethrowsAsIs() {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord = validRecord();
        doThrow(new NonRetryableErrorException("bad message"))
                .when(processedOutcomeDispatcher).dispatch(consumerRecord.value());

        assertThrows(NonRetryableErrorException.class, () -> consumer.consumeProcessedObjectionsMessage(1, consumerRecord));
    }

    @Test
    void consumeMessage_nullPayload_throwsNonRetryableErrorException() {
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> recordWithNullPayload =
                new ConsumerRecord<>("strike-off-partner-objections-processed", 0, 0L, null, null);

        assertThrows(NonRetryableErrorException.class,
                () -> consumer.consumeProcessedObjectionsMessage(1, recordWithNullPayload));
    }

    private ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> validRecord() {
        StrikeOffPartnerObjectionsProcessed event = new StrikeOffPartnerObjectionsProcessed(
                "strike-001",
                SuccessFailureIndicator.SUCCESS,
                null,
                EventType.OBJECTION,
                LocalDate.of(2026, 12, 31),
                "12345678"
        );
        return new ConsumerRecord<>("strike-off-partner-objections-processed", 0, 0L, null, event);
    }
}

