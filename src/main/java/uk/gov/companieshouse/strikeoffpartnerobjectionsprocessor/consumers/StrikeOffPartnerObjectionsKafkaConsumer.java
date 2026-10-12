package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers;

import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorDispatcher;

/**
 * Kafka consumer for strike-off partner objections events.
 *
 * <p>This component listens to the configured incoming and processed-outcome topics,
 * delegates processing to {@link ProcessorDispatcher}, with message logging managed by the
 * Kafka listener aspect.
 *
 * <p>Retry behaviour is managed by {@link RetryableTopic}. Exceptions of type
 * {@link NonRetryableErrorException} are excluded from retries and routed to the
 * configured error topic.
 */
@Component
public class StrikeOffPartnerObjectionsKafkaConsumer {
    private final ProcessorDispatcher processorDispatcher;

    public StrikeOffPartnerObjectionsKafkaConsumer(ProcessorDispatcher processorDispatcher) {
        this.processorDispatcher = processorDispatcher;
    }

    @RetryableTopic(
            attempts = "${kafka.max-attempts}",
            backOff = @BackOff(delayString = "${kafka.backoff-delay}"),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            dltTopicSuffix = "-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class,
            kafkaTemplate = "kafkaConsumerTemplate"
    )
    @KafkaListener(
            topics = "${kafka.topic.strikeoff.objections}",
            groupId = "${kafka.strikeoff.objections.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeStrikeOffObjectionsMessage(
            final @Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false) Integer attemptNumber,
            ConsumerRecord<String, StrikeOffPartnerObjections> consumerRecord) {

        StrikeOffPartnerObjections event = consumerRecord.value();
        if (event == null) {
            throw new NonRetryableErrorException("Missing StrikeOffPartnerObjections payload");
        }
        processorDispatcher.dispatch(event);
    }

    /**
     * Consumes outcomes published after CHIPS has completed processing an objection or withdrawal,
     * then makes those outcomes available for downstream processing.
     */
    @RetryableTopic(
            attempts = "${kafka.max-attempts}",
            backOff = @BackOff(delayString = "${kafka.backoff-delay}"),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            dltTopicSuffix = "-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class,
            kafkaTemplate = "processedKafkaConsumerTemplate"
    )
    @KafkaListener(
            topics = "${kafka.topic.strikeoff.processed-objections}",
            groupId = "${kafka.strikeoff.processed-objections.group-id}",
            containerFactory = "processedKafkaListenerContainerFactory"
    )
    public void consumeProcessedStrikeOffObjectionsMessage(
            final @Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false) Integer attemptNumber,
            ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord) {

        StrikeOffPartnerObjectionsProcessed event = consumerRecord.value();
        if (event == null) {
            throw new NonRetryableErrorException("Missing StrikeOffPartnerObjectionsProcessed payload");
        }
        processorDispatcher.dispatch(event);
    }

}
