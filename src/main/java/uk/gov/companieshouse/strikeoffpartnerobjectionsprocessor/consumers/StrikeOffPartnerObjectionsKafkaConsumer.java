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
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorDispatcher;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.DataMapHolder;

import java.util.function.Consumer;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;


/**
 * Kafka consumer for strike-off partner objections events.
 *
 * <p>This component listens to the configured incoming topic, logs message metadata,
 * and delegates processing to {@link ProcessorDispatcher}.
 *
 * <p>Retry behavior is managed by {@link RetryableTopic}. Exceptions of type
 * {@link NonRetryableErrorException} are excluded from retries and are routed directly
 * to the configured error topic.
 */
@Component
public class StrikeOffPartnerObjectionsKafkaConsumer {
    private final ProcessorDispatcher processorDispatcher;

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

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
            final @Header( name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false ) Integer attemptNumber,
            ConsumerRecord<String, StrikeOffPartnerObjections> consumerRecord) {

        final StrikeOffPartnerObjections event = consumerRecord.value();
        if (event == null) {
            throw new NonRetryableErrorException("Missing StrikeOffPartnerObjections payload");
        }
        final String eventId = event.getEventId() != null ? event.getEventId() : "unknown";
        logAndDispatchEvent(eventId, attemptNumber, event, processorDispatcher::dispatch);
    }

    // This topic is populated once chips has completed processing the objection or withdrawal
    // This method is tasked with consuming this topic and making available for downstream processing
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
            final @Header( name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false ) Integer attemptNumber,
            ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord) {

        final StrikeOffPartnerObjectionsProcessed event = consumerRecord.value();
        if (event == null) {
            throw new NonRetryableErrorException("Missing StrikeOffPartnerObjectionsProcessed payload");
        }
        final String eventId = event.getStrikeOffEventId() != null ? event.getStrikeOffEventId() : "unknown";
        logAndDispatchEvent(eventId, attemptNumber, event, processorDispatcher::dispatch);
    }

    private <T> void logAndDispatchEvent(String eventId,
                                         Integer attemptNumber,
                                         T event,
                                         Consumer<T> dispatchAction) {
        try {
            LOG.infoContext(eventId, "Consumed objections/withdrawals event", DataMapHolder.getLogMap());
            LOG.infoContext(eventId, "Kafka retry attempt: " + (attemptNumber == null ? 1 : attemptNumber), DataMapHolder.getLogMap());
            dispatchAction.accept(event);
            LOG.infoContext(eventId, "Event processed successfully", DataMapHolder.getLogMap());
        } catch (DuplicateRecordException duplicateRecordException) {
            LOG.info(duplicateRecordException.getMessage(), DataMapHolder.getLogMap());
        } catch (Exception exception) {
            LOG.error("Error encountered in StrikeOffPartnerObjectionsKafkaConsumer: " + exception.getMessage(), exception, DataMapHolder.getLogMap());
            throw exception;
        }
    }

}
