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
import uk.gov.companieshouse.logging.util.DataMap;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessedOutcomeDispatcher;

import java.util.Map;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;

/**
 * Kafka consumer for strike-off partner processed objections/withdrawals outcome events.
 *
 * <p>This component listens to the `strike-off-partner-objections-processed` topic,
 * logs message metadata, and delegates processing to {@link ProcessedOutcomeDispatcher}.
 *
 * <p>Retry behavior is managed by {@link RetryableTopic}. Exceptions of type
 * {@link NonRetryableErrorException} are excluded from retries and are routed directly
 * to the configured error topic.
 */
@Component
public class StrikeOffProcessedObjectionsKafkaConsumer {
    private final ProcessedOutcomeDispatcher processedOutcomeDispatcher;

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    public StrikeOffProcessedObjectionsKafkaConsumer(ProcessedOutcomeDispatcher processedOutcomeDispatcher) {
        this.processedOutcomeDispatcher = processedOutcomeDispatcher;
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
            topics = "${kafka.topic.strikeoff.processed}",
            groupId = "${kafka.strikeoff.processed.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeProcessedObjectionsMessage(
            final @Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false) Integer attemptNumber,
            ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord) {

        final StrikeOffPartnerObjectionsProcessed event = consumerRecord.value();
        if (event == null) {
            throw new NonRetryableErrorException("Missing StrikeOffPartnerObjectionsProcessed payload");
        }
        final String eventId = event.getStrikeOffEventId() != null ? event.getStrikeOffEventId() : "unknown";
        var logMap = buildKafkaLogMap(consumerRecord);
        try {
            LOG.infoContext(eventId, "Consumed processed outcome event", logMap);
            LOG.infoContext(eventId, "Kafka retry attempt: " + (attemptNumber == null ? 1 : attemptNumber), logMap);
            processedOutcomeDispatcher.dispatch(event);
            LOG.infoContext(eventId, "Processed outcome event handled successfully", logMap);
        } catch (Exception exception) {
            LOG.error("Error encountered in StrikeOffProcessedObjectionsKafkaConsumer: " + exception.getMessage(), exception, logMap);
            throw exception;
        }
    }

    private Map<String, Object> buildKafkaLogMap(ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord) {
        StrikeOffPartnerObjectionsProcessed msg = consumerRecord.value();
        var logMap = new DataMap.Builder()
                .topic(consumerRecord.topic())
                .partition(consumerRecord.partition())
                .offset(consumerRecord.offset())
                .companyNumber(msg != null ? msg.getCompanyNumber() : null)
                .build().getLogMap();
        logMap.put("strike_off_event_id", msg != null ? msg.getStrikeOffEventId() : null);
        logMap.put("event_type", msg != null ? msg.getEventType() : null);
        logMap.put("success_failure_indicator", msg != null ? msg.getSuccessFailureIndicator() : null);
        return logMap;
    }
}

