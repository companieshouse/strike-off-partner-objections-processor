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
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorDispatcher;

import java.util.Map;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerObjectionsProcessorConstants.APPLICATION_NAMESPACE;


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
            backOff = @BackOff(
                    delayString = "${kafka.backoff-delay}",
                    multiplierString = "${kafka.backoff-multiplier}",
                    maxDelayString = "${kafka.backoff-max-delay}"
            ),
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
        var logMap = buildKafkaLogMap(consumerRecord);
        try {

            LOG.infoContext(eventId, "Consumed objections/withdrawals event", logMap);
            LOG.infoContext(eventId, "Kafka retry attempt: " + (attemptNumber == null ? 1 : attemptNumber), logMap);
            processorDispatcher.dispatch(event);
            LOG.infoContext(eventId, "Event processed successfully", logMap);
        }
        catch (DuplicateRecordException duplicateRecordException) {
            LOG.info(duplicateRecordException.getMessage(), logMap);
        } catch (Exception exception){
            LOG.error("Error encountered in StrikeOffPartnerObjectionsKafkaConsumer: " + exception.getMessage(), exception, logMap);
            throw exception;
        }
    }

    private Map<String, Object> buildKafkaLogMap(ConsumerRecord<String, StrikeOffPartnerObjections> consumerRecord) {
        StrikeOffPartnerObjections msg = consumerRecord.value();
        var logMap = new DataMap.Builder()
                .topic(consumerRecord.topic())
                .partition(consumerRecord.partition())
                .offset(consumerRecord.offset())
                .companyNumber(msg != null ? msg.getCompanyNumber() : null)
                .build().getLogMap();
        logMap.put("strike_off_event_id", msg != null ? msg.getStrikeOffEventId() : null);
        logMap.put("partner_organisation", msg != null ? msg.getPartnerOrganisation() : null);
        logMap.put("event_type", msg != null ? msg.getEventType() : null);
        logMap.put("event_id", msg != null ? msg.getEventId() : null);
        return logMap;
    }
}
