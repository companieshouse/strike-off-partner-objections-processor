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

import java.util.Map;

import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.buildBaseKafkaLogMap;


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
        var logMap = buildKafkaLogMapForIncomingObjections(consumerRecord);
        logAndDispatchEvent(eventId, attemptNumber, logMap, () -> processorDispatcher.dispatch(event));
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
        var logMap = buildKafkaLogMapForProcessedObjections(consumerRecord);
        logAndDispatchEvent(eventId, attemptNumber, logMap, () -> processorDispatcher.dispatch(event));
    }

    private void logAndDispatchEvent(String eventId, Integer attemptNumber, Map<String, Object> logMap, Runnable dispatchAction) {
        try {
            LOG.infoContext(eventId, "Consumed objections/withdrawals event", logMap);
            LOG.infoContext(eventId, "Kafka retry attempt: " + (attemptNumber == null ? 1 : attemptNumber), logMap);
            dispatchAction.run();
            LOG.infoContext(eventId, "Event processed successfully", logMap);
        } catch (DuplicateRecordException duplicateRecordException) {
            LOG.info(duplicateRecordException.getMessage(), logMap);
        } catch (Exception exception) {
            LOG.error("Error encountered in StrikeOffPartnerObjectionsKafkaConsumer: " + exception.getMessage(), exception, logMap);
            throw exception;
        }
    }

    private Map<String, Object> buildKafkaLogMapForIncomingObjections(ConsumerRecord<String, StrikeOffPartnerObjections> consumerRecord) {
        StrikeOffPartnerObjections msg = consumerRecord.value();
        Map<String, Object> logMap = buildBaseKafkaLogMap(consumerRecord);
        logMap.put("company_number", msg != null ? msg.getCompanyNumber() : null);
        logMap.put("strike_off_event_id", msg != null ? msg.getStrikeOffEventId() : null);
        logMap.put("partner_organisation", msg != null ? msg.getPartnerOrganisation() : null);
        logMap.put("event_type", msg != null ? msg.getEventType() : null);
        logMap.put("event_id", msg != null ? msg.getEventId() : null);
        return logMap;
    }

    private Map<String, Object> buildKafkaLogMapForProcessedObjections(ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord) {
        StrikeOffPartnerObjectionsProcessed msg = consumerRecord.value();
        Map<String, Object> logMap = buildBaseKafkaLogMap(consumerRecord);
        logMap.put("strike_off_event_id", msg != null ? msg.getStrikeOffEventId() : null);
        logMap.put("event_type", msg != null ? msg.getEventType() : null);
        logMap.put("success_failure_indicator", msg != null ? msg.getSuccessFailureIndicator() : null);
        if (msg != null && msg.getSuccessFailureIndicator() == FAILURE) {
            logMap.put("error_message", msg.getErrorMessage());
        }
        if (msg != null && msg.getSuccessFailureIndicator() == SUCCESS) {
            logMap.put("initial_expiration_on", msg.getInitialExpirationOn());
        }
        return logMap;
    }
}
