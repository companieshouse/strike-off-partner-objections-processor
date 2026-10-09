package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import uk.gov.companieshouse.logging.util.DataMap;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Immutable structured logging context for one Kafka message.
 */
public final class ProcessorLogContext {
    public static final String KAFKA_MESSAGE_RECEIVED = "kafka_message_received";
    public static final String PROCESSING_STARTED = "processing_started";
    public static final String PROCESSING_COMPLETED = "processing_completed";
    public static final String PROCESSING_FAILED = "processing_failed";
    public static final String PROCESSING_SKIPPED = "processing_skipped";
    public static final String VALIDATION_STARTED = "validation_started";
    public static final String VALIDATION_COMPLETED = "validation_completed";
    public static final String INTERNAL_API_GET_OBJECTION_REQUEST = "internal_api_get_objection_request";
    public static final String INTERNAL_API_GET_OBJECTION_RESPONSE = "internal_api_get_objection_response";
    public static final String INTERNAL_API_GET_WITHDRAWAL_REQUEST = "internal_api_get_withdrawal_request";
    public static final String INTERNAL_API_GET_WITHDRAWAL_RESPONSE = "internal_api_get_withdrawal_response";
    public static final String INTERNAL_API_UPDATE_OBJECTION_REQUEST = "internal_api_update_objection_request";
    public static final String INTERNAL_API_UPDATE_OBJECTION_RESPONSE = "internal_api_update_objection_response";
    public static final String INTERNAL_API_UPDATE_WITHDRAWAL_REQUEST = "internal_api_update_withdrawal_request";
    public static final String INTERNAL_API_UPDATE_WITHDRAWAL_RESPONSE = "internal_api_update_withdrawal_response";
    public static final String CHIPS_REQUEST = "chips_request";
    public static final String CHIPS_RESPONSE = "chips_response";
    public static final String PROCESSED_OUTCOME_RECEIVED = "processed_outcome_received";
    public static final String RETRY = "retry";

    private final DataMap dataMap;

    private ProcessorLogContext(DataMap dataMap) {
        this.dataMap = dataMap;
    }

    public static ProcessorLogContext empty() {
        return new ProcessorLogContext(new DataMap.Builder().build());
    }

    public static ProcessorLogContext fromRecord(ConsumerRecord<String, ?> consumerRecord) {
        DataMap dataMap = new DataMap.Builder()
                .topic(consumerRecord.topic())
                .partition(consumerRecord.partition())
                .offset(consumerRecord.offset())
                .kafkaMessageKey(consumerRecord.key())
                .build();
        return new ProcessorLogContext(dataMap);
    }

    public ProcessorLogContext withIncomingEvent(StrikeOffPartnerObjections event) {
        return update(builder -> builder
                .companyNumber(event.getCompanyNumber())
                .correlationId(firstAvailable(event.getStrikeOffEventId(), event.getEventId()))
                .partnerOrganisation(event.getPartnerOrganisation())
                .source(event.getSource())
                .eventType(asString(event.getEventType())));
    }

    public ProcessorLogContext withProcessedEvent(StrikeOffPartnerObjectionsProcessed event) {
        return update(builder -> builder
                .companyNumber(event.getCompanyNumber())
                .correlationId(event.getStrikeOffEventId())
                .eventType(asString(event.getEventType()))
                .status(asString(event.getSuccessFailureIndicator())));
    }

    public ProcessorLogContext withOperation(String operation) {
        return update(builder -> builder.operation(operation));
    }

    public ProcessorLogContext withCompanyNumber(String companyNumber) {
        return update(builder -> builder.companyNumber(companyNumber));
    }

    public ProcessorLogContext withEventType(String eventType) {
        return update(builder -> builder.eventType(eventType));
    }

    public ProcessorLogContext withObjectionId(String objectionId) {
        return update(builder -> builder.objectionId(objectionId));
    }

    public ProcessorLogContext withWithdrawalId(String withdrawalId) {
        return update(builder -> builder.withdrawalId(withdrawalId));
    }

    public ProcessorLogContext withResource(String kind, String uri) {
        return update(builder -> builder.resourceKind(kind).resourceUri(uri));
    }

    public ProcessorLogContext withStatus(String status) {
        return update(builder -> builder.status(status));
    }

    public ProcessorLogContext withRetry(int retryCount, String retryReason) {
        return update(builder -> builder.retryCount(retryCount).retryReason(retryReason));
    }

    public ProcessorLogContext withErrorType(String errorType) {
        return update(builder -> builder.errorType(errorType));
    }

    public Map<String, Object> toLogMap() {
        return dataMap.getLogMap();
    }

    private ProcessorLogContext update(Consumer<DataMap.Builder> changes) {
        DataMap.Builder builder = new DataMap.Builder()
                .companyNumber(dataMap.companyNumber)
                .topic(dataMap.topic)
                .partition(dataMap.partition)
                .offset(dataMap.offset)
                .kafkaMessageKey(dataMap.kafkaMessageKey)
                .partnerOrganisation(dataMap.partnerOrganisation)
                .source(dataMap.source)
                .correlationId(dataMap.correlationId)
                .eventType(dataMap.eventType)
                .objectionId(dataMap.objectionId)
                .withdrawalId(dataMap.withdrawalId)
                .operation(dataMap.operation)
                .status(dataMap.status)
                .resourceId(dataMap.resourceId)
                .resourceKind(dataMap.resourceKind)
                .resourceUri(dataMap.resourceUri)
                .retryCount(dataMap.retryCount)
                .retryReason(dataMap.retryReason)
                .errorType(dataMap.errorType);
        changes.accept(builder);
        return new ProcessorLogContext(builder.build());
    }

    private static String firstAvailable(String primary, String fallback) {
        return primary != null ? primary : fallback;
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }
}
