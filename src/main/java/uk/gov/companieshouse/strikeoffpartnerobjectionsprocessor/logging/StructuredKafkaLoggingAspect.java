package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.logging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.logging.util.DataMap;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.DataMapHolder;

import java.util.Map;

import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

/**
 * Establishes structured logging context for incoming and processed Kafka events.
 */
@Aspect
@Component
public class StructuredKafkaLoggingAspect {

    private static final String COMPANY_NUMBER = "company_number";
    private static final String STRIKE_OFF_EVENT_ID = "strike_off_event_id";
    private static final String EVENT_TYPE = "event_type";

    /**
     * Keeps context available throughout listener execution and removes it afterwards.
     */
    @Around(value = "@annotation(org.springframework.kafka.annotation.KafkaListener)"
            + " && within(uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers.StrikeOffPartnerObjectionsKafkaConsumer)"
            + " && args(*, consumerRecord)", argNames = "joinPoint,consumerRecord")
    public Object logKafkaMessage(ProceedingJoinPoint joinPoint, ConsumerRecord<?, ?> consumerRecord)
            throws Throwable {
        DataMap dataMap = buildDataMap(consumerRecord);
        DataMapHolder.set(dataMap, buildLogMap(dataMap, consumerRecord.value()));
        try {
            return joinPoint.proceed();
        } finally {
            DataMapHolder.clear();
        }
    }

    private DataMap buildDataMap(ConsumerRecord<?, ?> consumerRecord) {
        DataMap.Builder builder = new DataMap.Builder()
                .topic(consumerRecord.topic())
                .partition(consumerRecord.partition())
                .offset(consumerRecord.offset());
        if (consumerRecord.value() instanceof StrikeOffPartnerObjections message) {
            builder.companyNumber(message.getCompanyNumber())
                    .requestId(message.getStrikeOffEventId());
        } else if (consumerRecord.value() instanceof StrikeOffPartnerObjectionsProcessed message) {
            builder.companyNumber(message.getCompanyNumber())
                    .requestId(message.getStrikeOffEventId());
        }
        return builder.build();
    }

    private Map<String, Object> buildLogMap(DataMap dataMap, Object payload) {
        Map<String, Object> logMap = dataMap.getLogMap();
        if (payload instanceof StrikeOffPartnerObjections message) {
            logMap.put(COMPANY_NUMBER, message.getCompanyNumber());
            logMap.put(STRIKE_OFF_EVENT_ID, message.getStrikeOffEventId());
            logMap.put("partner_organisation", message.getPartnerOrganisation());
            logMap.put(EVENT_TYPE, message.getEventType());
            logMap.put("event_id", message.getEventId());
        } else if (payload instanceof StrikeOffPartnerObjectionsProcessed message) {
            addProcessedContext(logMap, message);
        }
        return logMap;
    }

    private void addProcessedContext(Map<String, Object> logMap, StrikeOffPartnerObjectionsProcessed message) {
        logMap.put(COMPANY_NUMBER, message.getCompanyNumber());
        logMap.put(STRIKE_OFF_EVENT_ID, message.getStrikeOffEventId());
        logMap.put(EVENT_TYPE, message.getEventType());
        logMap.put("success_failure_indicator", message.getSuccessFailureIndicator());
        if (message.getSuccessFailureIndicator() == FAILURE) {
            logMap.put("error_message", message.getErrorMessage());
        }
        if (message.getSuccessFailureIndicator() == SUCCESS) {
            logMap.put("initial_expiration_on", message.getInitialExpirationOn());
        }
    }
}
