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

/**
 * Establishes structured logging context for incoming and processed Kafka events.
 */
@Aspect
@Component
public class StructuredKafkaLoggingAspect {

    /**
     * Keeps context available throughout listener execution and removes it afterwards.
     */
    @Around(value = "@annotation(org.springframework.kafka.annotation.KafkaListener)"
            + " && within(uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers.StrikeOffPartnerObjectionsKafkaConsumer)"
            + " && args(*, consumerRecord)", argNames = "joinPoint,consumerRecord")
    public Object logKafkaMessage(ProceedingJoinPoint joinPoint, ConsumerRecord<?, ?> consumerRecord)
            throws Throwable {
        try {
            populateContext(consumerRecord);
            return joinPoint.proceed();
        } finally {
            DataMapHolder.clear();
        }
    }

    private void populateContext(ConsumerRecord<?, ?> consumerRecord) {
        DataMap.Builder builder = DataMapHolder.getBuilder()
                .topic(consumerRecord.topic())
                .partition(consumerRecord.partition())
                .offset(consumerRecord.offset());
        if (consumerRecord.value() instanceof StrikeOffPartnerObjections message) {
            addMessageContext(builder, message.getCompanyNumber(), message.getStrikeOffEventId());
            builder.partnerOrganisation(message.getPartnerOrganisation());
        } else if (consumerRecord.value() instanceof StrikeOffPartnerObjectionsProcessed message) {
            addMessageContext(builder, message.getCompanyNumber(), message.getStrikeOffEventId());
        }
    }

    private void addMessageContext(DataMap.Builder builder, String companyNumber, String strikeOffEventId) {
        builder.companyNumber(companyNumber)
                .requestId(strikeOffEventId);
    }
}
