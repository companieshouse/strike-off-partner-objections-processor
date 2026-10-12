package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.logging;

import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.logging.util.DataMap;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.DataMapHolder;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;

/**
 * Owns structured logging and context cleanup for each synchronous Kafka delivery.
 */
@Component
@Aspect
public class StructuredLoggingKafkaListenerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    private final int maxAttempts;

    public StructuredLoggingKafkaListenerAspect(@Value("${kafka.max-attempts}") int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    @Around("@annotation(org.springframework.kafka.annotation.KafkaListener)")
    public Object logMessage(ProceedingJoinPoint joinPoint) throws Throwable {
        Object[] arguments = joinPoint.getArgs();
        int attempt = arguments[0] instanceof Integer number ? number : 1;
        int retryCount = attempt > 1 ? attempt - 1 : 0;
        try {
            initialise((ConsumerRecord<?, ?>) arguments[1], retryCount);
            logLifecycle("Kafka message received", DataMapHolder.KAFKA_MESSAGE_RECEIVED);
            logLifecycle("Message processing started", DataMapHolder.PROCESSING_STARTED);
            logProcessedOutcome((ConsumerRecord<?, ?>) arguments[1]);
            Object result = joinPoint.proceed();
            logLifecycle("Message processing completed", DataMapHolder.PROCESSING_COMPLETED);
            return result;
        } catch (DuplicateRecordException exception) {
            logLifecycle("Duplicate event skipped", DataMapHolder.PROCESSING_SKIPPED);
            return null;
        } catch (Exception exception) {
            logFailure(exception, attempt);
            throw exception;
        } finally {
            DataMapHolder.clear();
        }
    }

    private static void initialise(ConsumerRecord<?, ?> message, int retryCount) {
        DataMap.Builder builder = new DataMap.Builder()
                .topic(message.topic())
                .partition(message.partition())
                .offset(message.offset())
                .kafkaMessageKey(message.key() == null ? null : message.key().toString())
                .retryCount(retryCount);
        if (message.value() instanceof StrikeOffPartnerObjections event) {
            builder.companyNumber(event.getCompanyNumber())
                    .correlationId(event.getStrikeOffEventId() == null ? event.getEventId() : event.getStrikeOffEventId())
                    .partnerOrganisation(event.getPartnerOrganisation())
                    .source(event.getSource())
                    .eventType(asString(event.getEventType()));
        } else if (message.value() instanceof StrikeOffPartnerObjectionsProcessed event) {
            builder.companyNumber(event.getCompanyNumber())
                    .correlationId(event.getStrikeOffEventId())
                    .eventType(asString(event.getEventType()));
        }
        DataMapHolder.initialise(builder);
    }

    private static void logProcessedOutcome(ConsumerRecord<?, ?> message) {
        if (message.value() instanceof StrikeOffPartnerObjectionsProcessed event) {
            LOG.info("Processed outcome received", DataMapHolder.snapshot()
                    .operation(DataMapHolder.PROCESSED_OUTCOME_RECEIVED)
                    .status(asString(event.getSuccessFailureIndicator())).build().getLogMap());
        }
    }

    private static void logLifecycle(String message, String operation) {
        LOG.info(message, DataMapHolder.snapshot().operation(operation).build().getLogMap());
    }

    private void logFailure(Exception exception, int attempt) {
        ApiFailureDetails apiFailure = findApiFailure(exception);
        String errorType = apiFailure == null ? exception.getClass().getSimpleName() : apiFailure.exceptionType();
        DataMap.Builder failure = DataMapHolder.snapshot().retryReason(errorType).errorType(errorType);
        if (apiFailure != null) {
            failure.status(String.valueOf(apiFailure.statusCode()));
        }
        if (exception instanceof NonRetryableErrorException || attempt >= maxAttempts) {
            LOG.error("Message processing failed",
                    failure.operation(DataMapHolder.PROCESSING_FAILED).build().getLogMap());
            return;
        }
        LOG.info("Message processing will be retried", failure.operation(DataMapHolder.RETRY).build().getLogMap());
    }

    private static ApiFailureDetails findApiFailure(Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable cause = failure;
        while (cause != null && visited.add(cause)) {
            if (cause instanceof ApiErrorResponseException apiException) {
                return new ApiFailureDetails(apiException.getStatusCode(), cause.getClass().getSimpleName());
            }
            if (cause instanceof ChipsSubmissionException chipsException) {
                return new ApiFailureDetails(chipsException.getStatusCode(), cause.getClass().getSimpleName());
            }
            cause = cause.getCause();
        }
        return null;
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private record ApiFailureDetails(int statusCode, String exceptionType) {
    }
}
