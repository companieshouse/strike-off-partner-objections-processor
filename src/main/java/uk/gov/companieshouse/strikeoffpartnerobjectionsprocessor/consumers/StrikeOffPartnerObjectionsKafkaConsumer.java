package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.consumers;

import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor.ProcessorDispatcher;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.ProcessorLogContext;

import java.util.function.Consumer;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;

/**
 * Kafka consumer for strike-off partner objections events.
 *
 * <p>This component listens to the configured incoming and processed-outcome topics,
 * logs message metadata, and delegates processing to {@link ProcessorDispatcher}.
 *
 * <p>Retry behaviour is managed by {@link RetryableTopic}. Exceptions of type
 * {@link NonRetryableErrorException} are excluded from retries and routed to the
 * configured error topic.
 */
@Component
public class StrikeOffPartnerObjectionsKafkaConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    private final ProcessorDispatcher processorDispatcher;

    @Value("${kafka.max-attempts}")
    private int maxAttempts = 1;

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
        ProcessorLogContext logContext = ProcessorLogContext.fromRecord(consumerRecord);
        if (event != null) {
            logContext = logContext.withIncomingEvent(event);
        }
        logAndDispatchEvent(attemptNumber, logContext, attemptContext -> {
            if (event == null) {
                throw new NonRetryableErrorException("Missing StrikeOffPartnerObjections payload");
            }
            processorDispatcher.dispatch(event, attemptContext);
        });
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
        ProcessorLogContext logContext = ProcessorLogContext.fromRecord(consumerRecord);
        if (event != null) {
            logContext = logContext.withProcessedEvent(event);
        }
        logAndDispatchEvent(attemptNumber, logContext, attemptContext -> {
            if (event == null) {
                throw new NonRetryableErrorException("Missing StrikeOffPartnerObjectionsProcessed payload");
            }
            LOG.info("Processed outcome received",
                    attemptContext.withOperation(ProcessorLogContext.PROCESSED_OUTCOME_RECEIVED).toLogMap());
            processorDispatcher.dispatch(event, attemptContext);
        });
    }

    private void logAndDispatchEvent(
            Integer attemptNumber,
            ProcessorLogContext logContext,
            Consumer<ProcessorLogContext> dispatchAction) {
        int deliveryAttempt = attemptNumber == null ? 1 : attemptNumber;
        int retryCount = Math.max(deliveryAttempt - 1, 0);
        ProcessorLogContext attemptContext = logContext.withRetry(retryCount, null);
        LOG.info("Kafka message received",
                attemptContext.withOperation(ProcessorLogContext.KAFKA_MESSAGE_RECEIVED).toLogMap());

        try {
            LOG.info("Message processing started",
                    attemptContext.withOperation(ProcessorLogContext.PROCESSING_STARTED).toLogMap());
            dispatchAction.accept(attemptContext);
            LOG.info("Message processing completed",
                    attemptContext.withOperation(ProcessorLogContext.PROCESSING_COMPLETED).toLogMap());
        } catch (DuplicateRecordException exception) {
            ProcessorLogContext duplicateContext = exception.getLogContext() == null
                    ? attemptContext : exception.getLogContext();
            LOG.info("Duplicate event skipped",
                    duplicateContext.withRetry(retryCount, null)
                            .withOperation(ProcessorLogContext.PROCESSING_SKIPPED).toLogMap());
        } catch (Exception exception) {
            logProcessingFailure(exception, attemptContext, deliveryAttempt, retryCount);
            throw exception;
        }
    }

    private void logProcessingFailure(
            Exception exception, ProcessorLogContext logContext, int deliveryAttempt, int retryCount) {
        ApiFailureDetails apiFailure = findApiFailure(exception);
        String errorType = apiFailure == null
                ? exception.getClass().getSimpleName()
                : apiFailure.exceptionType();
        ProcessorLogContext failureContext = logContext
                .withRetry(retryCount, errorType)
                .withErrorType(errorType);
        if (apiFailure != null) {
            failureContext = failureContext.withStatus(String.valueOf(apiFailure.statusCode()));
        }
        boolean terminalFailure = exception instanceof NonRetryableErrorException
                || deliveryAttempt >= maxAttempts;

        if (terminalFailure) {
            LOG.error("Message processing failed",
                    failureContext.withOperation(ProcessorLogContext.PROCESSING_FAILED).toLogMap());
            return;
        }
        LOG.info("Message processing will be retried",
                failureContext.withOperation(ProcessorLogContext.RETRY).toLogMap());
    }

    private static ApiFailureDetails findApiFailure(Throwable failure) {
        Throwable cause = failure;
        while (cause != null) {
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

    private record ApiFailureDetails(int statusCode, String exceptionType) {
    }

}
