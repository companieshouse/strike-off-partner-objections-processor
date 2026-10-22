package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Intercepts records targeting -error topics and redirects non-retryable failures to -invalid.
 */
public class NonRetryableTopicErrorInterceptor implements ProducerInterceptor<String, Object> {

    private static final String ERROR_TOPIC_SUFFIX = "-error";
    private static final String INVALID_TOPIC_SUFFIX = "-invalid";
    private static final String EXCEPTION_FQCN_HEADER = "kafka_exception-fqcn";
    private static final String EXCEPTION_CAUSE_FQCN_HEADER = "kafka_exception-cause-fqcn";

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> producerRecord) {
        if (producerRecord == null || !producerRecord.topic().endsWith(ERROR_TOPIC_SUFFIX)) {
            return producerRecord;
        }

        String nextTopic = isNonRetryable(producerRecord)
                ? producerRecord.topic().replace(ERROR_TOPIC_SUFFIX, INVALID_TOPIC_SUFFIX)
                : producerRecord.topic();

        if (nextTopic.equals(producerRecord.topic())) {
            return producerRecord;
        }

        return new ProducerRecord<>(
                nextTopic,
                producerRecord.partition(),
                producerRecord.timestamp(),
                producerRecord.key(),
                producerRecord.value(),
                producerRecord.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // No-op.
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op.
    }

    private boolean isNonRetryable(ProducerRecord<String, Object> producerRecord) {
        Header causeHeader = producerRecord.headers().lastHeader(EXCEPTION_CAUSE_FQCN_HEADER);
        if (isNonRetryable(causeHeader)) {
            return true;
        }

        Header exceptionHeader = producerRecord.headers().lastHeader(EXCEPTION_FQCN_HEADER);
        return isNonRetryable(exceptionHeader);
    }

    private boolean isNonRetryable(Header header) {
        if (header == null) {
            return false;
        }

        try {
            String fqcn = new String(header.value(), StandardCharsets.UTF_8);
            Class<?> exceptionClass = Class.forName(fqcn);
            return NonRetryableErrorException.class.isAssignableFrom(exceptionClass);
        } catch (ClassNotFoundException exception) {
            return false;
        }
    }
}

