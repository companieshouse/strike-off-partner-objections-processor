package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import consumer.exception.NonRetryableErrorException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NonRetryableTopicErrorInterceptorTest {

    private static final String ERROR_TOPIC = "strike-off-partner-objections-incoming-error";

    private final NonRetryableTopicErrorInterceptor interceptor = new NonRetryableTopicErrorInterceptor();

    @Test
    void onSend_redirectsToInvalidTopic_whenCauseExceptionIsNonRetryableSubclass() {
        ProducerRecord<String, Object> producerRecord = recordWithHeaders(
                ERROR_TOPIC,
                RuntimeException.class.getName(),
                InvalidStrikeOffMessageException.class.getName());

        ProducerRecord<String, Object> redirected = interceptor.onSend(producerRecord);

        assertEquals("strike-off-partner-objections-incoming-invalid", redirected.topic());
    }

    @Test
    void onSend_redirectsToInvalidTopic_whenExceptionHeaderIsNonRetryableBaseClass() {
        ProducerRecord<String, Object> producerRecord = recordWithHeaders(
                ERROR_TOPIC,
                NonRetryableErrorException.class.getName(),
                null);

        ProducerRecord<String, Object> redirected = interceptor.onSend(producerRecord);

        assertEquals("strike-off-partner-objections-incoming-invalid", redirected.topic());
    }

    @Test
    void onSend_keepsErrorTopic_whenFailureIsRetryable() {
        ProducerRecord<String, Object> producerRecord = recordWithHeaders(
                ERROR_TOPIC,
                RuntimeException.class.getName(),
                RuntimeException.class.getName());

        ProducerRecord<String, Object> redirected = interceptor.onSend(producerRecord);

        assertEquals(ERROR_TOPIC, redirected.topic());
    }

    private ProducerRecord<String, Object> recordWithHeaders(String topic, String exceptionFqcn, String causeFqcn) {
        RecordHeaders headers = new RecordHeaders();
        if (exceptionFqcn != null) {
            headers.add("kafka_exception-fqcn", exceptionFqcn.getBytes(StandardCharsets.UTF_8));
        }
        if (causeFqcn != null) {
            headers.add("kafka_exception-cause-fqcn", causeFqcn.getBytes(StandardCharsets.UTF_8));
        }
        return new ProducerRecord<>(topic, 0, 123L, "k", "v", headers);
    }
}

