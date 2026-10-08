package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for shared Kafka consumer/producer/factory wiring used by topic-specific configs.
 */
final class KafkaFactoryBuilder {

    private static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
    private static final String ISOLATION_LEVEL_READ_COMMITTED = "read_committed";

    private final String bootstrapServers;
    private final int sessionTimeout;
    private final int maxPollInterval;
    private final int heartbeatInterval;
    private final int maxPollRecords;

    KafkaFactoryBuilder(String bootstrapServers,
                        int sessionTimeout,
                        int maxPollInterval,
                        int heartbeatInterval,
                        int maxPollRecords) {
        this.bootstrapServers = bootstrapServers;
        this.sessionTimeout = sessionTimeout;
        this.maxPollInterval = maxPollInterval;
        this.heartbeatInterval = heartbeatInterval;
        this.maxPollRecords = maxPollRecords;
    }

    <T> ConsumerFactory<String, T> buildConsumerFactory(
            String consumerGroupId,
            Class<?> valueDeserializerClass,
            Deserializer<T> valueDeserializer) {
        Map<String, Object> props = baseConsumerProps(consumerGroupId);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, valueDeserializerClass);
        return new DefaultKafkaConsumerFactory<>(props,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(valueDeserializer));
    }

    <T> ProducerFactory<String, T> createProducerFactory(Class<?> valueSerializerClass) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        return new DefaultKafkaProducerFactory<>(props);
    }

    <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory,
            KafkaTemplate<String, T> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setReplyTemplate(kafkaTemplate);
        return factory;
    }

    private Map<String, Object> baseConsumerProps(String consumerGroupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ISOLATION_LEVEL_READ_COMMITTED);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return props;
    }
}
