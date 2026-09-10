package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import consumer.deserialization.AvroDeserializer;
import consumer.serialization.AvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.deserialization.ProcessedEventDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for consuming and producing strike-off partner objections messages.
 * Configures separate consumer and producer factories for both incoming objections and
 * processed objections events, with appropriate error handling and date deserialization support.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
    private static final String ISOLATION_LEVEL_READ_COMMITTED = "read_committed";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.strikeoff.objections.group-id:default-group}")
    private String groupId;

    @Value("${kafka.strikeoff.processed-objections.group-id:default-processed-group}")
    private String processedGroupId;

    @Value("${kafka.session.timeout:10000}")
    private int sessionTimeout;

    @Value("${kafka.max.poll.interval:300000}")
    private int maxPollInterval;

    @Value("${kafka.heartbeat.interval:3000}")
    private int heartbeatInterval;

    @Value("${kafka.max.poll.records:500}")
    private int maxPollRecords;

    // =========================================================================
    // 1. Consumer Factories
    // =========================================================================
    @Bean
    public ConsumerFactory<String, StrikeOffPartnerObjections> consumerFactory() {
        return buildConsumerFactory(
                groupId,
                AvroDeserializer.class,
                new AvroDeserializer<>(StrikeOffPartnerObjections.class)
        );
    }

    @Bean
    public ConsumerFactory<String, StrikeOffPartnerObjectionsProcessed> processedConsumerFactory() {
        return buildConsumerFactory(
                processedGroupId,
                ProcessedEventDeserializer.class,
                new ProcessedEventDeserializer()
        );
    }

    private <T> ConsumerFactory<String, T> buildConsumerFactory(
            String consumerGroupId,
            Class<?> valueDeserializerClass,
            Deserializer<T> valueDeserializer) {
        Map<String, Object> props = baseConsumerProps(consumerGroupId);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, valueDeserializerClass);
        return new DefaultKafkaConsumerFactory<>(props,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(valueDeserializer));
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

    // =========================================================================
    // 2. Main Listener Container Factory
    // =========================================================================
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StrikeOffPartnerObjections> kafkaListenerContainerFactory(
            @Qualifier("consumerFactory") ConsumerFactory<String, StrikeOffPartnerObjections> consumerFactory,
            @Qualifier("kafkaConsumerTemplate") KafkaTemplate<String, StrikeOffPartnerObjections> kafkaConsumerTemplate) {
        return createListenerContainerFactory(consumerFactory, kafkaConsumerTemplate);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StrikeOffPartnerObjectionsProcessed>
            processedKafkaListenerContainerFactory(
                    @Qualifier("processedConsumerFactory")
                    ConsumerFactory<String, StrikeOffPartnerObjectionsProcessed> consumerFactory,
                    @Qualifier("processedKafkaConsumerTemplate")
                    KafkaTemplate<String, StrikeOffPartnerObjectionsProcessed> kafkaConsumerTemplate) {
        return createListenerContainerFactory(consumerFactory, kafkaConsumerTemplate);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory,
            KafkaTemplate<String, T> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setReplyTemplate(kafkaTemplate);
        return factory;
    }

    // =========================================================================
    // 3. Producer Factory & Template (Required for @RetryableTopic / DLT routing)
    // =========================================================================
    @Bean
    public ProducerFactory<String, StrikeOffPartnerObjections> producerFactory() {
        return createProducerFactory();
    }

    @Bean
    public ProducerFactory<String, StrikeOffPartnerObjectionsProcessed> processedProducerFactory() {
        return createProducerFactory();
    }

    private <T> ProducerFactory<String, T> createProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(name = "kafkaConsumerTemplate")
    public KafkaTemplate<String, StrikeOffPartnerObjections> kafkaConsumerTemplate(
            @Qualifier("producerFactory") ProducerFactory<String, StrikeOffPartnerObjections> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean(name = "processedKafkaConsumerTemplate")
    public KafkaTemplate<String, StrikeOffPartnerObjectionsProcessed> processedKafkaConsumerTemplate(
            @Qualifier("processedProducerFactory")
            ProducerFactory<String, StrikeOffPartnerObjectionsProcessed> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}

