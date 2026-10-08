package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import consumer.deserialization.AvroDeserializer;
import consumer.serialization.AvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.serialization.ProcessedEventDeserializer;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.serialization.ProcessedEventSerializer;

/**
 * Kafka configuration for consuming and producing strike-off partner objections messages.
 * Configures separate consumer and producer factories for both incoming objections and
 * processed objections events, with appropriate error handling and date deserialization support.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.strikeoff.objections.group-id}")
    private String groupId;

    @Value("${kafka.strikeoff.processed-objections.group-id}")
    private String processedGroupId;

    @Value("${kafka.session.timeout}")
    private int sessionTimeout;

    @Value("${kafka.max.poll.interval}")
    private int maxPollInterval;

    @Value("${kafka.heartbeat.interval}")
    private int heartbeatInterval;

    @Value("${kafka.max.poll.records}")
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
        return builder().buildConsumerFactory(consumerGroupId, valueDeserializerClass, valueDeserializer);
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
        return builder().createListenerContainerFactory(consumerFactory, kafkaTemplate);
    }

    // =========================================================================
    // 3. Producer Factory & Template (Required for @RetryableTopic / DLT routing)
    // =========================================================================
    @Bean
    public ProducerFactory<String, StrikeOffPartnerObjections> producerFactory() {
        return builder().createProducerFactory(AvroSerializer.class);
    }

    @Bean
    public ProducerFactory<String, StrikeOffPartnerObjectionsProcessed> processedProducerFactory() {
        return builder().createProducerFactory(ProcessedEventSerializer.class);
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

    private KafkaFactoryBuilder builder() {
        return new KafkaFactoryBuilder(
                bootstrapServers,
                sessionTimeout,
                maxPollInterval,
                heartbeatInterval,
                maxPollRecords);
    }
}
