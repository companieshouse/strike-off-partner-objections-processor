package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import consumer.deserialization.AvroDeserializer;
import consumer.serialization.AvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.strikeoff.objections.group-id:default-group}")
    private String groupId;

    @Value("${kafka.consumer.session-timeout-ms:10000}")
    private int sessionTimeout;

    @Value("${kafka.consumer.max-poll-interval-ms:300000}")
    private int maxPollInterval;

    @Value("${kafka.consumer.heartbeat-interval-ms:3000}")
    private int heartbeatInterval;

    @Value("${kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;


    // =========================================================================
    // 1. Consumer Factory Configuration
    // =========================================================================
    @Bean
    public ConsumerFactory<String, StrikeOffPartnerObjections> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, AvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new AvroDeserializer<>(StrikeOffPartnerObjections.class));
    }

    // =========================================================================
    // 2. Main Listener Container Factory
    // =========================================================================
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StrikeOffPartnerObjections> kafkaListenerContainerFactory(
            ConsumerFactory<String, StrikeOffPartnerObjections> consumerFactory,
            KafkaTemplate<String, StrikeOffPartnerObjections> kafkaConsumerTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, StrikeOffPartnerObjections> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // CRITICAL FOR RETRY TOPICS: Link the template used to forward failed messages
        factory.setReplyTemplate(kafkaConsumerTemplate);

        return factory;
    }

    // =========================================================================
    // 3. Producer Factory & Template (Required for @RetryableTopic / DLT routing)
    // =========================================================================
    @Bean
    public ProducerFactory<String, StrikeOffPartnerObjections> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(name = "kafkaConsumerTemplate")
    public KafkaTemplate<String, StrikeOffPartnerObjections> kafkaConsumerTemplate(ProducerFactory<String, StrikeOffPartnerObjections> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
