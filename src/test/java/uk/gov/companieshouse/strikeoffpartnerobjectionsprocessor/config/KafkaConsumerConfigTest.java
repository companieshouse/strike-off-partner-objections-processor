package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import consumer.deserialization.AvroDeserializer;
import consumer.serialization.AvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.test.util.ReflectionTestUtils;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class KafkaConsumerConfigTest {

    private KafkaConsumerConfig config;

    @BeforeEach
    void setUp() {
        config = new KafkaConsumerConfig();
        ReflectionTestUtils.setField(config, "bootstrapServers", "localhost:9092");
        ReflectionTestUtils.setField(config, "groupId",          "test-group");
        ReflectionTestUtils.setField(config, "sessionTimeout",   10000);
        ReflectionTestUtils.setField(config, "maxPollInterval",  300000);
        ReflectionTestUtils.setField(config, "heartbeatInterval",3000);
        ReflectionTestUtils.setField(config, "maxPollRecords",   500);
    }

    @Test
    void consumerFactory_isDefaultKafkaConsumerFactory() {
        assertNotNull(config.consumerFactory());
        ConsumerFactory<String, StrikeOffPartnerObjections> factory = config.consumerFactory();
        assertNotNull(factory);
        assertEquals(DefaultKafkaConsumerFactory.class, factory.getClass());
    }

    @Test
    void consumerFactory_assertCorrectAttributesSet() {
        DefaultKafkaConsumerFactory<String, StrikeOffPartnerObjections> factory =
                (DefaultKafkaConsumerFactory<String, StrikeOffPartnerObjections>) config.consumerFactory();
        assertEquals("localhost:9092",
                factory.getConfigurationProperties().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("test-group",
                factory.getConfigurationProperties().get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("earliest",
                factory.getConfigurationProperties().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("read_committed",
                factory.getConfigurationProperties().get(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        assertEquals(ErrorHandlingDeserializer.class,
                factory.getConfigurationProperties().get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(ErrorHandlingDeserializer.class,
                factory.getConfigurationProperties().get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals(StringDeserializer.class,
                factory.getConfigurationProperties().get(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS));
        assertEquals(AvroDeserializer.class,
                factory.getConfigurationProperties().get(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS));
        assertFalse((Boolean) factory.getConfigurationProperties()
                .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(10000,
                factory.getConfigurationProperties().get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(300000,
                factory.getConfigurationProperties().get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(3000,
                factory.getConfigurationProperties().get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(500,
                factory.getConfigurationProperties().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    void producerFactory_isDefaultKafkaProducerFactory() {
        assertNotNull(config.producerFactory());
        assertEquals(DefaultKafkaProducerFactory.class, config.producerFactory().getClass());
    }

    @Test
    void producerFactory_bootstrapServersSet() {
        DefaultKafkaProducerFactory<String, StrikeOffPartnerObjections> factory =
                (DefaultKafkaProducerFactory<String, StrikeOffPartnerObjections>) config.producerFactory();
        assertEquals("localhost:9092",
                factory.getConfigurationProperties().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class,
                factory.getConfigurationProperties().get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(AvroSerializer.class,
                factory.getConfigurationProperties().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void kafkaConsumerTemplate_isNotNull() {
        KafkaTemplate<String, StrikeOffPartnerObjections> template =
                config.kafkaConsumerTemplate(config.producerFactory());
        assertNotNull(template);
    }

    @SuppressWarnings("unchecked")
    @Test
    void kafkaListenerContainerFactory_consumerFactoryIsSet() {
        ConsumerFactory<String, StrikeOffPartnerObjections> consumerFactory = config.consumerFactory();
        KafkaTemplate<String, StrikeOffPartnerObjections> template = mock(KafkaTemplate.class);

        ConcurrentKafkaListenerContainerFactory<String, StrikeOffPartnerObjections> factory =
                config.kafkaListenerContainerFactory(consumerFactory, template);
        assertNotNull(factory);
        assertEquals(consumerFactory, factory.getConsumerFactory());
    }
}
