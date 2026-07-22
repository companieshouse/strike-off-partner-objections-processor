package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RetryConfigurationPropertiesTest {

    @Test
    void applicationProperties_defineRetryDefaultsAndEnvOverrides() throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            assertNotNull(inputStream);
            properties.load(inputStream);
        }

        assertEquals("${MAX_ATTEMPTS:5}", properties.getProperty("kafka.max-attempts"));
        assertEquals("${BACKOFF_DELAY:1000}", properties.getProperty("kafka.backoff-delay"));
        assertEquals("${BACKOFF_MULTIPLIER:2.0}", properties.getProperty("kafka.backoff-multiplier"));
        assertEquals("${BACKOFF_MAX_DELAY:16000}", properties.getProperty("kafka.backoff-max-delay"));
    }
}

