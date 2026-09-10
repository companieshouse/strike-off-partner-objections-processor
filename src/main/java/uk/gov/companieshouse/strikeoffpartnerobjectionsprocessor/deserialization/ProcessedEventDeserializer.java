package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.deserialization;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;

import java.io.IOException;
import java.io.Serial;

/**
 * Custom deserializer for StrikeOffPartnerObjectionsProcessed using a specific datum reader.
 */
public class ProcessedEventDeserializer implements Deserializer<StrikeOffPartnerObjectionsProcessed> {

    private static final Schema PROCESSED_EVENT_SCHEMA = StrikeOffPartnerObjectionsProcessed.getClassSchema();

    private final SpecificDatumReader<StrikeOffPartnerObjectionsProcessed> reader =
            new SpecificDatumReader<>(PROCESSED_EVENT_SCHEMA, PROCESSED_EVENT_SCHEMA);

    public ProcessedEventDeserializer() {
        // Intentionally empty: Kafka creates deserializers reflectively via the public no-arg constructor.
    }

    @Override
    public StrikeOffPartnerObjectionsProcessed deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (IOException | RuntimeException ioException) {
            throw new ProcessedEventDeserializationException(
                    "Failed to deserialize StrikeOffPartnerObjectionsProcessed", ioException);
        }
    }


    private static final class ProcessedEventDeserializationException extends RuntimeException {

        @Serial
        private static final long serialVersionUID = 1L;

        private ProcessedEventDeserializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

