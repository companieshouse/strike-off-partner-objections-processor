package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.deserialization;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;

import java.io.IOException;
import java.io.Serial;
import java.util.Map;

/**
 * Custom deserializer for StrikeOffPartnerObjectionsProcessed using a specific datum reader.
 *
 * <p>Using a specific datum reader applies Avro logical type conversions directly during
 * deserialization (for example int epoch-day to LocalDate).
 */
public class DateLogicalTypeDeserializer implements Deserializer<StrikeOffPartnerObjectionsProcessed> {

    private static final Schema PROCESSED_EVENT_SCHEMA = StrikeOffPartnerObjectionsProcessed.getClassSchema();

    private final SpecificDatumReader<StrikeOffPartnerObjectionsProcessed> reader =
            new SpecificDatumReader<>(PROCESSED_EVENT_SCHEMA, PROCESSED_EVENT_SCHEMA);

    public DateLogicalTypeDeserializer() {
        // Intentionally empty: Kafka creates deserializers reflectively via the public no-arg constructor.
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No-op: reader is fully defined by the generated specific schema.
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
            throw new DateLogicalTypeDeserializationException(
                    "Failed to deserialize StrikeOffPartnerObjectionsProcessed", ioException);
        }
    }

    @Override
    public void close() {
        // No-op.
    }

    private static final class DateLogicalTypeDeserializationException extends RuntimeException {

        @Serial
        private static final long serialVersionUID = 1L;

        private DateLogicalTypeDeserializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
