package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.serialization;

import consumer.exception.NonRetryableErrorException;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;

import java.io.IOException;

/**
 * Custom deserializer for StrikeOffPartnerObjectionsProcessed using a specific datum reader.
 */
public class ProcessedEventDeserializer implements Deserializer<StrikeOffPartnerObjectionsProcessed> {

    private final SpecificDatumReader<StrikeOffPartnerObjectionsProcessed> reader =
            new SpecificDatumReader<>(StrikeOffPartnerObjectionsProcessed.class);

    @Override
    public StrikeOffPartnerObjectionsProcessed deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (Exception exception) {
            throw new NonRetryableErrorException(
                    "Failed to deserialize StrikeOffPartnerObjectionsProcessed", exception);
        }
    }
}

