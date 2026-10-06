package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.serialization;

import consumer.exception.NonRetryableErrorException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;

import java.io.ByteArrayOutputStream;

/**
 * Custom serializer for StrikeOffPartnerObjectionsProcessed using a specific datum writer.
 */
public class ProcessedEventSerializer implements Serializer<StrikeOffPartnerObjectionsProcessed> {

    private final SpecificDatumWriter<StrikeOffPartnerObjectionsProcessed> writer =
            new SpecificDatumWriter<>(StrikeOffPartnerObjectionsProcessed.class);


    @Override
    public byte[] serialize(String topic, StrikeOffPartnerObjectionsProcessed data) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            writer.write(data, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (Exception exception) {
            throw new NonRetryableErrorException(
                    "Failed to serialize StrikeOffPartnerObjectionsProcessed for topic=" + topic,
                    exception
            );
        }
    }
}