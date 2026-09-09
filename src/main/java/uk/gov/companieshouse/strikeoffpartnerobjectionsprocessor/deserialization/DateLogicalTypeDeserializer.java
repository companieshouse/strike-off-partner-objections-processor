package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.deserialization;

import consumer.deserialization.AvroDeserializer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;
import org.apache.avro.Schema;

/**
 * Custom deserializer for StrikeOffPartnerObjectionsProcessed that handles Avro date logical type
 * conversion. The generated Avro class fails to handle the date logical type (int days since epoch
 * → LocalDate) during deserialization, so this wrapper catches the ClassCastException and performs
 * manual conversion.
 */
public class DateLogicalTypeDeserializer implements Deserializer<StrikeOffPartnerObjectionsProcessed> {

    private final AvroDeserializer<StrikeOffPartnerObjectionsProcessed> delegate;
    public final Class<?> avroClass;

    public DateLogicalTypeDeserializer() {
        this.delegate = new AvroDeserializer<>(StrikeOffPartnerObjectionsProcessed.class);
        this.avroClass = StrikeOffPartnerObjectionsProcessed.class;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public StrikeOffPartnerObjectionsProcessed deserialize(String topic, byte[] data) {
        try {
            return delegate.deserialize(topic, data);
        } catch (Exception e) {
            if (isDateConversionError(e)) {
                try {
                    return deserializeWithManualDateConversion(data);
                } catch (IOException ioException) {
                    throw new DateLogicalTypeDeserializationException(
                            "Failed to deserialize with manual date conversion", ioException);
                }
            }
            throw e;
        }
    }

    private boolean isDateConversionError(Throwable cause) {
        while (cause != null) {
            if (cause instanceof ClassCastException castException) {
                String msg = castException.getMessage();
                if (msg != null && msg.contains("Integer") && msg.contains("LocalDate")) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    private StrikeOffPartnerObjectionsProcessed deserializeWithManualDateConversion(byte[] data)
            throws IOException {
        // Get the schema from the generated class
        Schema schema = StrikeOffPartnerObjectionsProcessed.getClassSchema();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        StrikeOffPartnerObjectionsProcessed result = new StrikeOffPartnerObjectionsProcessed();
        for (var field : genericRecord.getSchema().getFields()) {
            result.put(field.pos(), convertFieldValue(field.name(), genericRecord.get(field.name())));
        }

        return result;
    }

    private Object convertFieldValue(String fieldName, Object value) {
        if (value == null) {
            return null;
        }

        if ("initial_expiration_on".equals(fieldName) && value instanceof Integer intValue) {
            return LocalDate.ofEpochDay(intValue);
        }

        if ("event_type".equals(fieldName) && value instanceof GenericData.EnumSymbol enumSymbol) {
            return ProcessedEventType.valueOf(enumSymbol.toString());
        }

        if ("success_failure_indicator".equals(fieldName) && value instanceof GenericData.EnumSymbol enumSymbol) {
            return SuccessFailureIndicator.valueOf(enumSymbol.toString());
        }

        return value;
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static final class DateLogicalTypeDeserializationException extends RuntimeException {

        private DateLogicalTypeDeserializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

