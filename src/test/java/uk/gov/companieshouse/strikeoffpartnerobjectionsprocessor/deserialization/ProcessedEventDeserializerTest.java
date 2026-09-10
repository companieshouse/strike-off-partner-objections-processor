package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.deserialization;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
class ProcessedEventDeserializerTest {
    private static final String TOPIC = "processed-topic";
    @Test
    void deserialize_whenDataIsNull_returnsNull() {
        try (ProcessedEventDeserializer deserializer = new ProcessedEventDeserializer()) {
            StrikeOffPartnerObjectionsProcessed result = deserializer.deserialize(TOPIC, null);
            assertNull(result);
        }
    }
    @Test
    void deserialize_validPayload_convertsLogicalDateAndEnums() throws IOException {
        try (ProcessedEventDeserializer deserializer = new ProcessedEventDeserializer()) {
            byte[] payload = serialisedProcessedRecord();
            StrikeOffPartnerObjectionsProcessed result = deserializer.deserialize(TOPIC, payload);
            assertEquals("strike-001", result.getStrikeOffEventId());
            assertEquals("12345678", result.getCompanyNumber());
            assertEquals(ProcessedEventType.OBJECTION, result.getEventType());
            assertEquals(SuccessFailureIndicator.SUCCESS, result.getSuccessFailureIndicator());
            assertEquals(LocalDate.ofEpochDay(20392), result.getInitialExpirationOn());
        }
    }
    @Test
    void deserialize_invalidPayload_throwsDeserializationException() {
        try (ProcessedEventDeserializer deserializer = new ProcessedEventDeserializer()) {
            byte[] invalidPayload = new byte[]{1, 2, 3};
            RuntimeException result = assertThrows(RuntimeException.class,
                    () -> deserializer.deserialize(TOPIC, invalidPayload));
            assertEquals("Failed to deserialize StrikeOffPartnerObjectionsProcessed", result.getMessage());
        }
    }

    private static byte[] serialisedProcessedRecord() throws IOException {
        final int epochDay = 20392;
        Schema schema = StrikeOffPartnerObjectionsProcessed.getClassSchema();
        GenericRecord processedRecord = new GenericData.Record(schema);
        processedRecord.put("strike_off_event_id", "strike-001");
        processedRecord.put("company_number", "12345678");
        processedRecord.put("event_type",
                new GenericData.EnumSymbol(resolveEnumFieldSchema(schema, "event_type"), "OBJECTION"));
        processedRecord.put("success_failure_indicator", new GenericData.EnumSymbol(
                resolveEnumFieldSchema(schema, "success_failure_indicator"), "SUCCESS"));
        processedRecord.put("initial_expiration_on", epochDay);
        processedRecord.put("error_message", null);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        writer.write(processedRecord, encoder);
        encoder.flush();
        return output.toByteArray();
    }
    private static Schema resolveEnumFieldSchema(Schema recordSchema, String fieldName) {
        Schema fieldSchema = recordSchema.getField(fieldName).schema();
        if (fieldSchema.getType() == Schema.Type.ENUM) {
            return fieldSchema;
        }
        if (fieldSchema.getType() != Schema.Type.UNION) {
            throw new IllegalStateException("Unsupported schema type for field: " + fieldName);
        }
        return fieldSchema.getTypes().stream()
                .filter(type -> type.getType() == Schema.Type.ENUM)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Expected enum for field: " + fieldName));
    }
}

