package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.deserialization;

import consumer.deserialization.AvroDeserializer;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class DateLogicalTypeDeserializerTest {

    private static final String TOPIC = "processed-topic";

    @Test
    void deserialize_delegateSuccess_returnsDelegateValue() {
        AvroDeserializer<StrikeOffPartnerObjectionsProcessed> delegate = newDelegateSpy();
        DateLogicalTypeDeserializer deserializer = new DateLogicalTypeDeserializer(delegate);
        StrikeOffPartnerObjectionsProcessed expected = StrikeOffPartnerObjectionsProcessed.newBuilder()
                .setStrikeOffEventId("strike-001")
                .setCompanyNumber("12345678")
                .setEventType(ProcessedEventType.OBJECTION)
                .setSuccessFailureIndicator(SuccessFailureIndicator.SUCCESS)
                .setInitialExpirationOn(LocalDate.parse("2026-10-31"))
                .setErrorMessage(null)
                .build();

        doReturn(expected).when(delegate).deserialize(TOPIC, new byte[]{1});

        StrikeOffPartnerObjectionsProcessed result = deserializer.deserialize(TOPIC, new byte[]{1});

        assertSame(expected, result);
    }

    @Test
    void deserialize_nonDateError_rethrowsOriginalException() {
        AvroDeserializer<StrikeOffPartnerObjectionsProcessed> delegate = newDelegateSpy();
        DateLogicalTypeDeserializer deserializer = new DateLogicalTypeDeserializer(delegate);
        RuntimeException exception = new RuntimeException("boom");

        doThrow(exception).when(delegate).deserialize(TOPIC, new byte[]{1});

        RuntimeException result = assertThrows(RuntimeException.class,
                () -> deserializer.deserialize(TOPIC, new byte[]{1}));

        assertSame(exception, result);
    }

    @Test
    void deserialize_dateConversionFailure_fallsBackToManualConversion() throws IOException {
        AvroDeserializer<StrikeOffPartnerObjectionsProcessed> delegate = newDelegateSpy();
        DateLogicalTypeDeserializer deserializer = new DateLogicalTypeDeserializer(delegate);
        byte[] payload = serialisedProcessedRecord();
        RuntimeException exception = new RuntimeException(
                "outer",
                new ClassCastException("class java.lang.Integer cannot be cast to class java.time.LocalDate"));

        doThrow(exception).when(delegate).deserialize(TOPIC, payload);

        StrikeOffPartnerObjectionsProcessed result = deserializer.deserialize(TOPIC, payload);

        assertEquals("strike-001", result.getStrikeOffEventId());
        assertEquals("12345678", result.getCompanyNumber());
        assertEquals(ProcessedEventType.OBJECTION, result.getEventType());
        assertEquals(SuccessFailureIndicator.SUCCESS, result.getSuccessFailureIndicator());
        assertEquals(LocalDate.ofEpochDay(20392), result.getInitialExpirationOn());
    }

    @Test
    void deserialize_dateConversionFailure_whenManualConversionFails_propagatesFallbackFailure() {
        AvroDeserializer<StrikeOffPartnerObjectionsProcessed> delegate = newDelegateSpy();
        DateLogicalTypeDeserializer deserializer = new DateLogicalTypeDeserializer(delegate);
        byte[] invalidPayload = new byte[]{1, 2, 3};
        RuntimeException delegateException = new RuntimeException(
                "outer",
                new ClassCastException("class java.lang.Integer cannot be cast to class java.time.LocalDate"));

        doThrow(delegateException).when(delegate).deserialize(TOPIC, invalidPayload);

        RuntimeException result = assertThrows(RuntimeException.class,
                () -> deserializer.deserialize(TOPIC, invalidPayload));

        assertNotSame(delegateException, result);
    }

    @Test
    void configureAndClose_delegateCallsArePropagated() {
        AvroDeserializer<StrikeOffPartnerObjectionsProcessed> delegate = newDelegateSpy();
        DateLogicalTypeDeserializer deserializer = new DateLogicalTypeDeserializer(delegate);
        Map<String, Object> config = Map.of("schema.registry.url", "unused-for-test");

        deserializer.configure(config, false);
        deserializer.close();

        verify(delegate).configure(config, false);
        verify(delegate).close();
    }

    private static byte[] serialisedProcessedRecord() throws IOException {
        final int epochDay = 20392;
        Schema schema = StrikeOffPartnerObjectionsProcessed.getClassSchema();
        GenericRecord processedRecord = new GenericData.Record(schema);
        processedRecord.put("strike_off_event_id", "strike-001");
        processedRecord.put("company_number", "12345678");
        processedRecord.put("event_type", new GenericData.EnumSymbol(resolveEnumFieldSchema(schema, "event_type"), "OBJECTION"));
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

    private static AvroDeserializer<StrikeOffPartnerObjectionsProcessed> newDelegateSpy() {
        return spy(new AvroDeserializer<>(StrikeOffPartnerObjectionsProcessed.class));
    }
}

