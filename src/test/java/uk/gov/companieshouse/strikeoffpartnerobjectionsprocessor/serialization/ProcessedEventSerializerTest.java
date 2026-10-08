package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.serialization;

import consumer.exception.NonRetryableErrorException;
import java.time.LocalDate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.WITHDRAWAL;

class ProcessedEventSerializerTest {

    private static final String TOPIC = "strike-off-partner-objections-processed";
    private ProcessedEventSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new ProcessedEventSerializer();
    }


    @Test
    void serialize_whenNullRecord_throwsNonRetryableErrorException() {
        NonRetryableErrorException ex = assertThrows(
                NonRetryableErrorException.class,
                () -> serializer.serialize(TOPIC, null)
        );

        assertTrue(ex.getMessage().contains("Failed to serialize StrikeOffPartnerObjectionsProcessed"));
    }

    @Test
    void serialize_whenValidRecordWithDate_returnsBytes() {
        StrikeOffPartnerObjectionsProcessed strikeOffPartnerObjectionsProcessed = buildValidRecord();
        byte[] result = serializer.serialize(TOPIC, strikeOffPartnerObjectionsProcessed);
        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    void serialize_whenValidRecordWithDateNull_returnBytes() {
        StrikeOffPartnerObjectionsProcessed strikeOffPartnerObjectionsProcessed = buildValidRecord();
        strikeOffPartnerObjectionsProcessed.setInitialExpirationOn(null);
        strikeOffPartnerObjectionsProcessed.setEventType(WITHDRAWAL);
        byte[] result = serializer.serialize(TOPIC, strikeOffPartnerObjectionsProcessed);
        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    private StrikeOffPartnerObjectionsProcessed buildValidRecord() {
        StrikeOffPartnerObjectionsProcessed strikeOffPartnerObjectionsProcessed = new StrikeOffPartnerObjectionsProcessed();
        strikeOffPartnerObjectionsProcessed.setCompanyNumber("12345678");
        strikeOffPartnerObjectionsProcessed.setStrikeOffEventId("event-id-123");
        strikeOffPartnerObjectionsProcessed.setEventType(OBJECTION);
        strikeOffPartnerObjectionsProcessed.setSuccessFailureIndicator(SuccessFailureIndicator.SUCCESS);
        strikeOffPartnerObjectionsProcessed.setErrorMessage(null);
        // Avro logical date expected backing type in your current runtime path.
        strikeOffPartnerObjectionsProcessed.setInitialExpirationOn(LocalDate.now());
        return strikeOffPartnerObjectionsProcessed;
    }
}