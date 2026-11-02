package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.logging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.DataMapHolder;

import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

class StructuredKafkaLoggingAspectTest {

    private final StructuredKafkaLoggingAspect aspect = new StructuredKafkaLoggingAspect();

    @AfterEach
    void clearContext() {
        DataMapHolder.clear();
    }

    @Test
    void logKafkaMessageAddsIncomingObjectionContextAndClearsItAfterProcessing() throws Throwable {
        StrikeOffPartnerObjections event = StrikeOffPartnerObjections.newBuilder()
                .setEventId("event-123")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setCompanyNumber("12345678")
                .setEventType(EventType.OBJECTION)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-123")
                .build();
        ConsumerRecord<String, StrikeOffPartnerObjections> consumerRecord =
                new ConsumerRecord<>("objections", 2, 10L, "key", event);
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        AtomicReference<Map<String, Object>> capturedContext = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedContext.set(DataMapHolder.getLogMap());
            return "processed";
        }).when(joinPoint).proceed();

        Object result = aspect.logKafkaMessage(joinPoint, consumerRecord);

        assertEquals("processed", result);
        assertEquals("12345678", capturedContext.get().get("company_number"));
        assertEquals("strike-123", capturedContext.get().get("strike_off_event_id"));
        assertEquals("TEST_ORG", capturedContext.get().get("partner_organisation"));
        assertEquals(EventType.OBJECTION, capturedContext.get().get("event_type"));
        assertEquals("event-123", capturedContext.get().get("event_id"));
        assertTrue(DataMapHolder.getLogMap().isEmpty());
    }

    @Test
    void logKafkaMessageAddsProcessedFailureContext() throws Throwable {
        StrikeOffPartnerObjectionsProcessed event = processedEvent(FAILURE);
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord =
                new ConsumerRecord<>("processed", 1, 5L, "key", event);
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        AtomicReference<Map<String, Object>> capturedContext = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedContext.set(DataMapHolder.getLogMap());
            return null;
        }).when(joinPoint).proceed();

        aspect.logKafkaMessage(joinPoint, consumerRecord);

        assertEquals("12345678", capturedContext.get().get("company_number"));
        assertEquals("strike-123", capturedContext.get().get("strike_off_event_id"));
        assertEquals(OBJECTION, capturedContext.get().get("event_type"));
        assertEquals(FAILURE, capturedContext.get().get("success_failure_indicator"));
        assertEquals("Processing failed", capturedContext.get().get("error_message"));
        assertFalse(capturedContext.get().containsKey("initial_expiration_on"));
    }

    @Test
    void logKafkaMessageAddsProcessedSuccessContext() throws Throwable {
        StrikeOffPartnerObjectionsProcessed event = processedEvent(SUCCESS);
        ConsumerRecord<String, StrikeOffPartnerObjectionsProcessed> consumerRecord =
                new ConsumerRecord<>("processed", 1, 5L, "key", event);
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        AtomicReference<Map<String, Object>> capturedContext = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedContext.set(DataMapHolder.getLogMap());
            return null;
        }).when(joinPoint).proceed();

        aspect.logKafkaMessage(joinPoint, consumerRecord);

        assertEquals(SUCCESS, capturedContext.get().get("success_failure_indicator"));
        assertEquals(LocalDate.parse("2026-07-06"), capturedContext.get().get("initial_expiration_on"));
        assertFalse(capturedContext.get().containsKey("error_message"));
    }

    @Test
    void logKafkaMessageHandlesOtherPayloadAndClearsContextWhenProcessingFails() throws Throwable {
        ConsumerRecord<String, Object> consumerRecord = new ConsumerRecord<>("other", 3, 12L, "key", new Object());
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        AtomicReference<Map<String, Object>> capturedContext = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedContext.set(DataMapHolder.getLogMap());
            throw new IllegalStateException("processing failed");
        }).when(joinPoint).proceed();

        assertThrows(IllegalStateException.class, () -> aspect.logKafkaMessage(joinPoint, consumerRecord));

        assertFalse(capturedContext.get().containsKey("event_type"));
        assertTrue(capturedContext.get().containsKey("topic"));
        assertTrue(DataMapHolder.getLogMap().isEmpty());
        verify(joinPoint).proceed();
    }

    private StrikeOffPartnerObjectionsProcessed processedEvent(
            SuccessFailureIndicator indicator) {
        return StrikeOffPartnerObjectionsProcessed.newBuilder()
                .setCompanyNumber("12345678")
                .setEventType(OBJECTION)
                .setStrikeOffEventId("strike-123")
                .setInitialExpirationOn(indicator == SUCCESS ? LocalDate.parse("2026-07-06") : null)
                .setErrorMessage(indicator == FAILURE ? "Processing failed" : null)
                .setSuccessFailureIndicator(indicator)
                .build();
    }
}
