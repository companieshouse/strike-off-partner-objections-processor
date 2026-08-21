package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType.WITHDRAWAL;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

class AbstractEventsProcessorTest {

    private SpecificRecordBase message;
    private AbstractEventsProcessor<SpecificRecordBase> processor;
    private final AtomicBoolean supported = new AtomicBoolean(true);
    private final AtomicBoolean validated = new AtomicBoolean();
    private final AtomicBoolean processed = new AtomicBoolean();

    @BeforeEach
    void setUp() {
        message = mock(SpecificRecordBase.class);
        processor = new AbstractEventsProcessor<>(mock(InternalApiClient.class),
                value -> "evt-001",
                value -> "12345678",
                value -> "strike-001") {
            @Override
            protected void validate(SpecificRecordBase message) {
                validated.set(true);
            }

            @Override
            protected boolean eventTypeSupported(SpecificRecordBase message) {
                return supported.get();
            }

            @Override
            protected void doProcess(SpecificRecordBase message) {
                processed.set(true);
            }
        };
    }

    @Test
    void process_supportedMessage_validatesAndProcesses() {
        processor.process(message);

        assertTrue(validated.get());
        assertTrue(processed.get());
    }

    @Test
    void process_unsupportedMessage_throwsAfterValidation() {
        supported.set(false);

        InvalidStrikeOffMessageException exception = assertThrows(
                InvalidStrikeOffMessageException.class, () -> processor.process(message));

        assertEquals("unsupported event type", exception.getMessage());
        assertTrue(validated.get());
        assertFalse(processed.get());
    }

    @Test
    void buildResourceUri_usesMessageIdentifiers() {
        assertEquals("/company/12345678/objections/strike-001",
                processor.buildResourceUri(message, "objections"));
    }

    @Test
    void buildInternalStatusUri_usesMessageIdentifiers() {
        assertEquals("/internal/company/12345678/objections/strike-001/status",
                processor.buildInternalStatusUri(message, "objections", "status"));
    }

    @Test
    void getEventId_returnsIdentifierFromConfiguredGetter() {
        assertEquals("evt-001", processor.getEventId(message));
    }

    @Test
    void validateIncomingEvent_validMessage_doesNotThrow() {
        assertDoesNotThrow(() -> processor.validateIncomingEvent(validIncomingMessage()));
    }

    @Test
    void validateIncomingEvent_nullMessage_throwsInvalidMessage() {
        assertMissingField(() -> processor.validateIncomingEvent(null), "message");
    }

    @ParameterizedTest
    @MethodSource("invalidIncomingFields")
    void validateIncomingEvent_invalidField_throwsInvalidMessage(
            Consumer<StrikeOffPartnerObjections> fieldSetter, String expectedField) {
        StrikeOffPartnerObjections incomingMessage = validIncomingMessage();
        fieldSetter.accept(incomingMessage);

        assertMissingField(() -> processor.validateIncomingEvent(incomingMessage), expectedField);
    }

    @Test
    void validateProcessedEvent_successMessage_doesNotThrow() {
        assertDoesNotThrow(() -> processor.validateProcessedEvent(validProcessedMessage(true)));
    }

    @Test
    void validateProcessedEvent_failureMessage_doesNotThrow() {
        assertDoesNotThrow(() -> processor.validateProcessedEvent(validProcessedMessage(false)));
    }

    @Test
    void validateProcessedEvent_nullMessage_throwsInvalidMessage() {
        assertMissingField(() -> processor.validateProcessedEvent(null), "message");
    }

    @ParameterizedTest
    @MethodSource("invalidProcessedFields")
    void validateProcessedEvent_invalidField_throwsInvalidMessage(
            Consumer<StrikeOffPartnerObjectionsProcessed> fieldSetter, String expectedField) {
        StrikeOffPartnerObjectionsProcessed processedMessage = validProcessedMessage(true);
        fieldSetter.accept(processedMessage);

        assertMissingField(() -> processor.validateProcessedEvent(processedMessage), expectedField);
    }

    @Test
    void validateProcessedEvent_failureWithoutErrorMessage_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed processedMessage = validProcessedMessage(false);
        processedMessage.setErrorMessage(null);

        assertMissingField(() -> processor.validateProcessedEvent(processedMessage), "ErrorMessage");
    }

    @Test
    void validateProcessedEvent_successWithoutExpirationDate_throwsInvalidMessage() {
        StrikeOffPartnerObjectionsProcessed processedMessage = validProcessedMessage(true);
        processedMessage.setInitialExpirationOn(null);

        assertMissingField(() -> processor.validateProcessedEvent(processedMessage), "InitialExpirationOn");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "null"})
    void validateNotBlank_invalidValue_throwsInvalidMessage(String value) {
        assertMissingField(() -> processor.validateNotBlank(value, "field"), "field");
    }

    @Test
    void validateNotBlank_nullValue_throwsInvalidMessage() {
        assertMissingField(() -> processor.validateNotBlank(null, "field"), "field");
    }

    @Test
    void mapApiException_uriValidationException_isNonRetryableAndRetainsCause() {
        URIValidationException cause = mock(URIValidationException.class);

        RuntimeException result = processor.mapApiException(message, cause);

        assertInstanceOf(InvalidStrikeOffMessageException.class, result);
        assertTrue(result.getMessage().contains("Non-retryable URI validation error"));
        assertTrue(result.getMessage().contains("evt-001"));
        assertSame(cause, result.getCause());
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 422, 499})
    void mapApiException_clientError_isNonRetryable(int status) {
        ApiErrorResponseException cause = apiException(status);

        RuntimeException result = processor.mapApiException(message, cause);

        assertInstanceOf(InvalidStrikeOffMessageException.class, result);
        assertTrue(result.getMessage().contains("Non-retryable API error (status=" + status + ")"));
        assertSame(cause, result.getCause());
    }

    @ParameterizedTest
    @ValueSource(ints = {399, 429, 500, 502, 503})
    void mapApiException_retryableApiStatus_returnsRuntimeException(int status) {
        ApiErrorResponseException cause = apiException(status);

        RuntimeException result = processor.mapApiException(message, cause);

        assertFalse(result instanceof InvalidStrikeOffMessageException);
        assertTrue(result.getMessage().contains("Retryable API error (status=" + status + ")"));
        assertSame(cause, result.getCause());
    }

    @Test
    void mapApiException_unknownException_isRetryable() {
        IllegalStateException cause = new IllegalStateException("boom");

        RuntimeException result = processor.mapApiException(message, cause);

        assertFalse(result instanceof InvalidStrikeOffMessageException);
        assertEquals("Retryable error for eventId=evt-001", result.getMessage());
        assertSame(cause, result.getCause());
    }

    @Test
    void isDuplicateRecord_matchingStatus_returnsTrueIgnoringCase() {
        assertTrue(processor.isDuplicateRecord("processed", "PROCESSED"));
    }

    @Test
    void isDuplicateRecord_differentStatus_returnsFalse() {
        assertFalse(processor.isDuplicateRecord("PROCESSED", "PENDING"));
    }

    @Test
    void isDuplicateRecord_nullProcessedStatus_returnsFalse() {
        assertFalse(processor.isDuplicateRecord("PROCESSED", null));
    }

    private static Stream<Arguments> invalidIncomingFields() {
        return Stream.of(
                Arguments.of((Consumer<StrikeOffPartnerObjections>) value -> value.setEventType(null), "eventType"),
                Arguments.of((Consumer<StrikeOffPartnerObjections>) value -> value.setEventId(null), "eventId"),
                Arguments.of((Consumer<StrikeOffPartnerObjections>) value -> value.setPartnerOrganisation(" "), "PartnerOrganisation"),
                Arguments.of((Consumer<StrikeOffPartnerObjections>) value -> value.setCompanyNumber(null), "Company number"),
                Arguments.of((Consumer<StrikeOffPartnerObjections>) value -> value.setStrikeOffEventId(" "), "StrikeOffEventId"));
    }

    private static Stream<Arguments> invalidProcessedFields() {
        return Stream.of(
                Arguments.of((Consumer<StrikeOffPartnerObjectionsProcessed>) value -> value.setStrikeOffEventId(null),
                        "StrikeOffEventId"),
                Arguments.of((Consumer<StrikeOffPartnerObjectionsProcessed>) value -> value.setEventType(null),
                        "processedEventType"),
                Arguments.of((Consumer<StrikeOffPartnerObjectionsProcessed>) value -> value.setSuccessFailureIndicator(null),
                        "SuccessFailureIndicator"),
                Arguments.of((Consumer<StrikeOffPartnerObjectionsProcessed>) value -> value.setCompanyNumber(" "),
                        "Company number"));
    }

    private static StrikeOffPartnerObjections validIncomingMessage() {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setCompanyNumber("12345678")
                .setEventType(OBJECTION)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();
    }

    private static StrikeOffPartnerObjectionsProcessed validProcessedMessage(boolean succeeded) {
        return StrikeOffPartnerObjectionsProcessed.newBuilder()
                .setEventType(WITHDRAWAL)
                .setInitialExpirationOn(succeeded ? LocalDate.parse("2024-12-31") : null)
                .setCompanyNumber("12345678")
                .setSuccessFailureIndicator(succeeded ? SUCCESS : FAILURE)
                .setErrorMessage(succeeded ? null : "Processing failed")
                .setStrikeOffEventId("strike-001")
                .build();
    }

    private static ApiErrorResponseException apiException(int status) {
        ApiErrorResponseException exception = mock(ApiErrorResponseException.class);
        when(exception.getStatusCode()).thenReturn(status);
        return exception;
    }

    private static void assertMissingField(Runnable action, String expectedField) {
        InvalidStrikeOffMessageException exception =
                assertThrows(InvalidStrikeOffMessageException.class, action::run);
        assertTrue(exception.getMessage().contains(expectedField));
    }

}
