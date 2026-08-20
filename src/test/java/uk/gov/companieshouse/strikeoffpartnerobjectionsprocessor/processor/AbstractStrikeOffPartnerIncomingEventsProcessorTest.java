package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.stream.Stream;
import java.util.function.Consumer;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.OBJECTION;
import static uk.gov.companieshouse.strikeoff.partner.objections.EventType.WITHDRAWAL;

class AbstractStrikeOffPartnerIncomingEventsProcessorTest {

    private AbstractStrikeOffPartnerIncomingEventsProcessor processor;

    @BeforeEach
    void setUp() {
        // Minimal concrete subclass that supports OBJECTION
        processor = new AbstractStrikeOffPartnerIncomingEventsProcessor(mock(InternalApiClient.class)) {
            @Override
            protected boolean supports(EventType eventType) {
                return eventType == OBJECTION;
            }

            @Override
            protected void doProcess(StrikeOffPartnerObjections message) {
                // no-op
            }
        };
    }

    @Test
    void process_validMessage_doesNotThrow() {
        assertDoesNotThrow(() -> processor.process(validMessage()));
    }

    @Test
    void validate_nullMessage_throwsInvalidMessage() {
        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(null));
        assertTrue(ex.getMessage().contains("eventType"));
    }

    @Test
    void validate_nullEventType_throwsInvalidMessage() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setEventType(null);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("eventType"));
    }

    @Test
    void process_unsupportedEventType_throwsRuntimeException() {
        StrikeOffPartnerObjections msg = validMessage();
        msg.setEventType(WITHDRAWAL);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains("unsupported event type"));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidFields")
    void validate_invalidField_throwsInvalidMessage(Consumer<StrikeOffPartnerObjections> fieldSetter, String expectedError) {
        StrikeOffPartnerObjections msg = validMessage();
        fieldSetter.accept(msg);

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> processor.process(msg));
        assertTrue(ex.getMessage().contains(expectedError),
                "Expected error message to contain: " + expectedError);
    }

    static Stream<Arguments> provideInvalidFields() {
        return Stream.of(
                Arguments.of((Consumer<StrikeOffPartnerObjections>) msg -> msg.setEventId(null), "eventId"),
                Arguments.of((Consumer<StrikeOffPartnerObjections>) msg -> msg.setPartnerOrganisation(null), "PartnerOrganisation"),
                Arguments.of((Consumer<StrikeOffPartnerObjections>) msg -> msg.setCompanyNumber("  "), "Company number"),
                Arguments.of((Consumer<StrikeOffPartnerObjections>) msg -> msg.setStrikeOffEventId("  "), "StrikeOffEventId")
        );
    }

    StrikeOffPartnerObjections validMessage() {
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
}
