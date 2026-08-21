package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

@ExtendWith(MockitoExtension.class)
class ProcessorDispatcherTest {

    @Mock
    private IncomingObjectionsProcessor processor;

    @Mock
    private ProcessedObjectionsProcessor processedEventsProcessor;

    private ProcessorDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new ProcessorDispatcher(List.of(processor), List.of(processedEventsProcessor));
    }

    @Test
    void dispatchIncomingObjections_delegatesToMatchingProcessor() {
        StrikeOffPartnerObjections msg = message(EventType.OBJECTION);
        when(processor.eventTypeSupported(msg)).thenReturn(true);

        dispatcher.dispatch(msg);

        verify(processor).process(msg);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void dispatchProcessedObjections_delegatesToMatchingProcessor(boolean wasSuccessful) {
        StrikeOffPartnerObjectionsProcessed msg = message(ProcessedEventType.OBJECTION, wasSuccessful);
        when(processedEventsProcessor.eventTypeSupported(msg)).thenReturn(true);

        dispatcher.dispatch(msg);

        verify(processedEventsProcessor).process(msg);
    }

    @Test
    void dispatchIncomingObjections_noMatchingProcessor_throwsIllegalArgument() {
        StrikeOffPartnerObjections msg = message(EventType.WITHDRAWAL);
        when(processor.eventTypeSupported(msg)).thenReturn(false);

        assertThrows(InvalidStrikeOffMessageException.class, () -> dispatcher.dispatch(msg));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void dispatchProcessedObjections_noMatchingProcessor_throwsIllegalArgument(boolean wasSuccessful) {
        StrikeOffPartnerObjectionsProcessed msg = message(ProcessedEventType.WITHDRAWAL, wasSuccessful);
        when(processedEventsProcessor.eventTypeSupported(msg)).thenReturn(false);

        assertThrows(InvalidStrikeOffMessageException.class, () -> dispatcher.dispatch(msg));
    }

    @Test
    void dispatchIncomingObjections_multipleProcessors_callsFirstMatch() {
        IncomingWithdrawalsProcessor noMatch = mock(IncomingWithdrawalsProcessor.class);
        IncomingObjectionsProcessor match = mock(IncomingObjectionsProcessor.class);

        ProcessorDispatcher newDispatcher = new ProcessorDispatcher(List.of(noMatch, match), List.of());
        StrikeOffPartnerObjections msg = message(EventType.OBJECTION);

        when(noMatch.eventTypeSupported(msg)).thenReturn(false);
        when(match.eventTypeSupported(msg)).thenReturn(true);

        newDispatcher.dispatch(msg);

        verify(match).process(msg);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void dispatchProcessedObjections_multipleProcessors_callsFirstMatch(boolean wasSuccessful) {
        ProcessedObjectionsProcessor noMatch = mock(ProcessedObjectionsProcessor.class);
        ProcessedObjectionsProcessor match = mock(ProcessedObjectionsProcessor.class);

        ProcessorDispatcher newDispatcher = new ProcessorDispatcher(List.of(), List.of(noMatch, match));
        StrikeOffPartnerObjectionsProcessed msg = message(ProcessedEventType.OBJECTION, wasSuccessful);

        when(noMatch.eventTypeSupported(msg)).thenReturn(false);
        when(match.eventTypeSupported(msg)).thenReturn(true);

        newDispatcher.dispatch(msg);

        verify(match).process(msg);
    }

    private StrikeOffPartnerObjections message(EventType type) {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setEventType(type)
                .setCompanyNumber("12345678")
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();
    }

    private StrikeOffPartnerObjectionsProcessed message(ProcessedEventType type, boolean wasSuccessful) {
        return StrikeOffPartnerObjectionsProcessed.newBuilder()
                .setEventType(type)
                .setCompanyNumber("12345678")
                .setSuccessFailureIndicator(wasSuccessful ? SUCCESS : FAILURE)
                .setErrorMessage(wasSuccessful ? null : "Error message")
                .setInitialExpirationOn(wasSuccessful ? LocalDate.parse("2026-07-06") : null)
                .setStrikeOffEventId("strike-001")
                .build();
    }
}
