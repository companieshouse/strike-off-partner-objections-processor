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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;

@ExtendWith(MockitoExtension.class)
class ProcessorDispatcherTest {

    @Mock
    private AbstractStrikeOffPartnerIncomingEventsProcessor processor;

    @Mock
    private AbstractStrikeOffPartnerProcessedEventsProcessor processedEventsProcessor;

    private ProcessorDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new ProcessorDispatcher(List.of(processor), List.of(processedEventsProcessor));
    }

    @Test
    void dispatchIncomingObjections_delegatesToMatchingProcessor() {
        StrikeOffPartnerObjections msg = message(EventType.OBJECTION);
        when(processor.supports(EventType.OBJECTION)).thenReturn(true);

        dispatcher.dispatch(msg);

        verify(processor, times(1)).process(msg);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void dispatchProcessedObjections_delegatesToMatchingProcessor(boolean wasSuccessful) {
        StrikeOffPartnerObjectionsProcessed msg = message(ProcessedEventType.OBJECTION, wasSuccessful);
        when(processedEventsProcessor.supports(ProcessedEventType.OBJECTION)).thenReturn(true);

        dispatcher.dispatch(msg);

        verify(processedEventsProcessor, times(1)).process(msg);
    }

    @Test
    void dispatchIncomingObjections_noMatchingProcessor_throwsIllegalArgument() {
        StrikeOffPartnerObjections msg = message(EventType.WITHDRAWAL);
        when(processor.supports(EventType.WITHDRAWAL)).thenReturn(false);

        assertThrows(InvalidStrikeOffMessageException.class, () -> dispatcher.dispatch(msg));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void dispatchProcessedObjections_noMatchingProcessor_throwsIllegalArgument(boolean wasSuccessful) {
        StrikeOffPartnerObjectionsProcessed msg = message(ProcessedEventType.WITHDRAWAL, wasSuccessful);
        when(processedEventsProcessor.supports(ProcessedEventType.WITHDRAWAL)).thenReturn(false);

        assertThrows(InvalidStrikeOffMessageException.class, () -> dispatcher.dispatch(msg));
    }

    @Test
    void dispatchIncomingObjections_multipleProcessors_callsFirstMatch() {
        AbstractStrikeOffPartnerIncomingEventsProcessor noMatch = mock(AbstractStrikeOffPartnerIncomingEventsProcessor.class);
        AbstractStrikeOffPartnerIncomingEventsProcessor match   = mock(AbstractStrikeOffPartnerIncomingEventsProcessor.class);

        when(noMatch.supports(EventType.OBJECTION)).thenReturn(false);
        when(match.supports(EventType.OBJECTION)).thenReturn(true);

        ProcessorDispatcher newDispatcher = new ProcessorDispatcher(List.of(noMatch, match), List.of());
        StrikeOffPartnerObjections msg = message(EventType.OBJECTION);

        newDispatcher.dispatch(msg);

        verify(match).process(msg);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void dispatchProcessedObjections_multipleProcessors_callsFirstMatch(boolean wasSuccessful) {
        AbstractStrikeOffPartnerProcessedEventsProcessor noMatch = mock(AbstractStrikeOffPartnerProcessedEventsProcessor.class);
        AbstractStrikeOffPartnerProcessedEventsProcessor match   = mock(AbstractStrikeOffPartnerProcessedEventsProcessor.class);

        when(noMatch.supports(ProcessedEventType.OBJECTION)).thenReturn(false);
        when(match.supports(ProcessedEventType.OBJECTION)).thenReturn(true);

        ProcessorDispatcher newDispatcher = new ProcessorDispatcher(List.of(), List.of(noMatch, match));
        StrikeOffPartnerObjectionsProcessed msg = message(ProcessedEventType.OBJECTION, wasSuccessful);

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

