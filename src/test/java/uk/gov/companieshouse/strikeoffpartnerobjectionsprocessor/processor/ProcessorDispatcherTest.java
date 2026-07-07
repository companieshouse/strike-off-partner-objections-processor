package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProcessorDispatcherTest {

    @Mock
    private AbstractStrikeOffPartnerObjectionsProcessor processor;

    private ProcessorDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new ProcessorDispatcher(List.of(processor));
    }

    @Test
    void dispatch_delegatesToMatchingProcessor() {
        StrikeOffPartnerObjections msg = message(EventType.OBJECTION);
        when(processor.supports(EventType.OBJECTION)).thenReturn(true);

        dispatcher.dispatch(msg);

        verify(processor, times(1)).process(msg);
    }

    @Test
    void dispatch_noMatchingProcessor_throwsIllegalArgument() {
        StrikeOffPartnerObjections msg = message(EventType.WITHDRAWAL);
        when(processor.supports(EventType.WITHDRAWAL)).thenReturn(false);

        assertThrows(IllegalArgumentException.class, () -> dispatcher.dispatch(msg));
    }

    @Test
    void dispatch_multipleProcessors_callsFirstMatch() {
        AbstractStrikeOffPartnerObjectionsProcessor noMatch = mock(AbstractStrikeOffPartnerObjectionsProcessor.class);
        AbstractStrikeOffPartnerObjectionsProcessor match   = mock(AbstractStrikeOffPartnerObjectionsProcessor.class);

        when(noMatch.supports(EventType.OBJECTION)).thenReturn(false);
        when(match.supports(EventType.OBJECTION)).thenReturn(true);

        ProcessorDispatcher newDispatcher = new ProcessorDispatcher(List.of(noMatch, match));
        StrikeOffPartnerObjections msg = message(EventType.OBJECTION);

        newDispatcher.dispatch(msg);

        verify(match).process(msg);
    }

    private StrikeOffPartnerObjections message(EventType type) {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-001")
                .setEventTime("2026-07-06T00:00:00Z")
                .setSource("test")
                .setEventType(type)
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-001")
                .build();
    }
}

