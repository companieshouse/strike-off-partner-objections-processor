package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ProcessedOutcomeDispatcherTest {

    @Test
    void dispatch_noProcessors_throwsInvalidMessage() {
        ProcessedOutcomeDispatcher dispatcher = new ProcessedOutcomeDispatcher(List.of());
        StrikeOffPartnerObjectionsProcessed message = validMessage();

        assertThrows(InvalidStrikeOffMessageException.class, () -> dispatcher.dispatch(message));
    }

    @Test
    void dispatch_invokesAllProcessors() {
        CountingProcessor processorOne = new CountingProcessor();
        CountingProcessor processorTwo = new CountingProcessor();
        ProcessedOutcomeDispatcher dispatcher = new ProcessedOutcomeDispatcher(List.of(processorOne, processorTwo));

        dispatcher.dispatch(validMessage());

        assertEquals(1, processorOne.getProcessedCount());
        assertEquals(1, processorTwo.getProcessedCount());
    }

    private StrikeOffPartnerObjectionsProcessed validMessage() {
        return new StrikeOffPartnerObjectionsProcessed(
                "strike-001",
                SuccessFailureIndicator.SUCCESS,
                null,
                EventType.OBJECTION,
                LocalDate.of(2026, 12, 31),
                "12345678"
        );
    }

    private static class CountingProcessor extends AbstractStrikeOffProcessedOutcomeProcessor {
        private int processedCount;

        CountingProcessor() {
            super(mock(InternalApiClient.class));
        }

        @Override
        protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
            processedCount++;
        }

        int getProcessedCount() {
            return processedCount;
        }
    }
}

