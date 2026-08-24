package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.List;

/**
 * Dispatches {@link StrikeOffPartnerObjectionsProcessed} messages to processors
 * that handle processed outcome events.
 *
 * <p>This component coordinates available {@link AbstractStrikeOffProcessedOutcomeProcessor}
 * implementations and ensures each message is processed by all applicable processors.
 */
@Component
public class ProcessedOutcomeDispatcher {
    private final List<AbstractStrikeOffProcessedOutcomeProcessor> processors;

    public ProcessedOutcomeDispatcher(List<AbstractStrikeOffProcessedOutcomeProcessor> processors) {
        this.processors = processors;
    }

    public void dispatch(StrikeOffPartnerObjectionsProcessed message) {
        if (processors.isEmpty()) {
            throw new InvalidStrikeOffMessageException("No processors available for processed outcomes");
        }
        processors.forEach(processor -> processor.process(message));
    }
}

