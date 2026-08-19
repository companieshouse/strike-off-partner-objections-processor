package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.List;

/**
 * Dispatches {@link StrikeOffPartnerObjections} messages to the first processor
 * that declares support for the message event type.
 *
 * <p>This component coordinates available {@link AbstractStrikeOffPartnerIncomingEventsProcessor}
 * implementations and routes each message to a single matching processor.
 *
 * <p>This component coordinates available {@link AbstractStrikeOffPartnerProcessedEventsProcessor}
 * implementations and routes each message to a single matching processor.
 *
 * <p>If no processor supports the message event type, an
 * {@link uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException}
 * is thrown.
 */
@Component
public class ProcessorDispatcher {
    private final List<AbstractStrikeOffPartnerIncomingEventsProcessor> processors;
    private final List<AbstractStrikeOffPartnerProcessedEventsProcessor> processedEventsProcessors;

    public ProcessorDispatcher(List<AbstractStrikeOffPartnerIncomingEventsProcessor> processors, List<AbstractStrikeOffPartnerProcessedEventsProcessor> processedEventsProcessors) {
        this.processors = processors;
        this.processedEventsProcessors = processedEventsProcessors;
    }

    public void dispatch(StrikeOffPartnerObjections message) {
        processors.stream()
                .filter(p -> p.supports(message.getEventType()))
                .findFirst()
                .orElseThrow(() -> new InvalidStrikeOffMessageException("No processor for " + message.getEventType()))
                .process(message);
    }

    public void dispatch(StrikeOffPartnerObjectionsProcessed message) {
        processedEventsProcessors.stream()
                .filter(p -> p.supports(message.getEventType()))
                .findFirst()
                .orElseThrow(() -> new InvalidStrikeOffMessageException("No processor for " + message.getEventType()))
                .process(message);
    }

}
