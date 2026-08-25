package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.List;

/**
 * Dispatches {@link StrikeOffPartnerObjections} and {@link StrikeOffPartnerObjectionsProcessed} messages to the first processor
 * that declares support for the message event type.
 *
 * <p>This component coordinates available {@link AbstractEventsProcessor}
 * implementations and routes each message to a single matching processor.
 *
 * <p>If no processor supports the message event type, an
 * {@link uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException}
 * is thrown.
 */
@Component
public class ProcessorDispatcher {
    private final List<AbstractEventsProcessor<StrikeOffPartnerObjections>> processors;
    private final List<AbstractEventsProcessor<StrikeOffPartnerObjectionsProcessed>> processedEventsProcessors;

    public ProcessorDispatcher(
            List<AbstractEventsProcessor<StrikeOffPartnerObjections>> processors,
            List<AbstractEventsProcessor<StrikeOffPartnerObjectionsProcessed>> processedEventsProcessors) {
        this.processors = processors;
        this.processedEventsProcessors = processedEventsProcessors;
    }

    public void dispatch(StrikeOffPartnerObjections message) {
        processors.stream()
                .filter(processor -> processor.eventTypeSupported(message))
                .findFirst()
                .orElseThrow(() -> new InvalidStrikeOffMessageException("No processor for " + message.getEventType()))
                .process(message);
    }

    public void dispatch(StrikeOffPartnerObjectionsProcessed message) {
        processedEventsProcessors.stream()
                .filter(processor -> processor.eventTypeSupported(message))
                .findFirst()
                .orElseThrow(() -> new InvalidStrikeOffMessageException("No processor for " + message.getEventType()))
                .process(message);
    }

}
