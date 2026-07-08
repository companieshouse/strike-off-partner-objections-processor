package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.List;

/**
 * Dispatches {@link StrikeOffPartnerObjections} messages to the first processor
 * that declares support for the message event type.
 *
 * <p>This component coordinates available {@link AbstractStrikeOffPartnerObjectionsProcessor}
 * implementations and routes each message to a single matching processor.
 *
 * <p>If no processor supports the message event type, an
 * {@link uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException}
 * is thrown.
 */
@Component
public class ProcessorDispatcher {
    private final List<AbstractStrikeOffPartnerObjectionsProcessor> processors;

    public ProcessorDispatcher(List<AbstractStrikeOffPartnerObjectionsProcessor> processors) {
        this.processors = processors;
    }

    public void dispatch(StrikeOffPartnerObjections message) {
        processors.stream()
                .filter(p -> p.supports(message.getEventType()))
                .findFirst()
                .orElseThrow(() -> new InvalidStrikeOffMessageException("No processor for " + message.getEventType()))
                .process(message);
    }

}
