package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.List;

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
