package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import java.util.List;

@Component
public class ProcessorDispatcher {
    private final List<AbstractStrikeOffPartnerObjectionsProcessor> processors;

    public ProcessorDispatcher(List<AbstractStrikeOffPartnerObjectionsProcessor> processors) {
        this.processors = processors;
    }

    public void dispatch(StrikeOffPartnerObjections message) {
        if (message == null || message.getEventType() == null) {
            throw new IllegalArgumentException("Missing eventType");
        }

        processors.stream()
                .filter(p -> p.supports(message.getEventType()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No processor for " + message.getEventType()))
                .process(message);
    }
}
