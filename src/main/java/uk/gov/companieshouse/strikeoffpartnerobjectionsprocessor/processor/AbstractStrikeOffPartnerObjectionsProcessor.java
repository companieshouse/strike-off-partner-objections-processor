package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

public abstract class AbstractStrikeOffPartnerObjectionsProcessor {
    public final void process(StrikeOffPartnerObjections message) {
        validate(message);

        if (!supports(message.getEventType())) {
            throw new InvalidStrikeOffMessageException("unsupported event type");
        }
        doProcess(message);
    }

    protected void validate(StrikeOffPartnerObjections message) {
        if (message == null || message.getEventType() == null) {
            throw new InvalidStrikeOffMessageException("Missing eventType");
        }
        if (message.getEventId() == null || message.getEventId().isBlank()) {
            throw new InvalidStrikeOffMessageException("Missing eventId");
        }
        if (message.getPartnerOrganisation() == null || message.getPartnerOrganisation().isBlank()) {
            throw new InvalidStrikeOffMessageException("Missing PartnerOrganisation");
        }
    }

    protected abstract boolean supports(EventType eventType);
    protected abstract void doProcess(StrikeOffPartnerObjections message);
}
