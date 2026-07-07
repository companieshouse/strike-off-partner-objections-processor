package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.commons.lang3.StringUtils;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

public abstract class AbstractStrikeOffPartnerObjectionsProcessor {
    public final void process(StrikeOffPartnerObjections message) {
        validate(message);

        if (!supports(message.getEventType())) {
            throw new IllegalArgumentException("unsupported event type");
        }
        doProcess(message);
    }

    protected void validate(StrikeOffPartnerObjections message) {
        if (message == null || message.getEventType() == null) {
            throw new IllegalArgumentException("Missing eventType");
        }
        if (StringUtils.isBlank(message.getEventId())) {
            throw new IllegalArgumentException("Missing eventId");
        }
        if (StringUtils.isBlank(message.getPartnerOrganisation())) {
            throw new IllegalArgumentException("Missing PartnerOrganisation");
        }
    }

    protected abstract boolean supports(EventType eventType);
    protected abstract void doProcess(StrikeOffPartnerObjections message);
}
