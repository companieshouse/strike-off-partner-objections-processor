package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.INTERNAL_COMPANY_URI;

public abstract class AbstractStrikeOffPartnerIncomingEventsProcessor extends AbstractStrikeOffPartnerEventsProcessor<StrikeOffPartnerObjections> {

    protected AbstractStrikeOffPartnerIncomingEventsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient, StrikeOffPartnerObjections::getEventId);
    }

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
        validateNotBlank(message.getEventId(), "eventId");
        validateNotBlank(message.getPartnerOrganisation(), "PartnerOrganisation");
        validateNotBlank(message.getCompanyNumber(), "Company number");
        validateNotBlank(message.getStrikeOffEventId(), "StrikeOffEventId");
    }

    @Override
    protected abstract boolean supports(EventType eventType);

    protected abstract void doProcess(StrikeOffPartnerObjections message) throws DuplicateRecordException;

    protected String buildResourceUri(StrikeOffPartnerObjections message, String resourceSegment) {
        return String.format("/company/%s/%s/%s", message.getCompanyNumber(), resourceSegment, message.getStrikeOffEventId());
    }

    protected String buildInternalStatusUri(StrikeOffPartnerObjections message, String resourceSegment, String statusSegment) {
        return String.format(INTERNAL_COMPANY_URI + "%s/%s/%s/%s",
                message.getCompanyNumber(), resourceSegment, message.getStrikeOffEventId(), statusSegment);
    }
}
