package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.INTERNAL_COMPANY_URI;

/**
 * Base processor for {@link StrikeOffPartnerObjections} events using the template method pattern.
 *
 * <p>The {@link #process(StrikeOffPartnerObjections)} method defines a fixed flow:
 * <ol>
 *   <li>Validate mandatory fields on the incoming message.</li>
 *   <li>Check whether this processor supports the message {@link EventType}.</li>
 *   <li>Delegate business handling to {@link #doProcess(StrikeOffPartnerObjections)}.</li>
 * </ol>
 *
 * <p>Subclasses must implement:
 * <ul>
 *   <li>{@link #supports(EventType)} to declare which event type(s) they handle.</li>
 *   <li>{@link #doProcess(StrikeOffPartnerObjections)} to execute event-specific processing.</li>
 * </ul>
 *
 * <p>If validation fails or the event type is unsupported, an
 * {@link uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException}
 * is thrown.
 */
public abstract class AbstractStrikeOffPartnerIncomingEventsProcessor extends AbstractStrikeOffPartnerEventsProcessor {

    protected AbstractStrikeOffPartnerIncomingEventsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
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

    /**
     * Builds a resource URI shared by concrete processors.
     * @param message         the event message
     * @param resourceSegment the resource path segment (e.g. {@code "strike-off-partner-objections"})
     * @return the constructed resource URI
     * */
    protected String buildResourceUri(StrikeOffPartnerObjections message, String resourceSegment) {
        return String.format("/company/%s/%s/%s", message.getCompanyNumber(), resourceSegment, message.getStrikeOffEventId());
    }

    protected String buildInternalStatusUri(StrikeOffPartnerObjections message, String resourceSegment, String statusSegment) {
        return String.format(INTERNAL_COMPANY_URI + "%s/%s/%s/%s",
                message.getCompanyNumber(), resourceSegment, message.getStrikeOffEventId(), statusSegment);
    }
}
