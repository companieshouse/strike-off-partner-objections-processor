package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

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
