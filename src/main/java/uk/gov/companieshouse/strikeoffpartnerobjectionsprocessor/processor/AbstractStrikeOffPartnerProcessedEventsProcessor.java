package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.INTERNAL_COMPANY_URI;

/**
 * Base processor for {@link StrikeOffPartnerObjectionsProcessed} events using the template method pattern.
 *
 * <p>The {@link #process(StrikeOffPartnerObjectionsProcessed)} method defines a fixed flow:
 * <ol>
 *   <li>Validate mandatory fields on the incoming message.</li>
 *   <li>Check whether this processor supports the message {@link ProcessedEventType}.</li>
 *   <li>Delegate business handling to {@link #doProcess(StrikeOffPartnerObjectionsProcessed)}.</li>
 * </ol>
 *
 * <p>Subclasses must implement:
 * <ul>
 *   <li>{@link #supports(ProcessedEventType)} to declare which event type(s) they handle.</li>
 *   <li>{@link #doProcess(StrikeOffPartnerObjectionsProcessed)} to execute event-specific processing.</li>
 * </ul>
 *
 * <p>If validation fails or the event type is unsupported, an
 * {@link InvalidStrikeOffMessageException}
 * is thrown.
 */
public abstract class AbstractStrikeOffPartnerProcessedEventsProcessor extends AbstractStrikeOffPartnerEventsProcessor {

    protected AbstractStrikeOffPartnerProcessedEventsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
    }

    public final void process(StrikeOffPartnerObjectionsProcessed message) {
        validate(message);

        if (!supports(message.getEventType())) {
            throw new InvalidStrikeOffMessageException("unsupported event type");
        }
        doProcess(message);
    }

    protected void validate(StrikeOffPartnerObjectionsProcessed message) {
        if (message == null || message.getEventType() == null) {
            throw new InvalidStrikeOffMessageException("Missing eventType");
        }
        validateNotBlank(message.getStrikeOffEventId(), "StrikeOffEventId");
        validateNotBlank(String.valueOf(message.getEventType()), "EventType");
        validateNotBlank(String.valueOf(message.getSuccessFailureIndicator()), "SuccessFailureIndicator");
        if (message.getSuccessFailureIndicator() == FAILURE) {
            validateNotBlank(message.getErrorMessage(), "ErrorMessage");
        }
        validateNotBlank(String.valueOf(message.getInitialExpirationOn()), "InitialExpirationOn");
    }

    protected abstract boolean supports(ProcessedEventType eventType);

    protected abstract void doProcess(StrikeOffPartnerObjectionsProcessed message) throws DuplicateRecordException;

    /**
     * Builds a resource URI shared by concrete processors.
     * @param message         the event message
     * @param resourceSegment the resource path segment (e.g. {@code "strike-off-partner-objections"})
     * @return the constructed resource URI
     * */
    protected String buildResourceUri(StrikeOffPartnerObjectionsProcessed message, String resourceSegment) {
        return String.format("/company/%s/%s/%s", message.getCompanyNumber(), resourceSegment, message.getStrikeOffEventId());
    }

    protected String buildInternalStatusUri(StrikeOffPartnerObjectionsProcessed message, String resourceSegment, String statusSegment) {
        return String.format(INTERNAL_COMPANY_URI + "%s/%s/%s/%s",
                message.getCompanyNumber(), resourceSegment, message.getStrikeOffEventId(), statusSegment);
    }
}
