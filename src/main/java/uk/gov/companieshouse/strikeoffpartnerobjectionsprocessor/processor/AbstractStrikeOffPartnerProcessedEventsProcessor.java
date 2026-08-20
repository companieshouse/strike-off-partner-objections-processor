package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;

public abstract class AbstractStrikeOffPartnerProcessedEventsProcessor extends AbstractStrikeOffPartnerEventsProcessor<StrikeOffPartnerObjectionsProcessed> {

    protected AbstractStrikeOffPartnerProcessedEventsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient, StrikeOffPartnerObjectionsProcessed::getStrikeOffEventId);
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
}
