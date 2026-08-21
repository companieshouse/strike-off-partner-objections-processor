package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.INTERNAL_COMPANY_URI;

public abstract class AbstractStrikeOffPartnerProcessedEventsProcessor extends AbstractStrikeOffPartnerEventsProcessor<StrikeOffPartnerObjectionsProcessed> {

    protected AbstractStrikeOffPartnerProcessedEventsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient, StrikeOffPartnerObjectionsProcessed::getStrikeOffEventId);
    }

    @Override
    public final void process(StrikeOffPartnerObjectionsProcessed message) {
        validate(message);

        if (!supports(message.getEventType())) {
            throw new InvalidStrikeOffMessageException("unsupported event type");
        }
        doProcess(message);
    }

    @Override
    protected void validate(StrikeOffPartnerObjectionsProcessed message) {
        if (message == null) {
            throw new InvalidStrikeOffMessageException("Missing message");
        }
        validateNotBlank(message.getStrikeOffEventId(), "StrikeOffEventId");
        validateNotBlank(String.valueOf(message.getEventType()), "processedEventType");
        validateNotBlank(String.valueOf(message.getSuccessFailureIndicator()), "SuccessFailureIndicator");
        validateNotBlank(message.getCompanyNumber(), "Company number");
        if (message.getSuccessFailureIndicator() == FAILURE) {
            validateNotBlank(message.getErrorMessage(), "ErrorMessage");
        }
        if (message.getSuccessFailureIndicator() == SUCCESS) {
            validateNotBlank(String.valueOf(message.getInitialExpirationOn()), "InitialExpirationOn");
        }
    }

    @Override
    protected abstract boolean supports(ProcessedEventType eventType);

    @Override
    protected abstract void doProcess(StrikeOffPartnerObjectionsProcessed message) throws DuplicateRecordException;

    @Override
    protected String buildResourceUri(StrikeOffPartnerObjectionsProcessed message, String resourceSegment) {
        return String.format("/company/%s/%s/%s", message.getCompanyNumber(), resourceSegment, message.getStrikeOffEventId());
    }

    @Override
    protected String buildInternalStatusUri(StrikeOffPartnerObjectionsProcessed message, String resourceSegment, String statusSegment) {
        return String.format(INTERNAL_COMPANY_URI + "%s/%s/%s/%s",
                message.getCompanyNumber(), resourceSegment, message.getStrikeOffEventId(), statusSegment);
    }
}
