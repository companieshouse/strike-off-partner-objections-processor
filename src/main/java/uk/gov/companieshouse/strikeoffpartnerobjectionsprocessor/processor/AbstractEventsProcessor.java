package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.function.Function;

import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.FAILURE;
import static uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator.SUCCESS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.INTERNAL_COMPANY_URI;

/**
 * Defines the common processing flow and infrastructure behaviour for strike-off events.
 *
 * @param <T> the Avro message type handled by the processor
 */
public abstract class AbstractEventsProcessor<T extends SpecificRecordBase> {

    protected static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    protected final InternalApiClient internalApiClient;
    private final Function<T, String> eventIdGetter;
    private final Function<T, String> companyNumberGetter;
    private final Function<T, String> strikeOffEventIdGetter;

    protected AbstractEventsProcessor(InternalApiClient internalApiClient,
                                      Function<T, String> eventIdGetter,
                                      Function<T, String> companyNumberGetter,
                                      Function<T, String> strikeOffEventIdGetter) {
        this.internalApiClient = internalApiClient;
        this.eventIdGetter = eventIdGetter;
        this.companyNumberGetter = companyNumberGetter;
        this.strikeOffEventIdGetter = strikeOffEventIdGetter;
    }

    public final void process(T message) {
        validate(message);
        if (!eventTypeSupported(message)) {
            throw new InvalidStrikeOffMessageException("unsupported event type");
        }
        doProcess(message);
    }

    protected abstract void validate(T message);

    protected abstract boolean eventTypeSupported(T message);

    protected abstract void doProcess(T message) throws DuplicateRecordException;

    protected final String buildResourceUri(T message, String resourceSegment) {
        return String.format("/company/%s/%s/%s",
                companyNumberGetter.apply(message), resourceSegment, strikeOffEventIdGetter.apply(message));
    }

    protected final String buildInternalStatusUri(T message, String resourceSegment, String statusSegment) {
        return String.format(INTERNAL_COMPANY_URI + "%s/%s/%s/%s",
                companyNumberGetter.apply(message), resourceSegment, strikeOffEventIdGetter.apply(message), statusSegment);
    }

    protected final String getEventId(T message) {
        return eventIdGetter.apply(message);
    }

    protected final void validateIncomingEvent(StrikeOffPartnerObjections message) {
        if (message == null) {
            throw new InvalidStrikeOffMessageException("Missing message");
        }
        validateNotBlank(String.valueOf(message.getEventType()), "eventType");
        validateNotBlank(message.getEventId(), "eventId");
        validateNotBlank(message.getPartnerOrganisation(), "PartnerOrganisation");
        validateNotBlank(message.getCompanyNumber(), "Company number");
        validateNotBlank(message.getStrikeOffEventId(), "StrikeOffEventId");
    }

    protected final void validateProcessedEvent(StrikeOffPartnerObjectionsProcessed message) {
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

    void validateNotBlank(String value, String fieldName) {
        if (value == null || value.equals("null") || value.isBlank()) {
            throw new InvalidStrikeOffMessageException("Missing " + fieldName);
        }
    }

    /**
     * Maps a checked API exception to a runtime exception.
     * <p>Wrapped in a plain {@link RuntimeException} so that transient/technical API failures
     * remain retryable (i.e. NOT excluded by the Kafka retry configuration).
     * @param message from which we can resolve the event id for context
     * @param exception the underlying API exception
     * @return a runtime exception to propagate
     **/
    protected final RuntimeException mapApiException(T message, Exception exception) {
        String eventId = getEventId(message);
        if (exception instanceof URIValidationException) {
            return new InvalidStrikeOffMessageException(
                    "Non-retryable URI validation error for eventId=" + eventId, exception);
        }

        if (exception instanceof ChipsSubmissionException chipsSubmissionException) {
            return classifyStatusCodeException(
                    eventId, chipsSubmissionException.getStatusCode(), chipsSubmissionException);
        }

        if (exception instanceof ApiErrorResponseException apiException) {
            return classifyStatusCodeException(eventId, apiException.getStatusCode(), apiException);
        }

        // Unknown/technical failure => retry
        return new RuntimeException("Retryable error for eventId=" + eventId, exception);
    }

    protected final boolean isDuplicateRecord(String status, String processedStatus) {
        return processedStatus != null && processedStatus.equalsIgnoreCase(status);
    }

    private RuntimeException classifyStatusCodeException(String eventId, int status, Exception ex) {
        LOG.error("API call failed: status=" + status + ", eventId=" + eventId, ex);
        if (status >= 400 && status < 500 && status != 429) {
            return new InvalidStrikeOffMessageException(
                    "Non-retryable API error (status=" + status + ") for eventId=" + eventId, ex);
        }
        return new RuntimeException(
                "Retryable API error (status=" + status + ") for eventId=" + eventId, ex);
    }
}
