package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;

public abstract class AbstractStrikeOffPartnerEventsProcessor {

    protected final InternalApiClient internalApiClient;
    protected static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    protected AbstractStrikeOffPartnerEventsProcessor(InternalApiClient internalApiClient) {
        this.internalApiClient = internalApiClient;
    }

    protected void process(SpecificRecordBase message) {
        throw new UnsupportedOperationException("Subclasses must implement process");
    }

    protected void validate(SpecificRecordBase message) {
        throw new UnsupportedOperationException("Subclasses must implement validate");
    }

    protected boolean supports(EventType eventType) {
        throw new UnsupportedOperationException("Subclasses must implement supports");
    }

    protected void doProcess(SpecificRecordBase message) throws DuplicateRecordException {
        throw new UnsupportedOperationException("Subclasses must implement doProcess");
    }

    /**
     * Builds a resource URI shared by concrete processors.
     * @param message         the event message
     * @param resourceSegment the resource path segment (e.g. {@code "strike-off-partner-objections"})
     * @return the constructed resource URI
     * */
    protected String buildResourceUri(SpecificRecordBase message, String resourceSegment) {
        throw new UnsupportedOperationException("Subclasses must implement buildResourceUri");
    }

    protected String buildInternalStatusUri(SpecificRecordBase message, String resourceSegment, String statusSegment) {
        throw new UnsupportedOperationException("Subclasses must implement buildInternalStatusUri");
    }

    void validateNotBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new InvalidStrikeOffMessageException("Missing " + fieldName);
        }
    }

    /**
     * Maps a checked API exception to a runtime exception.
     * <p>Wrapped in a plain {@link RuntimeException} so that transient/technical API failures
     * remain retryable (i.e. NOT excluded by the Kafka retry configuration).
     * @param eventId the event id for context
     * @param ex      the underlying API exception
     * @return a runtime exception to propagate
     **/
    protected RuntimeException mapApiException(String eventId, Exception ex) {
        if (ex instanceof URIValidationException) {
            return new InvalidStrikeOffMessageException(
                    "Non-retryable URI validation error for eventId=" + eventId, ex);
        }

        if (ex instanceof ApiErrorResponseException apiEx) {
            int status = apiEx.getStatusCode();
            LOG.info("updateWithdrawalStatus failed: status=" + status
                    + ", eventId=" + eventId);
            // 4xx (except 429) => permanent/client error => do not retry
            if (status >= 400 && status < 500 && status != 429) {
                return new InvalidStrikeOffMessageException(
                        "Non-retryable API error (status=" + status + ") for eventId=" + eventId, ex);
            }
            // 5xx, 429 => transient => retry
            return new RuntimeException(
                    "Retryable API error (status=" + status + ") for eventId=" + eventId, ex);
        }

        // Unknown/technical failure => retry
        return new RuntimeException(
                "Retryable error for eventId=" + eventId, ex);
    }

    protected boolean isDuplicateRecord(String status,
                                        String processedStatus) {
        return processedStatus != null && processedStatus.equalsIgnoreCase(status);
    }
}
