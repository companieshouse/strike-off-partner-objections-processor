package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.INTERNAL_COMPANY_URI;

/**
 * Base processor for {@link StrikeOffPartnerObjectionsProcessed} events using the template method pattern.
 *
 * <p>The {@link #process(StrikeOffPartnerObjectionsProcessed)} method defines a fixed flow:
 * <ol>
 *   <li>Validate mandatory fields on the incoming message.</li>
 *   <li>Delegate business handling to {@link #doProcess(StrikeOffPartnerObjectionsProcessed)}.</li>
 * </ol>
 *
 * <p>Subclasses must implement:
 * <ul>
 *   <li>{@link #doProcess(StrikeOffPartnerObjectionsProcessed)} to execute event-specific processing.</li>
 * </ul>
 *
 * <p>If validation fails, an
 * {@link uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException}
 * is thrown.
 */
public abstract class AbstractStrikeOffProcessedOutcomeProcessor {

    private static final String OBJECTION_STATUS_URI_TEMPLATE =
            INTERNAL_COMPANY_URI + "%s/strike-off-partner-objections/%s/status";
    private static final String WITHDRAWAL_STATUS_URI_TEMPLATE =
            INTERNAL_COMPANY_URI + "%s/strike-off-partner-objections-withdrawals/%s/withdrawal-status";

    protected final InternalApiClient internalApiClient;

    protected static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    protected AbstractStrikeOffProcessedOutcomeProcessor(InternalApiClient internalApiClient) {
        this.internalApiClient = internalApiClient;
    }

    public final void process(StrikeOffPartnerObjectionsProcessed message) {
        validate(message);
        doProcess(message);
    }

    protected void validate(StrikeOffPartnerObjectionsProcessed message) {
        if (message == null) {
            throw new InvalidStrikeOffMessageException("Missing StrikeOffPartnerObjectionsProcessed message");
        }
        validateNotBlank(message.getStrikeOffEventId(), "strike_off_event_id");
        validateNotNull(message.getSuccessFailureIndicator(), "success_failure_indicator");
        validateNotNull(message.getEventType(), "event_type");
        validateNotBlank(message.getCompanyNumber(), "company_number");
    }

    private void validateNotBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new InvalidStrikeOffMessageException("Missing " + fieldName);
        }
    }

    private void validateNotNull(Object value, String fieldName) {
        if (value == null) {
            throw new InvalidStrikeOffMessageException("Missing " + fieldName);
        }
    }

    protected abstract void doProcess(StrikeOffPartnerObjectionsProcessed message);

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
            LOG.info("updateObjectionStatus failed: status=" + status
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

    protected String buildObjectionStatusUri(StrikeOffPartnerObjectionsProcessed message,
                                             String objectionIdPath) {
        return buildStatusUri(message, objectionIdPath, OBJECTION_STATUS_URI_TEMPLATE);
    }

    protected String buildWithdrawalStatusUri(StrikeOffPartnerObjectionsProcessed message, String withdrawalIdPath) {
        return buildStatusUri(message, withdrawalIdPath, WITHDRAWAL_STATUS_URI_TEMPLATE);
    }

    private String buildStatusUri(StrikeOffPartnerObjectionsProcessed message,
                                  String idPath,
                                  String uriTemplate) {
        return String.format(uriTemplate, message.getCompanyNumber(), idPath);
    }
}

