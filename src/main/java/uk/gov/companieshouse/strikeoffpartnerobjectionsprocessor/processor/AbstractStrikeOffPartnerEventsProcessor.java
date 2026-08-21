package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.function.Function;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.OBJECTIONS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.STATUS;

/**
 * Base processor for {@link StrikeOffPartnerObjections} and {@link StrikeOffPartnerObjectionsProcessed} events using the template method pattern.
 *
 * <p>The {@link #process(SpecificRecordBase)} method defines a fixed flow:
 * <ol>
 *   <li>Validate mandatory fields on the incoming message.</li>
 *   <li>Check whether this processor supports the message {@link EventType} or {@link ProcessedEventType}.</li>
 *   <li>Delegate business handling to {@link #doProcess(SpecificRecordBase)}.</li>
 * </ol>
 *
 * <p>Subclasses must implement:
 * <ul>
 *   <li>{@link #supports(EventType)} or {@link #supports(ProcessedEventType)} to declare which event type(s) they handle.</li>
 *   <li>{@link #doProcess(SpecificRecordBase)} to execute event-specific processing.</li>
 * </ul>
 *
 * <p>If validation fails or the event type is unsupported, an
 * {@link InvalidStrikeOffMessageException}
 * is thrown.
 */
public abstract class AbstractStrikeOffPartnerEventsProcessor<T extends SpecificRecordBase> {

    protected final InternalApiClient internalApiClient;
    protected final Function<T, String> eventIdGetter;
    protected static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    protected AbstractStrikeOffPartnerEventsProcessor(InternalApiClient internalApiClient, 
                                                      Function<T, String> eventIdGetter) {
        this.internalApiClient = internalApiClient;
        this.eventIdGetter = eventIdGetter;
    }

    protected abstract void process(T message);

    protected abstract void validate(T message);

    protected boolean supports(EventType eventType) {
        throw new UnsupportedOperationException("Subclasses must implement supports");
    }

    protected boolean supports(ProcessedEventType eventType) {
        throw new UnsupportedOperationException("Subclasses must implement supports");
    }

    protected abstract void doProcess(T message) throws DuplicateRecordException;

    /**
     * Builds a resource URI shared by concrete processors.
     * @param message         the event message
     * @param resourceSegment the resource path segment (e.g. {@code "strike-off-partner-objections"})
     * @return the constructed resource URI
     * */
    protected String buildResourceUri(T message, String resourceSegment) {
        throw new UnsupportedOperationException("Subclasses must implement buildResourceUri");
    }

    protected String buildInternalStatusUri(T message, String resourceSegment, String statusSegment) {
        throw new UnsupportedOperationException("Subclasses must implement buildInternalStatusUri");
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

    protected boolean isDuplicateRecord(String status, String processedStatus) {
        return processedStatus != null && processedStatus.equalsIgnoreCase(status);
    }

    protected final BaseObjectionResponse getObjectionDetails(T message) {
        String uri = buildResourceUri(message, OBJECTIONS);
        try {
            var response = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getObjection(uri)
                    .execute();
            LOG.info("Fetched objection for objectionId=" + response.getData().getObjectionId()
                    + ", status=" + response.getStatusCode());
            return response.getData();
        } catch (Exception e) {
            LOG.info("Failed to get objection - api url: " + uri);
            throw mapApiException(eventIdGetter.apply(message), e);
        }
    }

    protected final void updateObjectionStatus(T message, ObjectionProcessingStatus status) {
        String uri = buildInternalStatusUri(message, OBJECTIONS, STATUS);
        try {
            UpdateObjectionStatusRequest request = new UpdateObjectionStatusRequest();
            request.setProcessingStatus(status);

            internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateObjectionStatus(uri, request)
                    .execute();
            LOG.info("Successfully updated objection status to "
                    + status
                    + " for eventId=" + eventIdGetter.apply(message));
        } catch (Exception e) {
            LOG.info("Failed to update Objection status using api url: " + uri);
            throw mapApiException(eventIdGetter.apply(message), e);
        }
    }
}
