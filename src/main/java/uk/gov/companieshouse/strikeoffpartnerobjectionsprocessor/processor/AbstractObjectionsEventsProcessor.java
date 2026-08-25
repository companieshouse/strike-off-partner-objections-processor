package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;

import java.util.function.Function;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.OBJECTIONS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.STATUS;

/**
 * Contains behaviour shared by incoming and processed objection events.
 *
 * @param <T> the objection event message type
 */
public abstract class AbstractObjectionsEventsProcessor<T extends SpecificRecordBase>
        extends AbstractEventsProcessor<T> {

    protected AbstractObjectionsEventsProcessor(InternalApiClient internalApiClient,
                                                Function<T, String> eventIdGetter,
                                                Function<T, String> companyNumberGetter,
                                                Function<T, String> strikeOffEventIdGetter) {
        super(internalApiClient, eventIdGetter, companyNumberGetter, strikeOffEventIdGetter);
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
        } catch (Exception exception) {
            LOG.info("Failed to get objection - api url: " + uri);
            throw mapApiException(message, exception);
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
            LOG.info("Successfully updated objection status to " + status
                    + " for eventId=" + getEventId(message));
        } catch (Exception exception) {
            LOG.info("Failed to update objection status using api url: " + uri);
            throw mapApiException(message, exception);
        }
    }
}
