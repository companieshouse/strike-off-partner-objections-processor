package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.OBJECTIONS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.STATUS;

/**
 * Processor for incoming strike-off partner objection events.
 *
 * <p>This implementation handles only {@link ProcessedEventType#OBJECTION} messages and
 * performs objection-specific processing after base validation is completed in
 * {@link AbstractStrikeOffPartnerProcessedEventsProcessor#process(StrikeOffPartnerObjectionsProcessed)}.
 */
@Component
public class StrikeOffPartnerProcessedObjectionsProcessor extends AbstractStrikeOffPartnerProcessedEventsProcessor {

    protected StrikeOffPartnerProcessedObjectionsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
    }

    @Override
    protected boolean supports(ProcessedEventType eventType) {
        return eventType == ProcessedEventType.OBJECTION;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
        LOG.info("Processing objection event with ID: " + message.getStrikeOffEventId());
        var objection = getObjectionDetails(message);

        // Idempotent check: if this has already been accepted or rejected, skip
        if (isDuplicateRecord(objection.getProcessingStatus().getValue(), ObjectionProcessingStatus.OBJECTION_ACCEPTED.getValue())
                || isDuplicateRecord(objection.getProcessingStatus().getValue(), ObjectionProcessingStatus.OBJECTION_REJECTED.getValue())) {
            throw new DuplicateRecordException("Duplicate/complete Objection skipped: strikeOffEventId=" + objection.getObjectionId()
                    + ", status=" + objection.getProcessingStatus().getValue());
        }

        LOG.info("Objection details fetched: objectionId=" + objection.getObjectionId());

        // Update status to accepted or rejected
        updateObjectionStatus(message);
        LOG.info("Updated objection status to OBJECTION_ACCEPTED or OBJECTION_REJECTED for objectionId=" + objection.getObjectionId());
    }

    private BaseObjectionResponse getObjectionDetails(StrikeOffPartnerObjectionsProcessed message) {
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
            throw mapApiException(message.getStrikeOffEventId(), e);
        }
    }

    private void updateObjectionStatus(StrikeOffPartnerObjectionsProcessed message) {
        String uri = buildInternalStatusUri(message, OBJECTIONS, STATUS);
        try {
            UpdateObjectionStatusRequest request = new UpdateObjectionStatusRequest();
            SuccessFailureIndicator successFailureIndicator = message.getSuccessFailureIndicator();
            request.setProcessingStatus(
                    successFailureIndicator == SuccessFailureIndicator.SUCCESS ?
                            ObjectionProcessingStatus.OBJECTION_ACCEPTED :
                            ObjectionProcessingStatus.OBJECTION_REJECTED);

            internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateObjectionStatus(uri, request)
                    .execute();
            LOG.info("Successfully updated objection status to "
                    + request.getProcessingStatus()
                    + " for eventId=" + message.getStrikeOffEventId());
        } catch (Exception e) {
            LOG.info("Failed to update Objection status using api url: " + uri);
            throw mapApiException(message.getStrikeOffEventId(), e);
        }
    }
}
