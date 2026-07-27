package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerObjectionsProcessorConstants.APPLICATION_NAMESPACE;

/**
 * Processor for strike-off partner objection events.
 *
 * <p>This implementation handles only {@link EventType#OBJECTION} messages and
 * performs objection-specific processing after base validation is completed in
 * {@link AbstractStrikeOffPartnerEventsProcessor#process(StrikeOffPartnerObjections)}.
 */
@Component
public class StrikeOffPartnerObjectionsProcessor extends AbstractStrikeOffPartnerEventsProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);
    private static final String RESOURCE_SEGMENT = "strike-off-partner-objections";

    protected StrikeOffPartnerObjectionsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
    }

    @Override
    protected boolean supports(EventType eventType) {
        return eventType == EventType.OBJECTION;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjections message) {
        LOG.info("Processing objection event with ID: " + message.getEventId());
        var objection = getObjectionDetails(message);

        // Idempotent check: if already processing, skip
        if (isDuplicateRecord(
                objection.getProcessingStatus().getValue(),
                ObjectionProcessingStatus.OBJECTION_PROCESSING.getValue())) {
            throw new DuplicateRecordException("Duplicate/complete Objection skipped: strikeOffEventId=" + objection.getObjectionId()
                    + ", status=" + objection.getProcessingStatus().getValue());
        }

        LOG.info("Objection details fetched: objectionId=" + objection.getObjectionId());

        // Update status to objection-processing
        updateObjectionStatus(message);
        LOG.info("Updated objection status to OBJECTION_PROCESSING for objectionId=" + objection.getObjectionId());
    }

    private BaseObjectionResponse getObjectionDetails(StrikeOffPartnerObjections message) {
        String uri = buildResourceUri(message, RESOURCE_SEGMENT);
        try {
            var response = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getObjection(uri)
                    .execute();
            LOG.info("Fetched objection for objectionId=" + response.getData().getObjectionId()
                    + ", status=" + response.getStatusCode());
            return response.getData();
        } catch (Exception e) {
            throw mapApiException(message.getEventId(), e);
        }
    }

    private void updateObjectionStatus(StrikeOffPartnerObjections message) {
        String uri = buildResourceUri(message, RESOURCE_SEGMENT);
        try {
            UpdateObjectionStatusRequest request = new UpdateObjectionStatusRequest();
            request.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_PROCESSING);

            internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateObjectionStatus(uri, request)
                    .execute();
            LOG.info("Successfully updated objection status to "
                    + ObjectionProcessingStatus.OBJECTION_PROCESSING
                    + " for eventId=" + message.getEventId());
        } catch (Exception e) {
            throw mapApiException(message.getEventId(), e);
        }
    }
}
