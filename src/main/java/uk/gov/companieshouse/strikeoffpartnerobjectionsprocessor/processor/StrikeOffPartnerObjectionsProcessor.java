package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.CreateObjectionRequest;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsRestInterfaceClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.PostObjectionException;

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
    @Autowired
    private ChipsRestInterfaceClient chipsClient;

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

    /**
     * Post objection to CHIPS via chips-rest-interfaces.
     *
     * @param message the strike-off objection event message
     */
    public void postObjectionToChips(StrikeOffPartnerObjections message) {
        try {
            var objection = getObjectionDetails(message);
            var createRequest = getCreateObjectionRequest(objection);

            chipsClient.postObjection(createRequest);
            LOG.info("Posted objection to CHIPS for objectionId=" + objection.getObjectionId());
        } catch (Exception e) {
            LOG.error("Failed to post objection to CHIPS for eventId=" + message.getEventId(), e);
            throw new PostObjectionException("Failed to post objection to CHIPS", e);
        }
    }

    private static @NonNull CreateObjectionRequest getCreateObjectionRequest(BaseObjectionResponse objection) {
        var createRequest = new CreateObjectionRequest();
        createRequest.setSubmissionCompanyName(objection.getSubmissionCompanyName());
        createRequest.setPartnerCaseReference(objection.getPartnerCaseReference());
        createRequest.setPartnerObjectionWorkstream(objection.getPartnerObjectionWorkstream());
        createRequest.setPartnerContactEmail(objection.getPartnerContactEmail());
        createRequest.setPartnerObjectionReason(objection.getPartnerObjectionReason());
        return createRequest;
    }
}
