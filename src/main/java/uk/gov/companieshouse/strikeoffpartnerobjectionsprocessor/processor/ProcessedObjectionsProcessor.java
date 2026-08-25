package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

/**
 * Processor for processed strike-off partner objection events.
 *
 * <p>This implementation handles only {@link ProcessedEventType#OBJECTION} messages and
 * performs objection-specific processing after base validation is completed in
 * {@link AbstractObjectionsEventsProcessor#process(org.apache.avro.specific.SpecificRecordBase)}.
 */
@Component
public class ProcessedObjectionsProcessor
        extends AbstractObjectionsEventsProcessor<StrikeOffPartnerObjectionsProcessed> {

    protected ProcessedObjectionsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient,
                StrikeOffPartnerObjectionsProcessed::getStrikeOffEventId,
                StrikeOffPartnerObjectionsProcessed::getCompanyNumber,
                StrikeOffPartnerObjectionsProcessed::getStrikeOffEventId);
    }

    @Override
    protected boolean eventTypeSupported(StrikeOffPartnerObjectionsProcessed message) {
        return message.getEventType() == ProcessedEventType.OBJECTION;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
        LOG.info("Processing objection event with ID: " + message.getStrikeOffEventId());
        var objection = getObjectionDetails(message);

        // Idempotent check: if this has already been accepted or rejected, skip
        if (isDuplicateRecord(objection.getProcessingStatus().getValue(), ObjectionProcessingStatus.OBJECTION_ACCEPTED.getValue())
                || isDuplicateRecord(objection.getProcessingStatus().getValue(), ObjectionProcessingStatus.OBJECTION_REJECTED.getValue())) {
            throw new DuplicateRecordException("Duplicate/complete Objection skipped: strikeOffEventId=" + message.getStrikeOffEventId()
                    + ", objectionId=" + objection.getObjectionId()
                    + ", status=" + objection.getProcessingStatus().getValue());
        }

        LOG.info("Objection details fetched: objectionId=" + objection.getObjectionId());

        // Update status and carry outcome fields through to the PATCH request.
        SuccessFailureIndicator successFailureIndicator = message.getSuccessFailureIndicator();
        UpdateObjectionStatusRequest request = new UpdateObjectionStatusRequest();
        if (successFailureIndicator == SuccessFailureIndicator.SUCCESS) {
            request.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_ACCEPTED);
            request.setInitialExpirationOn(message.getInitialExpirationOn());
        } else {
            request.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_REJECTED);
            request.setFailureReason(message.getErrorMessage());
        }
        updateObjectionStatus(message, request);
        LOG.info("Updated objection status to " + request.getProcessingStatus()
                + " for objectionId=" + objection.getObjectionId());
    }

    @Override
    protected void validate(StrikeOffPartnerObjectionsProcessed message) {
        validateProcessedEvent(message);
    }
}
