package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

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
            throw new DuplicateRecordException("Duplicate/complete Objection skipped: strikeOffEventId=" + message.getStrikeOffEventId()
                    + ", objectionId=" + objection.getObjectionId()
                    + ", status=" + objection.getProcessingStatus().getValue());
        }

        LOG.info("Objection details fetched: objectionId=" + objection.getObjectionId());

        // Update status to accepted or rejected
        SuccessFailureIndicator successFailureIndicator = message.getSuccessFailureIndicator();
        ObjectionProcessingStatus status = successFailureIndicator == SuccessFailureIndicator.SUCCESS ?
                ObjectionProcessingStatus.OBJECTION_ACCEPTED :
                ObjectionProcessingStatus.OBJECTION_REJECTED;
        updateObjectionStatus(message, status);
        LOG.info("Updated objection status to " + status + " for objectionId=" + objection.getObjectionId());
    }
}
