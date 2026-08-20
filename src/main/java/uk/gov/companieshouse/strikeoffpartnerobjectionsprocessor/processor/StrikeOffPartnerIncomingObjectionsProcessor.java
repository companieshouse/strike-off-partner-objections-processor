package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

/**
 * Processor for incoming strike-off partner objection events.
 *
 * <p>This implementation handles only {@link EventType#OBJECTION} messages and
 * performs objection-specific processing after base validation is completed in
 * {@link AbstractStrikeOffPartnerIncomingEventsProcessor#process(StrikeOffPartnerObjections)}.
 */
@Component
public class StrikeOffPartnerIncomingObjectionsProcessor extends AbstractStrikeOffPartnerIncomingEventsProcessor {

    protected StrikeOffPartnerIncomingObjectionsProcessor(InternalApiClient internalApiClient) {
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
        updateObjectionStatus(message, ObjectionProcessingStatus.OBJECTION_PROCESSING);
        LOG.info("Updated objection status to OBJECTION_PROCESSING for objectionId=" + objection.getObjectionId());
    }
}
