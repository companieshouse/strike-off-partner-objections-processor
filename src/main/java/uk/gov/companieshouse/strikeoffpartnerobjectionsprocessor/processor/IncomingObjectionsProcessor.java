package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

/**
 * Processor for incoming strike-off partner objection events.
 *
 * <p>This implementation handles only {@link EventType#OBJECTION} messages and
 * performs objection-specific processing after base validation is completed in
 * {@link AbstractEventsProcessor#process(SpecificRecordBase)}.
 */
@Component
public class IncomingObjectionsProcessor
        extends AbstractObjectionsEventsProcessor<StrikeOffPartnerObjections> {
    private final ChipsPartnerObjectionsSubmissionClient chipsPartnerObjectionsSubmissionClient;

    protected IncomingObjectionsProcessor(InternalApiClient internalApiClient, ChipsPartnerObjectionsSubmissionClient chipsPartnerObjectionsSubmissionClient) {
        super(internalApiClient,
                StrikeOffPartnerObjections::getEventId,
                StrikeOffPartnerObjections::getCompanyNumber,
                StrikeOffPartnerObjections::getStrikeOffEventId);
        this.chipsPartnerObjectionsSubmissionClient = chipsPartnerObjectionsSubmissionClient;
    }

    @Override
    protected boolean eventTypeSupported(StrikeOffPartnerObjections message) {
        return message.getEventType() == EventType.OBJECTION;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjections message) {
        LOG.info("Processing objection event with ID: " + message.getEventId());
        var objection = getObjectionDetails(message);

        LOG.info("Objection details fetched: objectionId=" + objection.getObjectionId());

        // Idempotent check: if already processing, skip
        if (isDuplicateRecord(
                objection.getProcessingStatus().getValue(),
                ObjectionProcessingStatus.OBJECTION_PROCESSING.getValue())) {
            throw new DuplicateRecordException("Duplicate/complete Objection skipped: strikeOffEventId=" + message.getStrikeOffEventId()
                    + ", objectionId=" + objection.getObjectionId()
                    + ", status=" + objection.getProcessingStatus().getValue());
        }

        LOG.info("Objection details fetched: objectionId=" + objection.getObjectionId());

        // Update status to objection-processing
        updateObjectionStatus(message, ObjectionProcessingStatus.OBJECTION_PROCESSING);
        LOG.info("Updated objection status to OBJECTION_PROCESSING for objectionId=" + objection.getObjectionId());
        submitToChips(message);
    }

    @Override
    protected void validate(StrikeOffPartnerObjections message) {
        validateIncomingEvent(message);
    }

    private void submitToChips(StrikeOffPartnerObjections message) {
        try {
            chipsPartnerObjectionsSubmissionClient.submit(message);
            LOG.info("Submitted objection to CHIPS endpoint for eventId=" + message.getEventId());
        } catch (Exception e) {
            LOG.info("Failed to submit objection to CHIPS endpoint for eventId=" + message.getEventId());
            throw mapApiException(message, e);
        }
    }
}
