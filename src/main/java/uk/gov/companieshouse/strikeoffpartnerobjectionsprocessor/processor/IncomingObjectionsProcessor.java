package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.ProcessorLogContext;

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
    protected void doProcess(StrikeOffPartnerObjections message, ProcessorLogContext logContext) {
        var objection = getObjectionDetails(message, logContext);
        ProcessorLogContext objectionContext = logContext.withObjectionId(objection.getObjectionId());

        // Idempotent check: if already processing, skip
        if (isDuplicateRecord(
                objection.getProcessingStatus().getValue(),
                ObjectionProcessingStatus.OBJECTION_PROCESSING.getValue())) {
            throw new DuplicateRecordException("Duplicate/complete Objection skipped: strikeOffEventId=" + message.getStrikeOffEventId()
                    + ", objectionId=" + objection.getObjectionId()
                    + ", status=" + objection.getProcessingStatus().getValue(),
                    objectionContext.withStatus(objection.getProcessingStatus().getValue()));
        }

        // Update status to objection-processing
        updateObjectionStatus(message, ObjectionProcessingStatus.OBJECTION_PROCESSING, objectionContext);
        submitToChips(objection, message, chipsPartnerObjectionsSubmissionClient, objectionContext);
    }

    @Override
    protected void validate(StrikeOffPartnerObjections message) {
        validateIncomingEvent(message);
    }
}
