package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

/**
 * Processor for strike-off partner withdrawal events.
 *
 * <p>This implementation handles only {@link EventType#WITHDRAWAL} messages and
 * performs withdrawal-specific processing after base validation is completed in
 * {@link AbstractWithdrawalsEventsProcessor#process(SpecificRecordBase)}.
 */
@Component
public class IncomingWithdrawalsProcessor
        extends AbstractWithdrawalsEventsProcessor<StrikeOffPartnerObjections> {
    private final ChipsPartnerObjectionsSubmissionClient chipsPartnerObjectionsSubmissionClient;

    protected IncomingWithdrawalsProcessor(InternalApiClient internalApiClient, ChipsPartnerObjectionsSubmissionClient chipsPartnerObjectionsSubmissionClient) {
        super(internalApiClient,
                StrikeOffPartnerObjections::getEventId,
                StrikeOffPartnerObjections::getCompanyNumber,
                StrikeOffPartnerObjections::getStrikeOffEventId);
        this.chipsPartnerObjectionsSubmissionClient = chipsPartnerObjectionsSubmissionClient;
    }

    @Override
    protected boolean eventTypeSupported(StrikeOffPartnerObjections message) {
        return message.getEventType() == EventType.WITHDRAWAL;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjections message) {
        LOG.info("Processing withdrawal event with ID: " + message.getEventId());
        var withdrawalDetails = getWithdrawalDetails(message);

        LOG.info("Withdrawal details fetched: withdrawalId=" + withdrawalDetails.getWithdrawalId());

        // Idempotent check: if already processing, skip
        if (isDuplicateRecord(
                withdrawalDetails.getProcessingStatus().getValue(),
                WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING.getValue())) {
            throw new DuplicateRecordException("Duplicate/complete Withdrawal skipped: strikeOffEventId=" +  message.getStrikeOffEventId()
                    + ", withdrawalId=" + withdrawalDetails.getWithdrawalId()
                    + ", status=" + withdrawalDetails.getProcessingStatus().getValue());
        }

        LOG.info("Withdrawal details fetched: withdrawalId=" + withdrawalDetails.getWithdrawalId());

        // Verify the current status is WITHDRAWAL_REQUESTED before updating to prevent invalid transitions.
        // This is a non-retryable guard: any status other than WITHDRAWAL_PROCESSING (duplicate) or
        // WITHDRAWAL_REQUESTED (expected) represents an unexpected system state that should be investigated.
        if (withdrawalDetails.getProcessingStatus() != WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED) {
            throw new InvalidStrikeOffMessageException("Invalid status transition attempted: current status="
                    + withdrawalDetails.getProcessingStatus().getValue()
                    + ", expected=WITHDRAWAL_REQUESTED for withdrawalId=" + withdrawalDetails.getWithdrawalId());
        }

        // Update status to withdrawal-processing (SDK support pending)
        updateWithdrawalStatus(message, WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING);
        LOG.info("Updated withdrawal status to WITHDRAWAL_PROCESSING for withdrawalId=" + withdrawalDetails.getWithdrawalId());
        submitToChips(message);
    }

    @Override
    protected void validate(StrikeOffPartnerObjections message) {
        validateIncomingEvent(message);
    }

    private void submitToChips(StrikeOffPartnerObjections message) {
        try {
            chipsPartnerObjectionsSubmissionClient.submit(message);
            LOG.info("Submitted withdrawal to CHIPS endpoint for eventId=" + message.getEventId());
        } catch (Exception e) {
            LOG.info("Failed to submit withdrawal to CHIPS endpoint for eventId=" + message.getEventId());
            throw mapApiException(message, e);
        }
    }
}