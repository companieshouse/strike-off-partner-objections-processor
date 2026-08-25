package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

/**
 * Processor for processed strike-off partner withdrawal events.
 *
 * <p>This implementation handles only {@link ProcessedEventType#WITHDRAWAL} messages and
 * updates the withdrawal status to accepted or rejected based on the processing outcome,
 * after base validation is completed in
 * {@link AbstractEventsProcessor#process(SpecificRecordBase)}.
 */
@Component
public class ProcessedWithdrawalsProcessor
        extends AbstractWithdrawalsEventsProcessor<StrikeOffPartnerObjectionsProcessed> {

    protected ProcessedWithdrawalsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient,
                StrikeOffPartnerObjectionsProcessed::getStrikeOffEventId,
                StrikeOffPartnerObjectionsProcessed::getCompanyNumber,
                StrikeOffPartnerObjectionsProcessed::getStrikeOffEventId);
    }

    @Override
    protected boolean eventTypeSupported(StrikeOffPartnerObjectionsProcessed message) {
        return message.getEventType() == ProcessedEventType.WITHDRAWAL;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
        LOG.info("Processing withdrawal outcome event with ID: " + message.getStrikeOffEventId());
        var withdrawal = getWithdrawalDetails(message);

        // Idempotent check: if already in a terminal state, skip
        if (isDuplicateRecord(withdrawal.getProcessingStatus().getValue(), WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED.getValue())
                || isDuplicateRecord(withdrawal.getProcessingStatus().getValue(), WithdrawalProcessingStatus.WITHDRAWAL_REJECTED.getValue())) {
            throw new DuplicateRecordException("Duplicate/complete Withdrawal skipped: strikeOffEventId=" + message.getStrikeOffEventId()
                    + ", withdrawalId=" + withdrawal.getWithdrawalId()
                    + ", status=" + withdrawal.getProcessingStatus().getValue());
        }

        LOG.info("Withdrawal details fetched: withdrawalId=" + withdrawal.getWithdrawalId());

        // Update status based on outcome
        WithdrawalProcessingStatus status = message.getSuccessFailureIndicator() == SuccessFailureIndicator.SUCCESS
                ? WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED
                : WithdrawalProcessingStatus.WITHDRAWAL_REJECTED;
        updateWithdrawalStatus(message, status);
        LOG.info("Updated withdrawal status to " + status + " for withdrawalId=" + withdrawal.getWithdrawalId());
    }

    @Override
    protected void validate(StrikeOffPartnerObjectionsProcessed message) {
        validateProcessedEvent(message);
    }
}

