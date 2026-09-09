package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

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

    private static final String NON_RETRYABLE_404_STATUS = "(status=404)";

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
        WithdrawAllObjectionsResponse withdrawal = getWithdrawalDetailsOrSkip(message);

        // Idempotent check: if already in a terminal state, skip
        if (isDuplicateRecord(withdrawal.getProcessingStatus().getValue(), WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED.getValue())
                || isDuplicateRecord(withdrawal.getProcessingStatus().getValue(), WithdrawalProcessingStatus.WITHDRAWAL_REJECTED.getValue())) {
            throw new DuplicateRecordException("Duplicate/complete Withdrawal skipped: strikeOffEventId=" + message.getStrikeOffEventId()
                    + ", withdrawalId=" + withdrawal.getWithdrawalId()
                    + ", status=" + withdrawal.getProcessingStatus().getValue());
        }

        LOG.info("Withdrawal details fetched: withdrawalId=" + withdrawal.getWithdrawalId());

        // Update status and carry failure reason through for failed outcomes.
        UpdateWithdrawalStatusRequest request = new UpdateWithdrawalStatusRequest();
        if (message.getSuccessFailureIndicator() == SuccessFailureIndicator.SUCCESS) {
            request.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED);
        } else {
            request.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED);
            request.setFailureReason(message.getErrorMessage());
        }
        updateWithdrawalStatus(message, request);
        LOG.info("Updated withdrawal status to " + request.getProcessingStatus()
                + " for withdrawalId=" + withdrawal.getWithdrawalId());
    }

    private WithdrawAllObjectionsResponse getWithdrawalDetailsOrSkip(StrikeOffPartnerObjectionsProcessed message) {
        try {
            return getWithdrawalDetails(message);
        } catch (InvalidStrikeOffMessageException exception) {
            if (!isNotFoundApiError(exception)) {
                throw exception;
            }
            throw new DuplicateRecordException("Skipping processed withdrawal event because withdrawal was not found: strikeOffEventId="
                    + message.getStrikeOffEventId()
                    + ", companyNumber=" + message.getCompanyNumber());
        }
    }

    private boolean isNotFoundApiError(RuntimeException exception) {
        String message = exception.getMessage();
        return message != null && message.contains(NON_RETRYABLE_404_STATUS);
    }

    @Override
    protected void validate(StrikeOffPartnerObjectionsProcessed message) {
        validateProcessedEvent(message);
    }
}

