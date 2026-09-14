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
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcOutcomeCallbackClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcOutcomeCallbackRequest;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.CALLBACK_KIND_WITHDRAWAL;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.WITHDRAWALS;

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

    private final HmrcOutcomeCallbackClient hmrcOutcomeCallbackClient;

    protected ProcessedWithdrawalsProcessor(
            InternalApiClient internalApiClient,
            HmrcOutcomeCallbackClient hmrcOutcomeCallbackClient) {
        super(internalApiClient,
                StrikeOffPartnerObjectionsProcessed::getStrikeOffEventId,
                StrikeOffPartnerObjectionsProcessed::getCompanyNumber,
                StrikeOffPartnerObjectionsProcessed::getStrikeOffEventId);
        this.hmrcOutcomeCallbackClient = hmrcOutcomeCallbackClient;
    }

    @Override
    protected boolean eventTypeSupported(StrikeOffPartnerObjectionsProcessed message) {
        return message.getEventType() == ProcessedEventType.WITHDRAWAL;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
        LOG.info("Processing withdrawal outcome event with ID: " + message.getStrikeOffEventId());
        WithdrawAllObjectionsResponse withdrawal = getOrSkipNotFound(
                () -> getWithdrawalDetails(message),
                () -> new DuplicateRecordException("Skipping processed withdrawal event because withdrawal was not found: strikeOffEventId="
                        + message.getStrikeOffEventId()
                        + ", companyNumber=" + message.getCompanyNumber()));

        UpdateWithdrawalStatusRequest request = buildUpdateRequest(message);
        WithdrawalProcessingStatus currentStatus = withdrawal.getProcessingStatus();
        WithdrawalProcessingStatus targetStatus = request.getProcessingStatus();

        if (isConflictingTerminalState(currentStatus, targetStatus)) {
            throw new DuplicateRecordException("Duplicate/complete Withdrawal skipped: strikeOffEventId="
                    + message.getStrikeOffEventId()
                    + ", withdrawalId=" + withdrawal.getWithdrawalId()
                    + ", status=" + currentStatus.getValue());
        }

        if (!isDuplicateRecord(currentStatus.getValue(), targetStatus.getValue())) {
            updateWithdrawalStatus(message, request);
            LOG.info("Updated withdrawal status to " + targetStatus + " for withdrawalId=" + withdrawal.getWithdrawalId());
        } else {
            LOG.info("Withdrawal already in target status " + targetStatus
                    + " for withdrawalId=" + withdrawal.getWithdrawalId()
                    + ". Proceeding with callback notification.");
        }

        submitOutcomeCallback(message, withdrawal.getWithdrawalId(), targetStatus);
    }

    private UpdateWithdrawalStatusRequest buildUpdateRequest(StrikeOffPartnerObjectionsProcessed message) {
        UpdateWithdrawalStatusRequest request = new UpdateWithdrawalStatusRequest();
        if (message.getSuccessFailureIndicator() == SuccessFailureIndicator.SUCCESS) {
            request.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED);
            return request;
        }

        request.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED);
        request.setFailureReason(message.getErrorMessage());
        return request;
    }

    private boolean isConflictingTerminalState(
            WithdrawalProcessingStatus currentStatus,
            WithdrawalProcessingStatus requestedStatus) {
        return currentStatus == WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED
                && requestedStatus == WithdrawalProcessingStatus.WITHDRAWAL_REJECTED
                || currentStatus == WithdrawalProcessingStatus.WITHDRAWAL_REJECTED
                && requestedStatus == WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED;
    }

    private void submitOutcomeCallback(
            StrikeOffPartnerObjectionsProcessed message,
            String resourceId,
            WithdrawalProcessingStatus status) {
        HmrcOutcomeCallbackRequest callbackRequest = new HmrcOutcomeCallbackRequest(
                CALLBACK_KIND_WITHDRAWAL,
                resourceId,
                message.getCompanyNumber(),
                buildResourceUri(message, WITHDRAWALS),
                status.getValue());
        try {
            hmrcOutcomeCallbackClient.submitOutcomeCallback(callbackRequest);
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
    }

    @Override
    protected void validate(StrikeOffPartnerObjectionsProcessed message) {
        validateProcessedEvent(message);
    }
}
