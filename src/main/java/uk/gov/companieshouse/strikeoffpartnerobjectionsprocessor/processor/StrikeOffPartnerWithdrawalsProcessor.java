package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.WITHDRAWALS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.WITHDRAWAL_STATUS;

/**
 * Processor for strike-off partner withdrawal events.
 *
 * <p>This implementation handles only {@link EventType#WITHDRAWAL} messages and
 * performs withdrawal-specific processing after base validation is completed in
 * {@link AbstractStrikeOffPartnerEventsProcessor#process(StrikeOffPartnerObjections)}.
 */
@Component
public class StrikeOffPartnerWithdrawalsProcessor extends AbstractStrikeOffPartnerEventsProcessor {


    protected StrikeOffPartnerWithdrawalsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
    }

    @Override
    protected boolean supports(EventType eventType) {
        return eventType == EventType.WITHDRAWAL;
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
            throw new DuplicateRecordException("Duplicate/complete Withdrawal skipped: strikeOffEventId=" + withdrawalDetails.getWithdrawalId()
                    + ", status=" + withdrawalDetails.getProcessingStatus().getValue());
        }

        // Verify the current status is WITHDRAWAL_REQUESTED before updating to prevent invalid transitions.
        // This is a non-retryable guard: any status other than WITHDRAWAL_PROCESSING (duplicate) or
        // WITHDRAWAL_REQUESTED (expected) represents an unexpected system state that should be investigated.
        if (withdrawalDetails.getProcessingStatus() != WithdrawalProcessingStatus.WITHDRAWAL_REQUESTED) {
            throw new InvalidStrikeOffMessageException("Invalid status transition attempted: current status="
                    + withdrawalDetails.getProcessingStatus().getValue()
                    + ", expected=WITHDRAWAL_REQUESTED for withdrawalId=" + withdrawalDetails.getWithdrawalId());
        }

        // Update status to withdrawal-processing (SDK support pending)
        updateWithdrawalStatus(message);
        LOG.info("Updated withdrawal status to WITHDRAWAL_PROCESSING for withdrawalId=" + withdrawalDetails.getWithdrawalId());
    }

    private WithdrawAllObjectionsResponse getWithdrawalDetails(StrikeOffPartnerObjections message) {
        String uri = buildResourceUri(message, WITHDRAWALS);
        try {
            var response = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getAllWithdrawals(uri)
                    .execute();

            LOG.info("Fetched withdrawal for withdrawalId=" + response.getData().getWithdrawalId()
                    + ", status=" + response.getStatusCode());
            return response.getData();
        } catch (Exception e) {
            LOG.info("Failed to update withdrawal status using api url: " + uri);
            throw mapApiException(message.getEventId(), e);
        }
    }

    private void updateWithdrawalStatus(StrikeOffPartnerObjections message) {
        String uri = buildInternalStatusUri(message, WITHDRAWALS, WITHDRAWAL_STATUS);
         try {
             UpdateWithdrawalStatusRequest request = new UpdateWithdrawalStatusRequest();
             request.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING);

             internalApiClient
                     .privateStrikeOffPartnerObjectionsResourceHandler()
                     .updateWithdrawalStatus(uri, request)
                     .execute();
             LOG.info("Successfully updated withdrawal status to "
                     + WithdrawalProcessingStatus.WITHDRAWAL_PROCESSING
                     + " for eventId=" + message.getEventId());
         } catch (Exception e) {
             throw mapApiException(message.getEventId(), e);
         }
    }
}