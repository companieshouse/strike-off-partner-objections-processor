package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.logging.util.DataMap;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.SuccessFailureIndicator;

import java.util.Map;

/**
 * Processor for handling processed withdrawal outcome events.
 *
 * <p>This processor updates the withdrawal status in the Objections API based on the
 * processing outcome. For failed withdrawals, it persists the failure reason.
 */
@Component
public class StrikeOffProcessedWithdrawalsProcessor extends AbstractStrikeOffProcessedOutcomeProcessor {

    public StrikeOffProcessedWithdrawalsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
        if (!EventType.WITHDRAWAL.equals(message.getEventType())) {
            return;
        }

        String withdrawalId = message.getStrikeOffEventId();
        String uri = buildWithdrawalStatusUri(message, withdrawalId);
        UpdateWithdrawalStatusRequest updateRequest = buildUpdateRequest(message);
        var logMap = buildLogMap(message);

        try {
            LOG.infoContext(message.getStrikeOffEventId(),
                    "Updating withdrawal status: " + updateRequest.getProcessingStatus(), logMap);

            internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateWithdrawalStatus(uri, updateRequest)
                    .execute();

            LOG.infoContext(message.getStrikeOffEventId(),
                    "Withdrawal status updated successfully to: " + updateRequest.getProcessingStatus(), logMap);
        } catch (Exception ex) {
            LOG.info("Failed to update withdrawal status using api url: " + uri);
            throw mapApiException(message.getStrikeOffEventId(), ex);
        }
    }

    private UpdateWithdrawalStatusRequest buildUpdateRequest(StrikeOffPartnerObjectionsProcessed message) {
        UpdateWithdrawalStatusRequest request = new UpdateWithdrawalStatusRequest();

        if (SuccessFailureIndicator.SUCCESS.equals(message.getSuccessFailureIndicator())) {
            request.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED);
        } else {
            request.setProcessingStatus(WithdrawalProcessingStatus.WITHDRAWAL_REJECTED);
            request.setFailureReason(message.getErrorMessage());
        }
        
        return request;
    }

    private Map<String, Object> buildLogMap(StrikeOffPartnerObjectionsProcessed message) {
        var logMap = new DataMap.Builder()
                .companyNumber(message.getCompanyNumber())
                .build().getLogMap();
        logMap.put("strike_off_event_id", message.getStrikeOffEventId());
        logMap.put("event_type", message.getEventType());
        logMap.put("success_failure_indicator", message.getSuccessFailureIndicator());
        return logMap;
    }
}

