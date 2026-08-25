package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;

import java.util.function.Function;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.WITHDRAWALS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.WITHDRAWAL_STATUS;

/**
 * Contains behaviour shared by withdrawal event processors.
 *
 * @param <T> the withdrawal event message type
 */
public abstract class AbstractWithdrawalsEventsProcessor<T extends SpecificRecordBase>
        extends AbstractEventsProcessor<T> {

    protected AbstractWithdrawalsEventsProcessor(InternalApiClient internalApiClient,
                                                 Function<T, String> eventIdGetter,
                                                 Function<T, String> companyNumberGetter,
                                                 Function<T, String> strikeOffEventIdGetter) {
        super(internalApiClient, eventIdGetter, companyNumberGetter, strikeOffEventIdGetter);
    }

    protected final WithdrawAllObjectionsResponse getWithdrawalDetails(T message) {
        String uri = buildResourceUri(message, WITHDRAWALS);
        try {
            var response = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getAllWithdrawals(uri)
                    .execute();
            LOG.info("Fetched withdrawal for withdrawalId=" + response.getData().getWithdrawalId()
                    + ", status=" + response.getStatusCode());
            return response.getData();
        } catch (Exception exception) {
            LOG.info("Failed to get withdrawal - api url: " + uri);
            throw mapApiException(message, exception);
        }
    }

    protected final void updateWithdrawalStatus(T message, WithdrawalProcessingStatus status) {
        String uri = buildInternalStatusUri(message, WITHDRAWALS, WITHDRAWAL_STATUS);
        try {
            UpdateWithdrawalStatusRequest request = new UpdateWithdrawalStatusRequest();
            request.setProcessingStatus(status);
            internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateWithdrawalStatus(uri, request)
                    .execute();
            LOG.info("Successfully updated withdrawal status to " + status
                    + " for eventId=" + getEventId(message));
        } catch (Exception exception) {
            LOG.info("Failed to update withdrawal status using api url: " + uri);
            throw mapApiException(message, exception);
        }
    }
}
