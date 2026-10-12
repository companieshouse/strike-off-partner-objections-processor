package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.logging.util.DataMap;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.DataMapHolder;

import java.util.function.Function;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.WITHDRAWALS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.WITHDRAWAL_RESOURCE_KIND;
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
        DataMap.Builder requestContext = DataMapHolder.snapshot()
                .operation(DataMapHolder.INTERNAL_API_GET_WITHDRAWAL_REQUEST)
                .resourceKind(WITHDRAWAL_RESOURCE_KIND).resourceUri(uri);
        LOG.info("Requesting withdrawal details from internal API", requestContext.build().getLogMap());
        WithdrawAllObjectionsResponse response;
        try {
            var apiResponse = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getAllWithdrawals(uri)
                    .execute();
            response = apiResponse.getData();
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
        DataMapHolder.get().withdrawalId(response.getWithdrawalId());
        LOG.info("Received withdrawal details from internal API",
                requestContext.operation(DataMapHolder.INTERNAL_API_GET_WITHDRAWAL_RESPONSE)
                        .withdrawalId(response.getWithdrawalId())
                        .status(response.getProcessingStatus().getValue())
                        .build().getLogMap());
        return response;
    }

    protected final void updateWithdrawalStatus(T message, WithdrawalProcessingStatus status) {
        UpdateWithdrawalStatusRequest request = new UpdateWithdrawalStatusRequest();
        request.setProcessingStatus(status);
        updateWithdrawalStatus(message, request);
    }

    protected final void updateWithdrawalStatus(T message, UpdateWithdrawalStatusRequest request) {
        String uri = buildInternalStatusUri(message, WITHDRAWALS, WITHDRAWAL_STATUS);
        DataMap.Builder requestContext = DataMapHolder.snapshot()
                .operation(DataMapHolder.INTERNAL_API_UPDATE_WITHDRAWAL_REQUEST)
                .resourceKind(WITHDRAWAL_RESOURCE_KIND).resourceUri(uri)
                .status(String.valueOf(request.getProcessingStatus()));
        LOG.info("Updating withdrawal status through internal API", requestContext.build().getLogMap());
        try {
            internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateWithdrawalStatus(uri, request)
                    .execute();
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
        LOG.info("Withdrawal status update completed",
                requestContext.operation(DataMapHolder.INTERNAL_API_UPDATE_WITHDRAWAL_RESPONSE)
                        .build().getLogMap());
    }

    protected final void submitToChips(
            WithdrawAllObjectionsResponse response,
            T message,
            ChipsPartnerObjectionsSubmissionClient submissionClient) {
        try {
            StrikeOffPartnerObjections objectionMessage = (StrikeOffPartnerObjections) message;
            submissionClient.submitForWithdrawals(response, objectionMessage);
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
    }
}
