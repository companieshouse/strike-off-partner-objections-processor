package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.UpdateWithdrawalStatusRequest;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.ProcessorLogContext;

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
        try {
            var response = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getAllWithdrawals(uri)
                    .execute();
            return response.getData();
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
    }

    protected final WithdrawAllObjectionsResponse getWithdrawalDetails(T message, ProcessorLogContext logContext) {
        String uri = buildResourceUri(message, WITHDRAWALS);
        ProcessorLogContext requestContext = logContext
                .withOperation(ProcessorLogContext.INTERNAL_API_GET_WITHDRAWAL_REQUEST)
                .withResource(WITHDRAWAL_RESOURCE_KIND, uri);
        LOG.info("Requesting withdrawal details from internal API", requestContext.toLogMap());
        WithdrawAllObjectionsResponse response = getWithdrawalDetails(message);
        LOG.info("Received withdrawal details from internal API",
                requestContext.withOperation(ProcessorLogContext.INTERNAL_API_GET_WITHDRAWAL_RESPONSE)
                        .withWithdrawalId(response.getWithdrawalId())
                        .withStatus(response.getProcessingStatus().getValue())
                        .toLogMap());
        return response;
    }

    protected final void updateWithdrawalStatus(T message, WithdrawalProcessingStatus status) {
        UpdateWithdrawalStatusRequest request = new UpdateWithdrawalStatusRequest();
        request.setProcessingStatus(status);
        updateWithdrawalStatus(message, request);
    }

    protected final void updateWithdrawalStatus(
            T message, WithdrawalProcessingStatus status, ProcessorLogContext logContext) {
        String uri = buildInternalStatusUri(message, WITHDRAWALS, WITHDRAWAL_STATUS);
        ProcessorLogContext requestContext = logContext
                .withOperation(ProcessorLogContext.INTERNAL_API_UPDATE_WITHDRAWAL_REQUEST)
                .withResource(WITHDRAWAL_RESOURCE_KIND, uri)
                .withStatus(String.valueOf(status));
        LOG.info("Updating withdrawal status through internal API", requestContext.toLogMap());
        updateWithdrawalStatus(message, status);
        LOG.info("Withdrawal status update completed",
                requestContext.withOperation(ProcessorLogContext.INTERNAL_API_UPDATE_WITHDRAWAL_RESPONSE)
                        .toLogMap());
    }

    protected final void updateWithdrawalStatus(T message, UpdateWithdrawalStatusRequest request) {
        String uri = buildInternalStatusUri(message, WITHDRAWALS, WITHDRAWAL_STATUS);
        try {
            internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateWithdrawalStatus(uri, request)
                    .execute();
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
    }

    protected final void updateWithdrawalStatus(
            T message, UpdateWithdrawalStatusRequest request, ProcessorLogContext logContext) {
        String uri = buildInternalStatusUri(message, WITHDRAWALS, WITHDRAWAL_STATUS);
        ProcessorLogContext requestContext = logContext
                .withOperation(ProcessorLogContext.INTERNAL_API_UPDATE_WITHDRAWAL_REQUEST)
                .withResource(WITHDRAWAL_RESOURCE_KIND, uri)
                .withStatus(String.valueOf(request.getProcessingStatus()));
        LOG.info("Updating withdrawal status through internal API", requestContext.toLogMap());
        updateWithdrawalStatus(message, request);
        LOG.info("Withdrawal status update completed",
                requestContext.withOperation(ProcessorLogContext.INTERNAL_API_UPDATE_WITHDRAWAL_RESPONSE)
                        .toLogMap());
    }

    protected final void submitToChips(
            WithdrawAllObjectionsResponse response,
            T message,
            ChipsPartnerObjectionsSubmissionClient submissionClient,
            ProcessorLogContext logContext) {
        try {
            StrikeOffPartnerObjections objectionMessage = (StrikeOffPartnerObjections) message;
            submissionClient.submitForWithdrawals(
                    response, objectionMessage, logContext.withWithdrawalId(response.getWithdrawalId()));
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
    }
}
