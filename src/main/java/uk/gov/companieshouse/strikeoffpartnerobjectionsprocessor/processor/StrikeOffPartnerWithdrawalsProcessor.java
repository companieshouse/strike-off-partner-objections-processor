package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerObjectionsProcessorConstants.APPLICATION_NAMESPACE;

/**
 * Processor for strike-off partner withdrawal events.
 *
 * <p>This implementation handles only {@link EventType#WITHDRAWAL} messages and
 * performs withdrawal-specific processing after base validation is completed in
 * {@link AbstractStrikeOffPartnerObjectionsProcessor#process(StrikeOffPartnerObjections)}.
 */
@Component
public class StrikeOffPartnerWithdrawalsProcessor extends AbstractStrikeOffPartnerObjectionsProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);
    private static final String RESOURCE_SEGMENT = "strike-off-partner-withdrawals";

    protected StrikeOffPartnerWithdrawalsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
    }

    @Override
    protected boolean supports(EventType eventType) {
        return eventType == EventType.WITHDRAWAL;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjections message) {
        LOG.info("Processing objection event with ID: " + message.getEventId());
        getWithdrawalDetails(message);
    }

    private void getWithdrawalDetails(StrikeOffPartnerObjections message) {
        String uri = buildResourceUri(message, RESOURCE_SEGMENT);
        try {
            var response = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getAllWithdrawals(uri)
                    .execute();

            LOG.info("Fetched withdrawal for withdrawalId=" + response.getData().getWithdrawalId()
                    + ", status=" + response.getStatusCode());
        } catch (Exception e) {
            throw mapApiException(message.getEventId(), e);
        }
    }
}