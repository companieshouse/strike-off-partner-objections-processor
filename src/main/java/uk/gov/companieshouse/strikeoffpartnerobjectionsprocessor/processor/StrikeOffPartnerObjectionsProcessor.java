package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerObjectionsProcessorConstants.APPLICATION_NAMESPACE;

/**
 * Processor for strike-off partner objection events.
 *
 * <p>This implementation handles only {@link EventType#OBJECTION} messages and
 * performs objection-specific processing after base validation is completed in
 * {@link AbstractStrikeOffPartnerObjectionsProcessor#process(StrikeOffPartnerObjections)}.
 */
@Component
public class StrikeOffPartnerObjectionsProcessor extends AbstractStrikeOffPartnerObjectionsProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);
    private static final String RESOURCE_SEGMENT = "strike-off-partner-objections";

    protected StrikeOffPartnerObjectionsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
    }

    @Override
    protected boolean supports(EventType eventType) {
        return eventType == EventType.OBJECTION;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjections message) {
        LOG.info("Processing objection event with ID: " + message.getEventId());
        var response = getObjectionDetails(message);
        LOG.info("Objection details fetched: objectionId=" + response.getObjectionId());
    }

    private BaseObjectionResponse getObjectionDetails(StrikeOffPartnerObjections message) {
        String uri = buildResourceUri(message, RESOURCE_SEGMENT);
        try {
            var response = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getObjection(uri)
                    .execute();
            LOG.info("Fetched objection for objectionId=" + response.getData().getObjectionId()
                    + ", status=" + response.getStatusCode());
            return response.getData();
        } catch (Exception e) {
            throw mapApiException(message.getEventId(), e);
        }
    }
}
