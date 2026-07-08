package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.StrikeOffPartnerObjectionsApiClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.util.Map;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerObjectionsProcessorConstants.APPLICATION_NAMESPACE;

@Component
public class StrikeOffPartnerObjectionsProcessor extends AbstractStrikeOffPartnerObjectionsProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);
    private static final String OBJECTION_SUBMITTED = "objection-submitted";

    private final StrikeOffPartnerObjectionsApiClient apiClient;

    public StrikeOffPartnerObjectionsProcessor(StrikeOffPartnerObjectionsApiClient apiClient) {
        this.apiClient = apiClient;
    }

    @Override
    protected boolean supports(EventType eventType) {
        return eventType == EventType.OBJECTION;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjections message) {
        final String requestId = message.getEventId();
        final String companyNumber = message.getSource();
        final String objectionId = message.getStrikeOffEventId();

        if (companyNumber == null || companyNumber.isBlank()) {
            throw new InvalidStrikeOffMessageException("Missing companyNumber in source");
        }
        if (objectionId == null || objectionId.isBlank()) {
            throw new InvalidStrikeOffMessageException("Missing objectionId");
        }

        String currentStatus = apiClient.getObjectionProcessingStatus(companyNumber, objectionId, requestId);
        if (!OBJECTION_SUBMITTED.equals(currentStatus)) {
            LOG.infoContext(requestId,
                    "Skipping duplicate/already-processed objection; no status update required",
                    Map.of("company_number", companyNumber, "objection_id", objectionId,
                            "current_status", currentStatus));
            return;
        }

        apiClient.updateObjectionStatusToProcessing(companyNumber, objectionId, requestId);
        LOG.infoContext(requestId,
                "Objection status moved to objection-processing",
                Map.of("company_number", companyNumber, "objection_id", objectionId));
    }
}
