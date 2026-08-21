package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.logging.util.DataMap;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.processed.SuccessFailureIndicator;

import java.util.Map;

/**
 * Processor for handling processed objection outcome events.
 *
 * <p>This processor updates the objection status in the Objections API based on the
 * processing outcome. For successful objections, it also persists the initial expiration
 * date. For failed objections, it persists the failure reason.
 */
@Component
public class StrikeOffProcessedObjectionsProcessor extends AbstractStrikeOffProcessedOutcomeProcessor {

    public StrikeOffProcessedObjectionsProcessor(InternalApiClient internalApiClient) {
        super(internalApiClient);
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
        if (!EventType.OBJECTION.equals(message.getEventType())) {
            return;
        }

        String objectionId = message.getStrikeOffEventId();
        String uri = buildObjectionStatusUri(message, objectionId);
        UpdateObjectionStatusRequest updateRequest = buildUpdateRequest(message);
        var logMap = buildLogMap(message);

        try {
            LOG.infoContext(message.getStrikeOffEventId(),
                    "Updating objection status: " + updateRequest.getProcessingStatus(), logMap);

            internalApiClient.privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateObjectionStatus(uri, updateRequest)
                    .execute();

            LOG.infoContext(message.getStrikeOffEventId(),
                    "Objection status updated successfully to: " + updateRequest.getProcessingStatus(), logMap);
        } catch (Exception ex) {
            LOG.info("Failed to update objection status using api url: " + uri);
            throw mapApiException(message.getStrikeOffEventId(), ex);
        }
    }

    private UpdateObjectionStatusRequest buildUpdateRequest(StrikeOffPartnerObjectionsProcessed message) {
        UpdateObjectionStatusRequest request = new UpdateObjectionStatusRequest();

        if (SuccessFailureIndicator.SUCCESS.equals(message.getSuccessFailureIndicator())) {
            request.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_ACCEPTED);
            if (message.getInitialExpirationOn() != null) {
                request.setInitialExpirationOn(message.getInitialExpirationOn());
            }
        } else {
            request.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_REJECTED);
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

