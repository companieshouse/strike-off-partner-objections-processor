package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.strikeoff.partner.objections.ProcessedEventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjectionsProcessed;
import uk.gov.companieshouse.strikeoff.partner.objections.SuccessFailureIndicator;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcOutcomeCallbackClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.HmrcOutcomeCallbackRequest;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.DuplicateRecordException;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.CALLBACK_KIND_OBJECTION;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.OBJECTIONS;

@Component
public class ProcessedObjectionsProcessor
        extends AbstractObjectionsEventsProcessor<StrikeOffPartnerObjectionsProcessed> {

    private final HmrcOutcomeCallbackClient hmrcOutcomeCallbackClient;

    protected ProcessedObjectionsProcessor(
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
        return message.getEventType() == ProcessedEventType.OBJECTION;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjectionsProcessed message) {
        LOG.info("Processing objection outcome event with ID: " + message.getStrikeOffEventId());
        BaseObjectionResponse objection = getOrSkipNotFound(
                () -> getObjectionDetails(message),
                () -> new DuplicateRecordException("Skipping processed objection event because objection was not found: strikeOffEventId="
                        + message.getStrikeOffEventId()
                        + ", companyNumber=" + message.getCompanyNumber()));

        UpdateObjectionStatusRequest request = buildUpdateRequest(message);
        ObjectionProcessingStatus currentStatus = objection.getProcessingStatus();
        ObjectionProcessingStatus targetStatus = request.getProcessingStatus();

        if (isConflictingTerminalState(currentStatus, targetStatus)) {
            throw new DuplicateRecordException("Duplicate/complete Objection skipped: strikeOffEventId="
                    + message.getStrikeOffEventId()
                    + ", objectionId=" + objection.getObjectionId()
                    + ", status=" + currentStatus.getValue());
        }

        if (!isDuplicateRecord(currentStatus.getValue(), targetStatus.getValue())) {
            updateObjectionStatus(message, request);
            LOG.info("Updated objection status to " + targetStatus + " for objectionId=" + objection.getObjectionId());
        } else {
            LOG.info("Objection already in target status " + targetStatus
                    + " for objectionId=" + objection.getObjectionId()
                    + ". Proceeding with callback notification.");
        }

        submitOutcomeCallback(message, objection.getObjectionId(), targetStatus);
    }

    private UpdateObjectionStatusRequest buildUpdateRequest(StrikeOffPartnerObjectionsProcessed message) {
        UpdateObjectionStatusRequest request = new UpdateObjectionStatusRequest();
        if (message.getSuccessFailureIndicator() == SuccessFailureIndicator.SUCCESS) {
            request.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_ACCEPTED);
            request.setInitialExpirationOn(message.getInitialExpirationOn());
            return request;
        }

        request.setProcessingStatus(ObjectionProcessingStatus.OBJECTION_REJECTED);
        request.setFailureReason(message.getErrorMessage());
        return request;
    }

    private boolean isConflictingTerminalState(
            ObjectionProcessingStatus currentStatus,
            ObjectionProcessingStatus requestedStatus) {
        return currentStatus == ObjectionProcessingStatus.OBJECTION_ACCEPTED
                && requestedStatus == ObjectionProcessingStatus.OBJECTION_REJECTED
                || currentStatus == ObjectionProcessingStatus.OBJECTION_REJECTED
                && requestedStatus == ObjectionProcessingStatus.OBJECTION_ACCEPTED;
    }

    private void submitOutcomeCallback(
            StrikeOffPartnerObjectionsProcessed message,
            String resourceId,
            ObjectionProcessingStatus status) {
        HmrcOutcomeCallbackRequest callbackRequest = new HmrcOutcomeCallbackRequest(
                CALLBACK_KIND_OBJECTION,
                resourceId,
                message.getCompanyNumber(),
                buildResourceUri(message, OBJECTIONS),
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
