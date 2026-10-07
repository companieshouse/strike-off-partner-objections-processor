package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.apache.avro.specific.SpecificRecordBase;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.UpdateObjectionStatusRequest;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.ProcessorLogContext;

import java.util.function.Function;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.OBJECTIONS;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.STATUS;

/**
 * Contains behaviour shared by incoming and processed objection events.
 *
 * @param <T> the objection event message type
 */
public abstract class AbstractObjectionsEventsProcessor<T extends SpecificRecordBase>
        extends AbstractEventsProcessor<T> {

    protected AbstractObjectionsEventsProcessor(InternalApiClient internalApiClient,
                                                Function<T, String> eventIdGetter,
                                                Function<T, String> companyNumberGetter,
                                                Function<T, String> strikeOffEventIdGetter) {
        super(internalApiClient, eventIdGetter, companyNumberGetter, strikeOffEventIdGetter);
    }

    protected final BaseObjectionResponse getObjectionDetails(T message) {
        String uri = buildResourceUri(message, OBJECTIONS);
        try {
            var response = internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .getObjection(uri)
                    .execute();
            return response.getData();
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
    }

    protected final BaseObjectionResponse getObjectionDetails(T message, ProcessorLogContext logContext) {
        String uri = buildResourceUri(message, OBJECTIONS);
        ProcessorLogContext requestContext = logContext
                .withOperation(ProcessorLogContext.INTERNAL_API_GET_OBJECTION_REQUEST)
                .withResource("objection", uri);
        LOG.info("Requesting objection details from internal API", requestContext.toLogMap());
        BaseObjectionResponse response = getObjectionDetails(message);
        LOG.info("Received objection details from internal API",
                requestContext.withOperation(ProcessorLogContext.INTERNAL_API_GET_OBJECTION_RESPONSE)
                        .withObjectionId(response.getObjectionId())
                        .withStatus(response.getProcessingStatus() == null
                                ? null : response.getProcessingStatus().getValue())
                        .toLogMap());
        return response;
    }

    protected final void updateObjectionStatus(T message, ObjectionProcessingStatus status) {
        UpdateObjectionStatusRequest request = new UpdateObjectionStatusRequest();
        request.setProcessingStatus(status);
        updateObjectionStatus(message, request);
    }

    protected final void updateObjectionStatus(
            T message, ObjectionProcessingStatus status, ProcessorLogContext logContext) {
        String uri = buildInternalStatusUri(message, OBJECTIONS, STATUS);
        ProcessorLogContext requestContext = logContext
                .withOperation(ProcessorLogContext.INTERNAL_API_UPDATE_OBJECTION_REQUEST)
                .withResource("objection", uri)
                .withStatus(String.valueOf(status));
        LOG.info("Updating objection status through internal API", requestContext.toLogMap());
        updateObjectionStatus(message, status);
        LOG.info("Objection status update completed",
                requestContext.withOperation(ProcessorLogContext.INTERNAL_API_UPDATE_OBJECTION_RESPONSE)
                        .toLogMap());
    }

    protected final void updateObjectionStatus(T message, UpdateObjectionStatusRequest request) {
        String uri = buildInternalStatusUri(message, OBJECTIONS, STATUS);
        try {
            internalApiClient
                    .privateStrikeOffPartnerObjectionsResourceHandler()
                    .updateObjectionStatus(uri, request)
                    .execute();
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
    }

    protected final void updateObjectionStatus(
            T message, UpdateObjectionStatusRequest request, ProcessorLogContext logContext) {
        String uri = buildInternalStatusUri(message, OBJECTIONS, STATUS);
        ProcessorLogContext requestContext = logContext
                .withOperation(ProcessorLogContext.INTERNAL_API_UPDATE_OBJECTION_REQUEST)
                .withResource("objection", uri)
                .withStatus(String.valueOf(request.getProcessingStatus()));
        LOG.info("Updating objection status through internal API", requestContext.toLogMap());
        updateObjectionStatus(message, request);
        LOG.info("Objection status update completed",
                requestContext.withOperation(ProcessorLogContext.INTERNAL_API_UPDATE_OBJECTION_RESPONSE)
                        .toLogMap());
    }

    protected final void submitToChips(
            BaseObjectionResponse response,
            T message,
            ChipsPartnerObjectionsSubmissionClient submissionClient,
            ProcessorLogContext logContext) {
        try {
            StrikeOffPartnerObjections objectionMessage = (StrikeOffPartnerObjections) message;
            submissionClient.submitForObjections(
                    response, objectionMessage, logContext.withObjectionId(response.getObjectionId()));
        } catch (Exception exception) {
            throw mapApiException(message, exception);
        }
    }

}
