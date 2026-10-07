package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.ProcessorLogContext;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants;

@Component
public class ChipsPartnerObjectionsSubmissionClient {
    private static final int ACCEPTED_STATUS = 202;
    private static final int UNAUTHORIZED_STATUS = 401;
    private static final int TRANSPORT_ERROR_STATUS = 503;
    private static final String CHIPS_REST_API_KEY_HEADER = "CHIPS-REST-API-KEY";
    private static final Logger LOG = LoggerFactory.getLogger(
            StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE);
    private final RestTemplate restTemplate;
    private final ChipsPartnerObjectionsSubmissionRequestMapper requestMapper;
    private final String chipsRestInterfaceBaseUrl;
    private final String chipsRestApiKey;

    public ChipsPartnerObjectionsSubmissionClient(
            RestTemplate restTemplate,
            @Value("${chips.rest-interface.base-url}") String chipsRestInterfaceBaseUrl,
            @Value("${chips.rest-interface.api-key}") String chipsRestApiKey,
            ChipsPartnerObjectionsSubmissionRequestMapper requestMapper) {
        this.restTemplate = restTemplate;
        this.requestMapper = requestMapper;
        this.chipsRestInterfaceBaseUrl = chipsRestInterfaceBaseUrl;
        this.chipsRestApiKey = chipsRestApiKey;
        
        if (chipsRestApiKey == null || chipsRestApiKey.isBlank()) {
            throw new IllegalArgumentException("CHIPS REST API key must not be blank");
        }
    }

    public void submitForObjections(BaseObjectionResponse baseResponse, StrikeOffPartnerObjections message) {
        submitForObjections(baseResponse, message, ProcessorLogContext.empty()
                .withCompanyNumber(baseResponse.getCompanyNumber())
                .withObjectionId(baseResponse.getObjectionId())
                .withEventType("objection"));
    }

    public void submitForObjections(
            BaseObjectionResponse baseResponse,
            StrikeOffPartnerObjections message,
            ProcessorLogContext logContext) {
        ChipsPartnerObjectionsSubmissionRequest request = requestMapper.objectionRequest(baseResponse, message);
        submit(request, logContext);
    }

    public void submitForWithdrawals(WithdrawAllObjectionsResponse baseResponse, StrikeOffPartnerObjections message) {
        submitForWithdrawals(baseResponse, message, ProcessorLogContext.empty()
                .withCompanyNumber(baseResponse.getCompanyNumber())
                .withWithdrawalId(baseResponse.getWithdrawalId())
                .withEventType("withdrawal"));
    }

    public void submitForWithdrawals(
            WithdrawAllObjectionsResponse baseResponse,
            StrikeOffPartnerObjections message,
            ProcessorLogContext logContext) {
        ChipsPartnerObjectionsSubmissionRequest request = requestMapper.withdrawalRequest(baseResponse, message);
        submit(request, logContext);
    }

    public void submit(ChipsPartnerObjectionsSubmissionRequest request) {
        ProcessorLogContext logContext = ProcessorLogContext.empty()
                .withCompanyNumber(request.company_number())
                .withEventType(request.kind())
                .withObjectionId("Objection".equals(request.kind()) ? request.strike_off_event_id() : null)
                .withWithdrawalId("Withdrawal".equals(request.kind()) ? request.strike_off_event_id() : null);
        submit(request, logContext);
    }

    public void submit(ChipsPartnerObjectionsSubmissionRequest request, ProcessorLogContext logContext) {
        String endpoint = buildEndpointUrl();
        ResponseEntity<String> response;

        ProcessorLogContext requestContext = logContext.withOperation(ProcessorLogContext.CHIPS_REQUEST);
        LOG.info("Submitting objection or withdrawal to CHIPS", requestContext.toLogMap());
        try {
            HttpEntity<ChipsPartnerObjectionsSubmissionRequest> httpEntity = createHttpEntity(request);
            response = restTemplate.postForEntity(endpoint, httpEntity, String.class);
        } catch (HttpStatusCodeException exception) {
            LOG.info("CHIPS response received",
                    logContext.withOperation(ProcessorLogContext.CHIPS_RESPONSE)
                            .withStatus(String.valueOf(exception.getStatusCode().value()))
                            .withErrorType(exception.getClass().getSimpleName())
                            .toLogMap());
            if (exception.getStatusCode().value() == UNAUTHORIZED_STATUS) {
                throw new ChipsSubmissionException("CHIPS authentication failed - invalid or missing API key", exception.getStatusCode().value(), exception);
            }
            throw new ChipsSubmissionException("CHIPS submission failed", exception.getStatusCode().value(), exception);
        } catch (RestClientException exception) {
            LOG.info("CHIPS submission failed before receiving a response",
                    logContext.withOperation(ProcessorLogContext.CHIPS_RESPONSE)
                            .withErrorType(exception.getClass().getSimpleName())
                            .toLogMap());
            throw new ChipsSubmissionException("CHIPS submission failed due to transport error", TRANSPORT_ERROR_STATUS, exception);
        }

        int statusCode = response.getStatusCode().value();
        LOG.info("CHIPS response received",
                logContext.withOperation(ProcessorLogContext.CHIPS_RESPONSE)
                        .withStatus(String.valueOf(statusCode))
                        .toLogMap());
        if (statusCode == UNAUTHORIZED_STATUS) {
            throw new ChipsSubmissionException("CHIPS authentication failed - invalid or missing API key", statusCode);
        }
        if (statusCode != ACCEPTED_STATUS) {
            throw new ChipsSubmissionException("CHIPS submission returned unexpected status", statusCode);
        }
    }

    private HttpEntity<ChipsPartnerObjectionsSubmissionRequest> createHttpEntity(ChipsPartnerObjectionsSubmissionRequest request) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(CHIPS_REST_API_KEY_HEADER, chipsRestApiKey);
        return new HttpEntity<>(request, headers);
    }

    private String buildEndpointUrl() {
        if (chipsRestInterfaceBaseUrl.endsWith("/")) {
            return chipsRestInterfaceBaseUrl.substring(0, chipsRestInterfaceBaseUrl.length() - 1)
                    + StrikeOffPartnerEventsProcessorConstants.CHIPS_PARTNER_OBJECTIONS_ENDPOINT;
        }
        return chipsRestInterfaceBaseUrl + StrikeOffPartnerEventsProcessorConstants.CHIPS_PARTNER_OBJECTIONS_ENDPOINT;
    }
}
