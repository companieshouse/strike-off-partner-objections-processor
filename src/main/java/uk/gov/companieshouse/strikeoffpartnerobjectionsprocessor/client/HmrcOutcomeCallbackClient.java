package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.APPLICATION_NAMESPACE;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.HMRC_CALLBACK_UPDATE_ENDPOINT;

@Component
public class HmrcOutcomeCallbackClient {
    private static final int ACCEPTED_STATUS = 202;
    private static final int TRANSPORT_ERROR_STATUS = 503;
    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    private final RestTemplate restTemplate;
    private final String hmrcCallbackBaseUrl;

    public HmrcOutcomeCallbackClient(
            RestTemplate restTemplate,
            @Value("${hmrc.callback.base-url}") String hmrcCallbackBaseUrl) {
        this.restTemplate = restTemplate;
        this.hmrcCallbackBaseUrl = hmrcCallbackBaseUrl;
    }

    public void submitOutcomeCallback(HmrcOutcomeCallbackRequest request) {
        String endpoint = buildHmrcCallbackEndpointUrl();
        Map<String, Object> logMap = new HashMap<>();
        logMap.put("resource_id", request.resource_id());
        logMap.put("company_number", request.company_number());
        logMap.put("processing_outcome", request.processing_outcome());

        LOG.infoContext(request.resource_id(), "Sending HMRC callback notification", logMap);
        try {
            int statusCode = submitToEndpoint(endpoint, request);
            logMap.put("status_code", statusCode);
            LOG.infoContext(request.resource_id(), "HMRC callback notification accepted", logMap);
        } catch (RuntimeException exception) {
            LOG.errorContext(request.resource_id(), "HMRC callback notification failed", exception, logMap);
            throw exception;
        }
    }

    private int submitToEndpoint(String endpoint, HmrcOutcomeCallbackRequest request) {
        ResponseEntity<String> response;

        try {
            response = restTemplate.postForEntity(endpoint, request, String.class);
        } catch (HttpStatusCodeException exception) {
            throw new HmrcCallbackException("HMRC callback failed", exception.getStatusCode().value(), exception);
        } catch (RestClientException exception) {
            throw new HmrcCallbackException("HMRC callback failed due to transport error", TRANSPORT_ERROR_STATUS, exception);
        }

        int statusCode = response.getStatusCode().value();
        if (statusCode != ACCEPTED_STATUS) {
            throw new HmrcCallbackException("HMRC callback returned unexpected status", statusCode);
        }
        return statusCode;
    }

    private String buildHmrcCallbackEndpointUrl() {
        if (hmrcCallbackBaseUrl.endsWith("/")) {
            return hmrcCallbackBaseUrl.substring(0, hmrcCallbackBaseUrl.length() - 1) + HMRC_CALLBACK_UPDATE_ENDPOINT;
        }
        return hmrcCallbackBaseUrl + HMRC_CALLBACK_UPDATE_ENDPOINT;
    }
}

