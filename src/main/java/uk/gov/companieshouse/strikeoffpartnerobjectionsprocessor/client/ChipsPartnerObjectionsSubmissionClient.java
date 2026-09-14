package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.CHIPS_PARTNER_OBJECTIONS_ENDPOINT;

@Component
public class ChipsPartnerObjectionsSubmissionClient {
    private static final int ACCEPTED_STATUS = 202;
    private static final int TRANSPORT_ERROR_STATUS = 503;

    private final RestTemplate restTemplate;
    private final String chipsRestInterfaceBaseUrl;

    public ChipsPartnerObjectionsSubmissionClient(
            RestTemplate restTemplate,
            @Value("${chips.rest-interface.base-url}") String chipsRestInterfaceBaseUrl) {
        this.restTemplate = restTemplate;
        this.chipsRestInterfaceBaseUrl = chipsRestInterfaceBaseUrl;
    }

    public void submitForObjections(BaseObjectionResponse baseResponse, StrikeOffPartnerObjections message) {
        ChipsPartnerObjectionsSubmissionRequest request = ChipsPartnerObjectionsSubmissionRequest.from(baseResponse, message);
        submitToEndpoint(buildChipsEndpointUrl(), request);
    }

    public void submitForWithdrawals(WithdrawAllObjectionsResponse baseResponse, StrikeOffPartnerObjections message) {
        ChipsPartnerObjectionsSubmissionRequest request = ChipsPartnerObjectionsSubmissionRequest.from(baseResponse, message);
        submitToEndpoint(buildChipsEndpointUrl(), request);
    }

    private void submitToEndpoint(String endpoint, Object request) {
        ResponseEntity<String> response;

        try {
            response = restTemplate.postForEntity(endpoint, request, String.class);
        } catch (HttpStatusCodeException exception) {
            throw new ChipsSubmissionException("CHIPS submission failed", exception.getStatusCode().value(), exception);
        } catch (RestClientException exception) {
            throw new ChipsSubmissionException("CHIPS submission failed due to transport error", TRANSPORT_ERROR_STATUS, exception);
        }

        int statusCode = response.getStatusCode().value();
        if (statusCode != ACCEPTED_STATUS) {
            throw new ChipsSubmissionException("CHIPS submission returned unexpected status", statusCode);
        }
    }

    private String buildChipsEndpointUrl() {
        return joinBaseAndPath(chipsRestInterfaceBaseUrl, CHIPS_PARTNER_OBJECTIONS_ENDPOINT);
    }


    private String joinBaseAndPath(String baseUrl, String path) {
        if (baseUrl.endsWith("/")) {
            return baseUrl.substring(0, baseUrl.length() - 1) + path;
        }
        return baseUrl + path;
    }
}
