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
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants;

@Component
public class ChipsPartnerObjectionsSubmissionClient {
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
        submit(request);
    }

    public void submitForWithdrawals(WithdrawAllObjectionsResponse baseResponse, StrikeOffPartnerObjections message) {
        ChipsPartnerObjectionsSubmissionRequest request = ChipsPartnerObjectionsSubmissionRequest.from(baseResponse, message);
        submit(request);
    }

    public void submit(ChipsPartnerObjectionsSubmissionRequest request) {
        String endpoint = buildEndpointUrl();
        ResponseEntity<String> response;

        try {
            response = restTemplate.postForEntity(endpoint, request, String.class);
        } catch (HttpStatusCodeException exception) {
            throw new ChipsSubmissionException("CHIPS submission failed", exception.getStatusCode().value(), exception);
        } catch (RestClientException exception) {
            throw new ChipsSubmissionException("CHIPS submission failed due to transport error", TRANSPORT_ERROR_STATUS, exception);
        }

        int statusCode = response.getStatusCode().value();
        if (statusCode != 202) {
            throw new ChipsSubmissionException("CHIPS submission returned unexpected status", statusCode);
        }
    }

    private String buildEndpointUrl() {
        if (chipsRestInterfaceBaseUrl.endsWith("/")) {
            return chipsRestInterfaceBaseUrl.substring(0, chipsRestInterfaceBaseUrl.length() - 1)
                    + StrikeOffPartnerEventsProcessorConstants.CHIPS_PARTNER_OBJECTIONS_ENDPOINT;
        }
        return chipsRestInterfaceBaseUrl + StrikeOffPartnerEventsProcessorConstants.CHIPS_PARTNER_OBJECTIONS_ENDPOINT;
    }
}
