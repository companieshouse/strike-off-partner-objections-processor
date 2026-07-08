package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.net.URI;
import java.util.Map;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerObjectionsProcessorConstants.APPLICATION_NAMESPACE;

@Component
public class StrikeOffPartnerObjectionsApiClient {

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);
    private static final String PROCESSING_STATUS = "processing_status";

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final String apiBaseUrl;
    private final String getObjectionPath;
    private final String updateObjectionStatusPath;

    @Autowired
    public StrikeOffPartnerObjectionsApiClient(
            @Value("${strikeoff.api.base-url}") String apiBaseUrl,
            @Value("${strikeoff.api.path.get-objection}") String getObjectionPath,
            @Value("${strikeoff.api.path.update-objection-status}") String updateObjectionStatusPath) {
        this(new RestTemplate(), new ObjectMapper(), apiBaseUrl, getObjectionPath, updateObjectionStatusPath);
    }

    StrikeOffPartnerObjectionsApiClient(RestTemplate restTemplate,
                                        ObjectMapper objectMapper,
                                        String apiBaseUrl,
                                        String getObjectionPath,
                                        String updateObjectionStatusPath) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.apiBaseUrl = apiBaseUrl;
        this.getObjectionPath = getObjectionPath;
        this.updateObjectionStatusPath = updateObjectionStatusPath;
    }

    public String getObjectionProcessingStatus(String companyNumber, String objectionId, String requestId) {
        URI uri = buildUri(getObjectionPath, companyNumber, objectionId);
        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    uri,
                    HttpMethod.GET,
                    new HttpEntity<>(createHeaders(requestId)),
                    String.class
            );

            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
                throw new IllegalStateException("Unexpected API response for objection: " + objectionId);
            }
            return extractProcessingStatus(response.getBody(), objectionId);
        } catch (HttpClientErrorException.NotFound ex) {
            throw new InvalidStrikeOffMessageException("Objection not found: " + objectionId);
        } catch (HttpClientErrorException ex) {
            throw new InvalidStrikeOffMessageException(
                    "Failed to fetch objection: " + objectionId + " status=" + ex.getStatusCode());
        }
    }

    public void updateObjectionStatusToProcessing(String companyNumber, String objectionId, String requestId) {
        Map<String, String> body = Map.of(PROCESSING_STATUS, "objection-processing");
        URI uri = buildUri(updateObjectionStatusPath, companyNumber, objectionId);

        try {
            restTemplate.exchange(
                    uri,
                    HttpMethod.POST,
                    new HttpEntity<>(body, createHeaders(requestId)),
                    Void.class
            );
        } catch (HttpClientErrorException.Conflict ex) {
            LOG.infoContext(requestId,
                    "Ignoring conflict when setting objection status to processing for objection_id=" + objectionId,
                    Map.of("objection_id", objectionId));
        } catch (HttpClientErrorException.NotFound ex) {
            throw new InvalidStrikeOffMessageException("Objection not found: " + objectionId);
        } catch (HttpClientErrorException ex) {
            throw new InvalidStrikeOffMessageException(
                    "Failed to update objection status for: " + objectionId + " status=" + ex.getStatusCode());
        }
    }

    private String extractProcessingStatus(String responseBody, String objectionId) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);
            JsonNode statusNode = root.path(PROCESSING_STATUS);
            if (statusNode.isMissingNode() || statusNode.isNull() || statusNode.asText().isBlank()) {
                throw new InvalidStrikeOffMessageException(
                        "Missing processing_status in API response for objection: " + objectionId);
            }
            return statusNode.asText();
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Invalid API response JSON for objection: " + objectionId, ex);
        }
    }

    private HttpHeaders createHeaders(String requestId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("X-Request-Id", requestId);
        return headers;
    }

    private URI buildUri(String path, String companyNumber, String objectionId) {
        return UriComponentsBuilder.fromUriString(apiBaseUrl)
                .path(path)
                .buildAndExpand(Map.of("company_number", companyNumber, "objection_id", objectionId))
                .toUri();
    }
}
