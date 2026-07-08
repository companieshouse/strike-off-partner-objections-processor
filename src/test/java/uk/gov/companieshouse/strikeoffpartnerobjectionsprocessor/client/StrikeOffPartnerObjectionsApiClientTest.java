package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StrikeOffPartnerObjectionsApiClientTest {

    private RestTemplate restTemplate;
    private StrikeOffPartnerObjectionsApiClient client;

    @BeforeEach
    void setUp() {
        restTemplate = mock(RestTemplate.class);
        client = new StrikeOffPartnerObjectionsApiClient(
                restTemplate,
                new ObjectMapper(),
                "http://localhost:8080",
                "/company/{company_number}/strike-off-partner-objections/{objection_id}",
                "/internal/company/{company_number}/strike-off-partner-objections/{objection_id}/status"
        );
    }

    @Test
    void getObjectionProcessingStatus_returnsProcessingStatus() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class)))
                .thenReturn(ResponseEntity.ok("{\"processing_status\":\"objection-submitted\"}"));

        String status = client.getObjectionProcessingStatus("00006401", "obj-001", "evt-001");

        assertEquals("objection-submitted", status);
        verify(restTemplate).exchange(
                any(URI.class),
                eq(HttpMethod.GET),
                argThat((HttpEntity<?> entity) -> "evt-001".equals(entity.getHeaders().getFirst("X-Request-Id"))),
                eq(String.class));
    }

    @Test
    void getObjectionProcessingStatus_notFound_throwsInvalidMessage() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.NOT_FOUND,
                        "Not Found",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8));

        assertThrows(InvalidStrikeOffMessageException.class,
                () -> client.getObjectionProcessingStatus("00006401", "obj-001", "evt-001"));
    }

    @Test
    void getObjectionProcessingStatus_nullBody_throwsIllegalStateException() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class)))
                .thenReturn(ResponseEntity.ok(null));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> client.getObjectionProcessingStatus("00006401", "obj-001", "evt-001"));

        assertTrue(ex.getMessage().contains("Unexpected API response"));
    }

    @Test
    void getObjectionProcessingStatus_non2xxResponse_throwsIllegalStateException() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class)))
                .thenReturn(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("{\"processing_status\":\"objection-submitted\"}"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> client.getObjectionProcessingStatus("00006401", "obj-001", "evt-001"));

        assertTrue(ex.getMessage().contains("Unexpected API response"));
    }

    @Test
    void getObjectionProcessingStatus_malformedJson_throwsIllegalStateException() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class)))
                .thenReturn(ResponseEntity.ok("{invalid-json}"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> client.getObjectionProcessingStatus("00006401", "obj-001", "evt-001"));

        assertTrue(ex.getMessage().contains("Invalid API response JSON"));
    }

    @Test
    void getObjectionProcessingStatus_missingProcessingStatus_throwsInvalidMessage() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class)))
                .thenReturn(ResponseEntity.ok("{\"status\":\"objection-submitted\"}"));

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> client.getObjectionProcessingStatus("00006401", "obj-001", "evt-001"));

        assertTrue(ex.getMessage().contains("Missing processing_status"));
    }

    @Test
    void getObjectionProcessingStatus_blankProcessingStatus_throwsInvalidMessage() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class)))
                .thenReturn(ResponseEntity.ok("{\"processing_status\":\"   \"}"));

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> client.getObjectionProcessingStatus("00006401", "obj-001", "evt-001"));

        assertTrue(ex.getMessage().contains("Missing processing_status"));
    }

    @Test
    void getObjectionProcessingStatus_httpClientError_throwsInvalidMessage() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.BAD_REQUEST,
                        "Bad Request",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8));

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> client.getObjectionProcessingStatus("00006401", "obj-001", "evt-001"));

        assertTrue(ex.getMessage().contains("Failed to fetch objection"));
    }

    @Test
    void updateObjectionStatusToProcessing_postsExpectedPayload() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.NO_CONTENT));

        client.updateObjectionStatusToProcessing("00006401", "obj-001", "evt-001");

        verify(restTemplate).exchange(
                any(URI.class),
                eq(HttpMethod.POST),
                argThat((HttpEntity<?> entity) -> {
                    if (!"evt-001".equals(entity.getHeaders().getFirst("X-Request-Id"))) {
                        return false;
                    }
                    Object body = entity.getBody();
                    if (!(body instanceof Map<?, ?> requestBody)) {
                        return false;
                    }
                    return "objection-processing".equals(requestBody.get("processing_status"));
                }),
                eq(Void.class));
    }

    @Test
    void updateObjectionStatusToProcessing_conflict_isTreatedAsIdempotent() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.CONFLICT,
                        "Conflict",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8));

        assertDoesNotThrow(() -> client.updateObjectionStatusToProcessing("00006401", "obj-001", "evt-001"));
    }

    @Test
    void updateObjectionStatusToProcessing_notFound_throwsInvalidMessage() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.NOT_FOUND,
                        "Not Found",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8));

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> client.updateObjectionStatusToProcessing("00006401", "obj-001", "evt-001"));

        assertTrue(ex.getMessage().contains("Objection not found"));
    }

    @Test
    void updateObjectionStatusToProcessing_httpClientError_throwsInvalidMessage() {
        when(restTemplate.exchange(any(URI.class), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.BAD_REQUEST,
                        "Bad Request",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8));

        InvalidStrikeOffMessageException ex = assertThrows(InvalidStrikeOffMessageException.class,
                () -> client.updateObjectionStatusToProcessing("00006401", "obj-001", "evt-001"));

        assertTrue(ex.getMessage().contains("Failed to update objection status"));
    }
}

