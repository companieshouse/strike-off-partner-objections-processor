package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

class HmrcOutcomeCallbackClientTest {
    private static final String CALLBACK_BASE_URL = "http://hmrc-callback";
    private static final String CALLBACK_ENDPOINT_URL = CALLBACK_BASE_URL + "/internal/callback-update";

    private MockRestServiceServer server;
    private HmrcOutcomeCallbackClient client;

    @BeforeEach
    void setUp() {
        RestTemplate restTemplate = new RestTemplate();
        this.server = MockRestServiceServer.bindTo(restTemplate).build();
        this.client = new HmrcOutcomeCallbackClient(restTemplate, CALLBACK_BASE_URL);
    }

    @Test
    void submitOutcomeCallback_postsOutcomePayloadToHmrcEndpoint() {
        HmrcOutcomeCallbackRequest request = new HmrcOutcomeCallbackRequest(
                "strike-off-partner-objection#objection",
                "obj-001",
                "12345678",
                "/company/12345678/strike-off-partner-objections/strike-001",
                "objection-accepted");

        server.expect(requestTo(CALLBACK_ENDPOINT_URL))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.resource_kind").value("strike-off-partner-objection#objection"))
                .andExpect(jsonPath("$.resource_id").value("obj-001"))
                .andExpect(jsonPath("$.company_number").value("12345678"))
                .andExpect(jsonPath("$.resource_uri").value("/company/12345678/strike-off-partner-objections/strike-001"))
                .andExpect(jsonPath("$.processing_outcome").value("objection-accepted"))
                .andRespond(withStatus(HttpStatus.ACCEPTED));

        assertDoesNotThrow(() -> client.submitOutcomeCallback(request));
        server.verify();
    }

    @Test
    void submitOutcomeCallback_throwsWhenStatusIsNot202() {
        HmrcOutcomeCallbackRequest request = new HmrcOutcomeCallbackRequest(
                "strike-off-partner-objection#withdrawal",
                "wd-001",
                "12345678",
                "/company/12345678/strike-off-partner-objections-withdrawals/strike-001",
                "withdrawal-rejected");

        server.expect(requestTo(CALLBACK_ENDPOINT_URL))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));

        HmrcCallbackException exception = assertThrows(
                HmrcCallbackException.class,
                () -> client.submitOutcomeCallback(request));

        assertEquals(200, exception.getStatusCode());
    }

    @Test
    void submitOutcomeCallback_usesStringResponseTypeForHmrcCall() {
        RestTemplate restTemplate = mock(RestTemplate.class);
        HmrcOutcomeCallbackClient callbackClient =
                new HmrcOutcomeCallbackClient(restTemplate, CALLBACK_BASE_URL);

        when(restTemplate.postForEntity(
                eq(CALLBACK_ENDPOINT_URL),
                any(HmrcOutcomeCallbackRequest.class),
                eq(String.class)))
                .thenReturn(ResponseEntity.accepted().build());

        assertDoesNotThrow(() -> callbackClient.submitOutcomeCallback(new HmrcOutcomeCallbackRequest(
                "strike-off-partner-objection#objection",
                "obj-001",
                "12345678",
                "/company/12345678/strike-off-partner-objections/strike-001",
                "objection-accepted")));

        verify(restTemplate).postForEntity(
                eq(CALLBACK_ENDPOINT_URL),
                any(HmrcOutcomeCallbackRequest.class),
                eq(String.class));
    }
}

